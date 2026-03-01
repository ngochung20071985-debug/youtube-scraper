#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scraper.py — Background worker quét YouTube API v3 và ghi vào Supabase (PostgreSQL)

✅ Chạy phù hợp cho GitHub Actions (cron 4h/lần)
✅ Dùng async + aiohttp để tăng tốc
✅ Lưu dữ liệu vào 3 bảng: channels, videos, snapshots

ENV bắt buộc (GitHub Actions Secrets):
- YOUTUBE_API_KEY
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY   (khuyến nghị để ghi dữ liệu, bypass RLS)

ENV tuỳ chọn:
- CONCURRENCY=10
- MAX_VIDEOS_PER_CHANNEL=50
- MAX_CHANNELS_PER_RUN=0        (0 = không giới hạn)
- DRY_RUN=0                     (1 = chỉ in log, không ghi DB)
"""

from __future__ import annotations

import os
import re
import math
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from supabase import create_client, Client


YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Thiếu biến môi trường: {name}")
    return str(v).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def iso_to_dt(s: str) -> datetime:
    # YouTube returns RFC3339 like 2026-02-28T12:00:00Z
    s = (s or "").strip()
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


_DURATION_RE = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")


def iso8601_duration_to_seconds(d: str) -> int:
    """
    YouTube duration: 'PT1H2M3S' → seconds
    """
    if not d:
        return 0
    m = _DURATION_RE.fullmatch(d)
    if not m:
        return 0
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + mi * 60 + s


def pick_thumbnail(snippet: Dict[str, Any]) -> str:
    thumbs = (snippet or {}).get("thumbnails") or {}
    for key in ("maxres", "standard", "high", "medium", "default"):
        if key in thumbs and thumbs[key].get("url"):
            return thumbs[key]["url"]
    return ""


@dataclass
class Cfg:
    youtube_api_key: str
    supabase_url: str
    supabase_key: str
    concurrency: int = 10
    max_videos_per_channel: int = 50
    max_channels_per_run: int = 0
    dry_run: bool = False


def load_cfg() -> Cfg:
    return Cfg(
        youtube_api_key=_env("YOUTUBE_API_KEY"),
        supabase_url=_env("SUPABASE_URL"),
        supabase_key=_env("SUPABASE_SERVICE_ROLE_KEY"),
        concurrency=_env_int("CONCURRENCY", 10),
        max_videos_per_channel=_env_int("MAX_VIDEOS_PER_CHANNEL", 50),
        max_channels_per_run=_env_int("MAX_CHANNELS_PER_RUN", 0),
        dry_run=os.getenv("DRY_RUN", "0").strip() == "1",
    )


def supa(cfg: Cfg) -> Client:
    return create_client(cfg.supabase_url, cfg.supabase_key)


async def fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    params: Dict[str, Any],
    *,
    retries: int = 4,
    backoff: float = 1.2,
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for i in range(retries + 1):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"HTTP {resp.status}: {text[:500]}")
                return await resp.json()
        except Exception as e:
            last_err = e
            await asyncio.sleep(backoff ** i)
    raise RuntimeError(f"Request failed after retries: {url} | {params}") from last_err


async def yt_channels(
    session: aiohttp.ClientSession, api_key: str, channel_id: str
) -> Optional[Dict[str, Any]]:
    url = f"{YOUTUBE_API_BASE}/channels"
    params = {
        "part": "snippet,statistics,contentDetails",
        "id": channel_id,
        "key": api_key,
        "maxResults": 1,
    }
    data = await fetch_json(session, url, params)
    items = data.get("items") or []
    return items[0] if items else None


async def yt_playlist_items_video_ids(
    session: aiohttp.ClientSession,
    api_key: str,
    uploads_playlist_id: str,
    *,
    limit: int,
) -> List[str]:
    """
    Lấy video_ids từ uploads playlist (rẻ quota hơn search.list).
    """
    url = f"{YOUTUBE_API_BASE}/playlistItems"
    video_ids: List[str] = []
    page_token: Optional[str] = None

    while len(video_ids) < limit:
        params = {
            "part": "contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": min(50, limit - len(video_ids)),
            "key": api_key,
        }
        if page_token:
            params["pageToken"] = page_token
        data = await fetch_json(session, url, params)
        for it in data.get("items") or []:
            cd = it.get("contentDetails") or {}
            vid = cd.get("videoId")
            if vid:
                video_ids.append(vid)
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return video_ids[:limit]


async def yt_videos_details(
    session: aiohttp.ClientSession, api_key: str, video_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    videos.list: tối đa 50 ids/lần.
    """
    if not video_ids:
        return []
    url = f"{YOUTUBE_API_BASE}/videos"
    params = {
        "part": "snippet,statistics,contentDetails",
        "id": ",".join(video_ids),
        "key": api_key,
        "maxResults": 50,
    }
    data = await fetch_json(session, url, params)
    return data.get("items") or []


def to_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def upsert_channels(client: Client, rows: List[Dict[str, Any]], *, dry_run: bool) -> None:
    if not rows:
        return
    if dry_run:
        print(f"[DRY_RUN] upsert channels: {len(rows)}")
        return
    # on_conflict needs a unique constraint/PK on channel_id
    client.table("channels").upsert(rows, on_conflict="channel_id").execute()


def upsert_videos(client: Client, rows: List[Dict[str, Any]], *, dry_run: bool) -> None:
    if not rows:
        return
    if dry_run:
        print(f"[DRY_RUN] upsert videos: {len(rows)}")
        return
    client.table("videos").upsert(rows, on_conflict="video_id").execute()


def insert_snapshots(client: Client, rows: List[Dict[str, Any]], *, dry_run: bool) -> None:
    if not rows:
        return
    if dry_run:
        print(f"[DRY_RUN] insert snapshots: {len(rows)}")
        return
    client.table("snapshots").insert(rows).execute()


def list_channels_to_scan(client: Client, *, limit: int = 0) -> List[str]:
    """
    Lấy danh sách channel_id cần quét.
    Giả định bảng channels có cột channel_id.
    """
    q = client.table("channels").select("channel_id").order("channel_id", desc=False)
    if limit and limit > 0:
        q = q.limit(limit)
    res = q.execute()
    data = res.data or []
    ids = [d["channel_id"] for d in data if d.get("channel_id")]
    # de-dup
    return list(dict.fromkeys(ids))


async def process_one_channel(
    sem: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    cfg: Cfg,
    client: Client,
    channel_id: str,
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Returns: (channel_row, video_rows, snapshot_row)
    """
    async with sem:
        ch = await yt_channels(session, cfg.youtube_api_key, channel_id)
        if not ch:
            print(f"[WARN] channel not found: {channel_id}")
            return None, [], None

        snippet = ch.get("snippet") or {}
        stats = ch.get("statistics") or {}
        content = ch.get("contentDetails") or {}
        uploads = ((content.get("relatedPlaylists") or {}).get("uploads")) or ""

        title = safe_str(snippet.get("title"))
        avatar = pick_thumbnail(snippet)
        country = safe_str(snippet.get("country"))
        default_language = safe_str(snippet.get("defaultLanguage"))
        default_audio_language = safe_str(snippet.get("defaultAudioLanguage"))
        yt_published_at = safe_str(snippet.get("publishedAt"))

        subscribers = to_int(stats.get("subscriberCount"))
        total_views = to_int(stats.get("viewCount"))
        video_count = to_int(stats.get("videoCount"))

        captured_at = utc_now_iso()

        channel_row = {
            "channel_id": channel_id,
            "title": title,
            "avatar_url": avatar,
            "country": country or None,
            "default_language": default_language or None,
            "default_audio_language": default_audio_language or None,
            "yt_published_at": yt_published_at or None,
            "subscribers": subscribers,
            "total_views": total_views,
            "video_count": video_count,
            "last_scanned_at": captured_at,
        }

        snapshot_row = {
            "channel_id": channel_id,
            "captured_at": captured_at,
            "subscribers": subscribers,
            "total_views": total_views,
            "video_count": video_count,
        }

        video_rows: List[Dict[str, Any]] = []
        if uploads:
            limit = max(1, int(cfg.max_videos_per_channel))
            vids = await yt_playlist_items_video_ids(session, cfg.youtube_api_key, uploads, limit=limit)

            # fetch video details in batches
            for i in range(0, len(vids), 50):
                batch = vids[i : i + 50]
                items = await yt_videos_details(session, cfg.youtube_api_key, batch)
                for it in items:
                    vid = safe_str(it.get("id"))
                    vsn = it.get("snippet") or {}
                    vstat = it.get("statistics") or {}
                    vcd = it.get("contentDetails") or {}

                    vtitle = safe_str(vsn.get("title"))
                    published_at = safe_str(vsn.get("publishedAt"))
                    thumb = pick_thumbnail(vsn)
                    duration_sec = iso8601_duration_to_seconds(safe_str(vcd.get("duration")))
                    views = to_int(vstat.get("viewCount"))
                    likes = to_int(vstat.get("likeCount"))
                    comments = to_int(vstat.get("commentCount"))

                    video_rows.append(
                        {
                            "video_id": vid,
                            "channel_id": channel_id,
                            "title": vtitle,
                            "published_at": published_at or None,
                            "duration_sec": duration_sec,
                            "views": views,
                            "likes": likes,
                            "comments": comments,
                            "thumb_url": thumb,
                            "url": f"https://www.youtube.com/watch?v={vid}",
                            "updated_at": captured_at,
                        }
                    )

        return channel_row, video_rows, snapshot_row


async def run_async(cfg: Cfg) -> None:
    client = supa(cfg)
    channel_ids = list_channels_to_scan(client, limit=cfg.max_channels_per_run)
    if not channel_ids:
        print("[INFO] Không có channel nào trong bảng channels. Hãy insert channel_id trước.")
        return

    print(f"[INFO] Sẽ quét {len(channel_ids)} kênh | max_videos_per_channel={cfg.max_videos_per_channel} | concurrency={cfg.concurrency}")

    sem = asyncio.Semaphore(max(1, cfg.concurrency))

    headers = {
        "User-Agent": "toolwatch-scraper/1.0",
        "Accept": "application/json",
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [
            process_one_channel(sem, session, cfg, client, cid)
            for cid in channel_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    ch_rows: List[Dict[str, Any]] = []
    vid_rows: List[Dict[str, Any]] = []
    snap_rows: List[Dict[str, Any]] = []

    for r in results:
        if isinstance(r, Exception):
            print(f"[ERROR] channel task failed: {r}")
            continue
        ch_row, vrows, srow = r
        if ch_row:
            ch_rows.append(ch_row)
        if vrows:
            vid_rows.extend(vrows)
        if srow:
            snap_rows.append(srow)

    print(f"[INFO] Upsert channels={len(ch_rows)} | videos={len(vid_rows)} | snapshots={len(snap_rows)}")
    upsert_channels(client, ch_rows, dry_run=cfg.dry_run)
    upsert_videos(client, vid_rows, dry_run=cfg.dry_run)
    insert_snapshots(client, snap_rows, dry_run=cfg.dry_run)

    print("[DONE] OK")


def main() -> None:
    cfg = load_cfg()
    asyncio.run(run_async(cfg))


if __name__ == "__main__":
    main()
