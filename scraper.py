#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scraper.py — YouTube API v3 -> Supabase + Gemini AI enrichment

✅ async + aiohttp
✅ Không dùng sqlite
✅ Quét kênh từ Supabase table: channels
✅ Ghi video vào Supabase table: videos (có niche, sentiment)
✅ Ghi snapshot video vào Supabase table: snapshots (view/like/comment theo thời điểm)

ENV bắt buộc:
- YOUTUBE_API_KEY
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY
- GEMINI_API_KEY   (để phân tích AI; nếu không có sẽ skip)

ENV tuỳ chọn:
- CONCURRENCY=10
- AI_CONCURRENCY=2
- MAX_VIDEOS_PER_CHANNEL=50
- MAX_CHANNELS_PER_RUN=0
- GEMINI_MODEL=gemini-2.0-flash
- DRY_RUN=0
"""

from __future__ import annotations

import os
import re
import json
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from supabase import create_client, Client

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta"


# -------------------------
# ENV helpers
# -------------------------
def _env(name: str, default: Optional[str] = None, required: bool = True) -> str:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Thiếu biến môi trường: {name}")
    return "" if v is None else str(v).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_DURATION_RE = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")


def iso8601_duration_to_seconds(d: str) -> int:
    if not d:
        return 0
    m = _DURATION_RE.fullmatch(d)
    if not m:
        return 0
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + mi * 60 + s


def to_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def pick_thumbnail(snippet: Dict[str, Any]) -> str:
    thumbs = (snippet or {}).get("thumbnails") or {}
    for key in ("maxres", "standard", "high", "medium", "default"):
        if key in thumbs and thumbs[key].get("url"):
            return thumbs[key]["url"]
    return ""


# -------------------------
# Config
# -------------------------
@dataclass
class Cfg:
    youtube_api_key: str
    supabase_url: str
    supabase_key: str

    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.0-flash"

    concurrency: int = 10
    ai_concurrency: int = 2
    max_videos_per_channel: int = 50
    max_channels_per_run: int = 0
    dry_run: bool = False


def load_cfg() -> Cfg:
    return Cfg(
        youtube_api_key=_env("YOUTUBE_API_KEY"),
        supabase_url=_env("SUPABASE_URL"),
        supabase_key=_env("SUPABASE_SERVICE_ROLE_KEY"),
        gemini_api_key=_env("GEMINI_API_KEY", required=False),
        gemini_model=_env("GEMINI_MODEL", default="gemini-2.0-flash", required=False) or "gemini-2.0-flash",
        concurrency=_env_int("CONCURRENCY", 10),
        ai_concurrency=_env_int("AI_CONCURRENCY", 2),
        max_videos_per_channel=_env_int("MAX_VIDEOS_PER_CHANNEL", 50),
        max_channels_per_run=_env_int("MAX_CHANNELS_PER_RUN", 0),
        dry_run=os.getenv("DRY_RUN", "0").strip() == "1",
    )


def supa(cfg: Cfg) -> Client:
    return create_client(cfg.supabase_url, cfg.supabase_key)


# -------------------------
# HTTP helpers
# -------------------------
async def fetch_json_get(
    session: aiohttp.ClientSession,
    url: str,
    params: Dict[str, Any],
    *,
    retries: int = 4,
    backoff: float = 1.4,
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for i in range(retries + 1):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=40)) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"HTTP {resp.status}: {text[:500]}")
                return await resp.json()
        except Exception as e:
            last_err = e
            await asyncio.sleep(backoff ** i)
    raise RuntimeError(f"GET failed after retries: {url}") from last_err


async def fetch_json_post(
    session: aiohttp.ClientSession,
    url: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    *,
    retries: int = 3,
    backoff: float = 1.5,
) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for i in range(retries + 1):
        try:
            async with session.post(
                url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=60)
            ) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"HTTP {resp.status}: {text[:500]}")
                return await resp.json()
        except Exception as e:
            last_err = e
            await asyncio.sleep(backoff ** i)
    raise RuntimeError(f"POST failed after retries: {url}") from last_err


# -------------------------
# YouTube API
# -------------------------
async def yt_channel_by_id(session: aiohttp.ClientSession, api_key: str, channel_id: str) -> Optional[Dict[str, Any]]:
    url = f"{YOUTUBE_API_BASE}/channels"
    params = {"part": "snippet,statistics,contentDetails", "id": channel_id, "key": api_key, "maxResults": 1}
    data = await fetch_json_get(session, url, params)
    items = data.get("items") or []
    return items[0] if items else None


async def yt_channel_by_handle(session: aiohttp.ClientSession, api_key: str, handle: str) -> Optional[Dict[str, Any]]:
    # YouTube API supports "forHandle" (handle without '@') in many setups.
    h = (handle or "").strip()
    if h.startswith("@"):
        h = h[1:]
    if not h:
        return None
    url = f"{YOUTUBE_API_BASE}/channels"
    params = {"part": "snippet,statistics,contentDetails", "forHandle": h, "key": api_key, "maxResults": 1}
    try:
        data = await fetch_json_get(session, url, params)
        items = data.get("items") or []
        return items[0] if items else None
    except Exception:
        return None


async def yt_playlist_items_video_ids(
    session: aiohttp.ClientSession, api_key: str, uploads_playlist_id: str, *, limit: int
) -> List[str]:
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
        data = await fetch_json_get(session, url, params)

        for it in data.get("items") or []:
            cd = it.get("contentDetails") or {}
            vid = cd.get("videoId")
            if vid:
                video_ids.append(vid)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return video_ids[:limit]


async def yt_videos_details(session: aiohttp.ClientSession, api_key: str, video_ids: List[str]) -> List[Dict[str, Any]]:
    if not video_ids:
        return []
    url = f"{YOUTUBE_API_BASE}/videos"
    params = {
        "part": "snippet,statistics,contentDetails",
        "id": ",".join(video_ids),
        "key": api_key,
        "maxResults": 50,
    }
    data = await fetch_json_get(session, url, params)
    return data.get("items") or []


# -------------------------
# Gemini AI
# -------------------------
def _extract_model_text(resp: Dict[str, Any]) -> str:
    # candidates[0].content.parts[*].text
    try:
        cands = resp.get("candidates") or []
        if not cands:
            return ""
        content = (cands[0] or {}).get("content") or {}
        parts = content.get("parts") or []
        texts = []
        for p in parts:
            t = p.get("text")
            if t:
                texts.append(t)
        return "\n".join(texts).strip()
    except Exception:
        return ""


def _safe_json_load(s: str) -> Optional[Dict[str, Any]]:
    s = (s or "").strip()
    if not s:
        return None
    # remove code fences if any
    s = re.sub(r"^```(json)?", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"```$", "", s).strip()
    # try direct
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    # try slice first {...}
    try:
        i = s.find("{")
        j = s.rfind("}")
        if i >= 0 and j > i:
            obj = json.loads(s[i : j + 1])
            return obj if isinstance(obj, dict) else None
    except Exception:
        return None
    return None


async def analyze_video_with_ai(
    session: aiohttp.ClientSession,
    cfg: Cfg,
    title: str,
    description: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (niche, sentiment) by Gemini.
    Uses JSON mode via generationConfig.response_mime_type + response_schema.
    """
    if not cfg.gemini_api_key:
        return None, None

    prompt = (
        "Bạn là hệ thống phân loại video YouTube.\n"
        "Hãy phân tích Tiêu đề và Mô tả để trả về JSON đúng schema.\n"
        "Quy tắc:\n"
        "- niche: ngách chủ đề chính (1-3 từ, ví dụ: Tài chính, Tâm lý học, Khoa học, Giải trí, Game, Học tập...)\n"
        "- sentiment: phong cách/cảm xúc chính (1-3 từ, ví dụ: Giật gân, Hài hước, Giáo dục, Truyền cảm hứng, Drama, Review...)\n"
        "- Không thêm field khác.\n\n"
        f"Tiêu đề: {title}\n"
        f"Mô tả: {description[:1500]}\n"
    )

    url = f"{GEMINI_BASE}/models/{cfg.gemini_model}:generateContent?key={cfg.gemini_api_key}"
    headers = {"Content-Type": "application/json"}

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 128,
            "response_mime_type": "application/json",
            "response_schema": {
                "type": "OBJECT",
                "properties": {
                    "niche": {"type": "STRING"},
                    "sentiment": {"type": "STRING"},
                },
                "required": ["niche", "sentiment"],
            },
        },
    }

    try:
        resp = await fetch_json_post(session, url, headers, payload)
        text = _extract_model_text(resp)
        obj = _safe_json_load(text)
        if not obj:
            return None, None
        niche = safe_str(obj.get("niche")).strip() or None
        sentiment = safe_str(obj.get("sentiment")).strip() or None
        return niche, sentiment
    except Exception as e:
        print(f"[AI_WARN] analyze failed: {e}")
        return None, None


# -------------------------
# Supabase ops
# -------------------------
def list_channels_to_scan(client: Client, *, limit: int = 0) -> List[Dict[str, Any]]:
    q = client.table("channels").select("channel_id,handle").order("created_at", desc=False)
    if limit and limit > 0:
        q = q.limit(limit)
    res = q.execute()
    rows = res.data or []
    # keep as list of dict
    return rows


def upsert_channels(client: Client, rows: List[Dict[str, Any]], *, dry_run: bool) -> None:
    if not rows:
        return
    if dry_run:
        print(f"[DRY_RUN] upsert channels: {len(rows)}")
        return
    # requires unique on channel_id (recommended)
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


# -------------------------
# Main pipeline
# -------------------------
async def process_one_channel(
    yt_sem: asyncio.Semaphore,
    ai_sem: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    cfg: Cfg,
    client: Client,
    channel_row_in_db: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns:
      - channel_upsert_row
      - video_upsert_rows (with niche/sentiment)
      - snapshot_insert_rows
    """
    async with yt_sem:
        channel_id = safe_str(channel_row_in_db.get("channel_id")).strip()
        handle = safe_str(channel_row_in_db.get("handle")).strip()

        ch = None
        if channel_id:
            ch = await yt_channel_by_id(session, cfg.youtube_api_key, channel_id)
        elif handle:
            ch = await yt_channel_by_handle(session, cfg.youtube_api_key, handle)

        if not ch:
            print(f"[WARN] channel not found (id/handle): {channel_id or handle}")
            return None, [], []

        snippet = ch.get("snippet") or {}
        stats = ch.get("statistics") or {}
        content = ch.get("contentDetails") or {}
        uploads = ((content.get("relatedPlaylists") or {}).get("uploads")) or ""

        channel_id = safe_str(ch.get("id")).strip() or channel_id
        title = safe_str(snippet.get("title")).strip()
        avatar = pick_thumbnail(snippet)
        subs = to_int(stats.get("subscriberCount"))

        # handle from YouTube (customUrl sometimes)
        yt_custom = safe_str(snippet.get("customUrl")).strip()
        if yt_custom and not yt_custom.startswith("@"):
            yt_custom = "@" + yt_custom
        merged_handle = (handle or yt_custom or "").strip() or None

        captured_at = utc_now_iso()

        # IMPORTANT: only columns that exist in channels table
        channel_upsert = {
            "channel_id": channel_id,
            "title": title,
            "handle": merged_handle,
            "avatar_url": avatar,
            "subscribers": subs,
            # created_at is DB-managed
        }

    # videos + snapshots (outside yt_sem? still counts quota; keep simple: still under same channel processing)
    video_rows: List[Dict[str, Any]] = []
    snapshot_rows: List[Dict[str, Any]] = []

    if uploads:
        limit = max(1, int(cfg.max_videos_per_channel))
        vids = await yt_playlist_items_video_ids(session, cfg.youtube_api_key, uploads, limit=limit)

        # fetch details in batches
        for i in range(0, len(vids), 50):
            batch = vids[i : i + 50]
            items = await yt_videos_details(session, cfg.youtube_api_key, batch)
            for it in items:
                vid = safe_str(it.get("id")).strip()
                vsn = it.get("snippet") or {}
                vstat = it.get("statistics") or {}
                vcd = it.get("contentDetails") or {}

                vtitle = safe_str(vsn.get("title")).strip()
                vdesc = safe_str(vsn.get("description"))
                published_at = safe_str(vsn.get("publishedAt")).strip() or None
                thumb = pick_thumbnail(vsn)
                duration_sec = iso8601_duration_to_seconds(safe_str(vcd.get("duration")))

                views = to_int(vstat.get("viewCount"))
                likes = to_int(vstat.get("likeCount"))
                comments = to_int(vstat.get("commentCount"))

                # AI enrich (only for "new video" or missing fields would be ideal;
                # here we do it for every video fetched, but Gemini concurrency is limited.
                niche = None
                sentiment = None
                if cfg.gemini_api_key:
                    async with ai_sem:
                        niche, sentiment = await analyze_video_with_ai(session, cfg, vtitle, vdesc)

                # IMPORTANT: only columns you said exist + common fields
                video_rows.append(
                    {
                        "video_id": vid,
                        "channel_id": channel_id,
                        "title": vtitle,
                        "description": vdesc,
                        "published_at": published_at,
                        "duration_sec": duration_sec,
                        "thumb_url": thumb,
                        "url": f"https://www.youtube.com/watch?v={vid}",
                        "updated_at": captured_at,
                        "niche": niche,
                        "sentiment": sentiment,
                    }
                )

                # snapshots table: video metrics by time
                snapshot_rows.append(
                    {
                        "video_id": vid,
                        "captured_at": captured_at,
                        "view_count": views,
                        "like_count": likes,
                        "comment_count": comments,
                    }
                )

    return channel_upsert, video_rows, snapshot_rows


async def run_async(cfg: Cfg) -> None:
    client = supa(cfg)
    rows = list_channels_to_scan(client, limit=cfg.max_channels_per_run)
    if not rows:
        print("[INFO] Không có kênh trong bảng channels.")
        return

    print(
        f"[INFO] Quét {len(rows)} kênh | max_videos_per_channel={cfg.max_videos_per_channel} | "
        f"concurrency={cfg.concurrency} | ai_concurrency={cfg.ai_concurrency} | ai={'on' if cfg.gemini_api_key else 'off'}"
    )

    yt_sem = asyncio.Semaphore(max(1, cfg.concurrency))
    ai_sem = asyncio.Semaphore(max(1, cfg.ai_concurrency))

    headers = {"User-Agent": "toolwatch-scraper/2.0", "Accept": "application/json"}

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [
            process_one_channel(yt_sem, ai_sem, session, cfg, client, r)
            for r in rows
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    ch_rows: List[Dict[str, Any]] = []
    vid_rows: List[Dict[str, Any]] = []
    snap_rows: List[Dict[str, Any]] = []

    for r in results:
        if isinstance(r, Exception):
            print(f"[ERROR] channel task failed: {r}")
            continue
        ch, vids, snaps = r
        if ch:
            ch_rows.append(ch)
        if vids:
            vid_rows.extend(vids)
        if snaps:
            snap_rows.extend(snaps)

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
