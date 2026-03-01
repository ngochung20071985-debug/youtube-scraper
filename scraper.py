#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scraper.py (Supabase schema-aligned)

ENV required:
- YOUTUBE_API_KEY
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY

Optional:
- GEMINI_API_KEY
- GEMINI_MODEL=gemini-2.0-flash
- CONCURRENCY=8
- AI_CONCURRENCY=2
- MAX_VIDEOS_PER_CHANNEL=50
- DISCOVERY_KEYWORDS=kiếm tiền online,kể chuyện đêm khuya,AI tools
- DISCOVERY_MAX_NEW=50
- DISCOVERY_PAGES_PER_KEYWORD=1
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
# ENV
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


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def to_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


_DURATION_RE = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")


def iso8601_duration_to_seconds(d: str) -> int:
    # not stored in your videos table; kept if you want later
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
    for k in ("maxres", "standard", "high", "medium", "default"):
        if k in thumbs and thumbs[k].get("url"):
            return thumbs[k]["url"]
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

    concurrency: int = 8
    ai_concurrency: int = 2
    max_videos_per_channel: int = 50

    discovery_keywords: List[str] = None
    discovery_max_new: int = 50
    discovery_pages_per_keyword: int = 1

    dry_run: bool = False


def load_cfg() -> Cfg:
    kw_raw = os.getenv("DISCOVERY_KEYWORDS", "").strip()
    keywords = [k.strip() for k in kw_raw.split(",") if k.strip()]

    return Cfg(
        youtube_api_key=_env("YOUTUBE_API_KEY"),
        supabase_url=_env("SUPABASE_URL"),
        supabase_key=_env("SUPABASE_SERVICE_ROLE_KEY"),
        gemini_api_key=_env("GEMINI_API_KEY", required=False),
        gemini_model=_env("GEMINI_MODEL", default="gemini-2.0-flash", required=False) or "gemini-2.0-flash",
        concurrency=_env_int("CONCURRENCY", 8),
        ai_concurrency=_env_int("AI_CONCURRENCY", 2),
        max_videos_per_channel=_env_int("MAX_VIDEOS_PER_CHANNEL", 50),
        discovery_keywords=keywords,
        discovery_max_new=_env_int("DISCOVERY_MAX_NEW", 50),
        discovery_pages_per_keyword=_env_int("DISCOVERY_PAGES_PER_KEYWORD", 1),
        dry_run=os.getenv("DRY_RUN", "0").strip() == "1",
    )


def supa(cfg: Cfg) -> Client:
    return create_client(cfg.supabase_url, cfg.supabase_key)


# -------------------------
# HTTP
# -------------------------
async def fetch_json_get(session: aiohttp.ClientSession, url: str, params: Dict[str, Any], retries: int = 4) -> Dict[str, Any]:
    last: Optional[Exception] = None
    for i in range(retries + 1):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"HTTP {resp.status}: {text[:600]}")
                return await resp.json()
        except Exception as e:
            last = e
            await asyncio.sleep(1.4 ** i)
    raise RuntimeError(f"GET failed: {url}") from last


async def fetch_json_post(session: aiohttp.ClientSession, url: str, payload: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
    last: Optional[Exception] = None
    for i in range(retries + 1):
        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=60)
            ) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"HTTP {resp.status}: {text[:600]}")
                return await resp.json()
        except Exception as e:
            last = e
            await asyncio.sleep(1.5 ** i)
    raise RuntimeError(f"POST failed: {url}") from last


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


async def yt_playlist_items_video_ids(session: aiohttp.ClientSession, api_key: str, uploads_playlist_id: str, limit: int) -> List[str]:
    url = f"{YOUTUBE_API_BASE}/playlistItems"
    out: List[str] = []
    page_token: Optional[str] = None
    while len(out) < limit:
        params = {
            "part": "contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": min(50, limit - len(out)),
            "key": api_key,
        }
        if page_token:
            params["pageToken"] = page_token
        data = await fetch_json_get(session, url, params)
        for it in data.get("items") or []:
            vid = ((it.get("contentDetails") or {}).get("videoId")) or ""
            if vid:
                out.append(vid)
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return out[:limit]


async def yt_videos_details(session: aiohttp.ClientSession, api_key: str, video_ids: List[str]) -> List[Dict[str, Any]]:
    if not video_ids:
        return []
    url = f"{YOUTUBE_API_BASE}/videos"
    params = {
        "part": "snippet,statistics,contentDetails",
        "id": ",".join(video_ids[:50]),
        "key": api_key,
        "maxResults": 50,
    }
    data = await fetch_json_get(session, url, params)
    return data.get("items") or []


async def yt_search_channels(session: aiohttp.ClientSession, api_key: str, query: str, page_token: Optional[str] = None) -> Tuple[List[str], Optional[str]]:
    url = f"{YOUTUBE_API_BASE}/search"
    params = {"part": "snippet", "type": "channel", "q": query, "maxResults": 50, "key": api_key}
    if page_token:
        params["pageToken"] = page_token
    data = await fetch_json_get(session, url, params)
    ids: List[str] = []
    for it in data.get("items") or []:
        cid = ((it.get("id") or {}).get("channelId")) or ""
        if cid:
            ids.append(cid)
    return ids, data.get("nextPageToken")


async def yt_channels_bulk(session: aiohttp.ClientSession, api_key: str, channel_ids: List[str]) -> List[Dict[str, Any]]:
    if not channel_ids:
        return []
    url = f"{YOUTUBE_API_BASE}/channels"
    params = {"part": "snippet,statistics", "id": ",".join(channel_ids[:50]), "key": api_key, "maxResults": 50}
    data = await fetch_json_get(session, url, params)
    return data.get("items") or []


# -------------------------
# Gemini AI (optional)
# -------------------------
def _extract_model_text(resp: Dict[str, Any]) -> str:
    try:
        cands = resp.get("candidates") or []
        if not cands:
            return ""
        parts = ((cands[0] or {}).get("content") or {}).get("parts") or []
        texts = [p.get("text") for p in parts if p.get("text")]
        return "\n".join(texts).strip()
    except Exception:
        return ""


def _safe_json_load(s: str) -> Optional[Dict[str, Any]]:
    s = (s or "").strip()
    if not s:
        return None
    s = re.sub(r"^```(json)?", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"```$", "", s).strip()
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    try:
        i = s.find("{"); j = s.rfind("}")
        if i >= 0 and j > i:
            obj = json.loads(s[i:j+1])
            return obj if isinstance(obj, dict) else None
    except Exception:
        return None
    return None


async def analyze_video_with_ai(session: aiohttp.ClientSession, cfg: Cfg, title: str, description: str) -> Tuple[Optional[str], Optional[str]]:
    if not cfg.gemini_api_key:
        return None, None

    prompt = (
        "Hãy phân tích tiêu đề + mô tả video YouTube và trả về JSON đúng schema.\n"
        "Chỉ trả về 2 field:\n"
        '- niche: ngách chủ đề chính (ví dụ: "Tài chính", "Kể chuyện", "AI tools")\n'
        '- sentiment: phong cách/cảm xúc (ví dụ: "Giật gân", "Hài hước", "Giáo dục")\n\n'
        f"Tiêu đề: {title}\n"
        f"Mô tả: {description[:1500]}\n"
    )

    url = f"{GEMINI_BASE}/models/{cfg.gemini_model}:generateContent?key={cfg.gemini_api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 128,
            "response_mime_type": "application/json",
            "response_schema": {
                "type": "OBJECT",
                "properties": {"niche": {"type": "STRING"}, "sentiment": {"type": "STRING"}},
                "required": ["niche", "sentiment"],
            },
        },
    }

    try:
        resp = await fetch_json_post(session, url, payload)
        text = _extract_model_text(resp)
        obj = _safe_json_load(text)
        if not obj:
            return None, None
        niche = safe_str(obj.get("niche")).strip() or None
        sentiment = safe_str(obj.get("sentiment")).strip() or None
        return niche, sentiment
    except Exception as e:
        print(f"[AI_WARN] {e}")
        return None, None


# -------------------------
# Supabase state helpers (daily)
# -------------------------
def get_state(client: Client, key: str) -> Optional[str]:
    try:
        r = client.table("scraper_state").select("value").eq("key", key).limit(1).execute()
        row = (r.data or [None])[0]
        if row and row.get("value"):
            return str(row["value"])
    except Exception:
        return None
    return None


def set_state(client: Client, key: str, value: str) -> None:
    try:
        client.table("scraper_state").upsert({"key": key, "value": value}, on_conflict="key").execute()
    except Exception:
        pass


# -------------------------
# DB ops (schema aligned to your tables)
# -------------------------
def list_channels(client: Client) -> List[Dict[str, Any]]:
    # include id so we can update channel_id for handle rows
    r = client.table("channels").select("id,channel_id,handle").execute()
    return r.data or []


def upsert_channels(client: Client, rows: List[Dict[str, Any]], dry: bool) -> None:
    if not rows:
        return
    if dry:
        print(f"[DRY] upsert channels: {len(rows)}")
        return
    # columns that exist in your channels table only
    safe_rows = []
    for x in rows:
        safe_rows.append({
            "id": x.get("id"),  # optional
            "channel_id": x.get("channel_id"),
            "title": x.get("title"),
            "handle": x.get("handle"),
            "avatar_url": x.get("avatar_url"),
            "subscribers": x.get("subscribers"),
        })
    try:
        client.table("channels").upsert(safe_rows, on_conflict="channel_id").execute()
    except Exception:
        # fallback (if no unique)
        for row in safe_rows:
            if row.get("id"):
                client.table("channels").update(row).eq("id", row["id"]).execute()


def update_channel_id_by_row_id(client: Client, row_id: int, channel_id: str, dry: bool) -> None:
    if dry:
        print(f"[DRY] update channels.id={row_id} channel_id={channel_id}")
        return
    client.table("channels").update({"channel_id": channel_id}).eq("id", row_id).execute()


def upsert_videos_schema_aligned(client: Client, rows: List[Dict[str, Any]], dry: bool) -> None:
    if not rows:
        return
    if dry:
        print(f"[DRY] upsert videos: {len(rows)}")
        return

    # Your videos columns (from screenshot):
    # video_id, channel_id, published_at, title, description, tags_json (+ niche, sentiment if exist)
    def try_upsert(payload_rows: List[Dict[str, Any]]) -> bool:
        try:
            client.table("videos").upsert(payload_rows, on_conflict="video_id").execute()
            return True
        except Exception as e:
            print(f"[WARN] upsert videos failed: {e}")
            return False

    payload = []
    for r in rows:
        base = {
            "video_id": r["video_id"],
            "channel_id": r["channel_id"],
            "published_at": r.get("published_at"),
            "title": r.get("title"),
            "description": r.get("description"),
            "tags_json": r.get("tags_json"),
        }
        # include AI cols if present in your table; if not, we’ll strip on failure
        if r.get("niche") is not None:
            base["niche"] = r.get("niche")
        if r.get("sentiment") is not None:
            base["sentiment"] = r.get("sentiment")
        payload.append(base)

    if try_upsert(payload):
        return

    # fallback: remove AI fields if schema doesn't have them
    for p in payload:
        p.pop("niche", None)
        p.pop("sentiment", None)
    if try_upsert(payload):
        return

    # last fallback insert (may fail on duplicates)
    client.table("videos").insert(payload).execute()


def insert_snapshots_schema_aligned(client: Client, rows: List[Dict[str, Any]], dry: bool) -> None:
    if not rows:
        return
    if dry:
        print(f"[DRY] insert snapshots: {len(rows)}")
        return
    payload = []
    for r in rows:
        payload.append({
            "video_id": r["video_id"],
            "captured_at": r["captured_at"],
            "view_count": r["view_count"],
            "like_count": r["like_count"],
            "comment_count": r["comment_count"],
        })
    client.table("snapshots").insert(payload).execute()


# -------------------------
# Auto discover (daily)
# -------------------------
async def auto_discover_channels(session: aiohttp.ClientSession, cfg: Cfg, client: Client) -> int:
    keywords = cfg.discovery_keywords or []
    if not keywords:
        return 0

    found: List[str] = []
    pages = max(1, int(cfg.discovery_pages_per_keyword))

    for kw in keywords:
        tok = None
        for _ in range(pages):
            ids, tok = await yt_search_channels(session, cfg.youtube_api_key, kw, page_token=tok)
            found.extend(ids)
            if not tok:
                break

    found = list(dict.fromkeys([x for x in found if x]))
    if not found:
        return 0

    # existing channel_ids
    existing = set()
    try:
        r = client.table("channels").select("channel_id").execute()
        for row in r.data or []:
            if row.get("channel_id"):
                existing.add(str(row["channel_id"]))
    except Exception:
        pass

    candidates: List[Dict[str, Any]] = []
    for i in range(0, len(found), 50):
        batch = found[i:i+50]
        items = await yt_channels_bulk(session, cfg.youtube_api_key, batch)
        for it in items:
            cid = safe_str(it.get("id")).strip()
            if not cid or cid in existing:
                continue
            sn = it.get("snippet") or {}
            stt = it.get("statistics") or {}
            subs = to_int(stt.get("subscriberCount"))
            if subs < 1000 or subs > 50000:
                continue
            title = safe_str(sn.get("title")).strip()
            avatar = pick_thumbnail(sn)
            custom = safe_str(sn.get("customUrl")).strip()
            handle = ("@" + custom) if custom and not custom.startswith("@") else (custom or None)

            candidates.append({
                "channel_id": cid,
                "title": title,
                "handle": handle,
                "avatar_url": avatar,
                "subscribers": subs,
            })

    if not candidates:
        return 0

    candidates = candidates[:max(1, int(cfg.discovery_max_new))]
    if cfg.dry_run:
        print(f"[DRY] discover would add {len(candidates)}")
        return len(candidates)

    try:
        client.table("channels").upsert(candidates, on_conflict="channel_id").execute()
    except Exception:
        client.table("channels").insert(candidates).execute()

    print(f"[DISCOVER] added {len(candidates)} channels")
    return len(candidates)


# -------------------------
# Main scan
# -------------------------
async def process_one_channel(
    yt_sem: asyncio.Semaphore,
    ai_sem: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    cfg: Cfg,
    client: Client,
    row: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (video_rows, snapshot_rows) for this channel.
    Also resolves handle -> channel_id if needed.
    """
    row_id = row.get("id")
    channel_id = safe_str(row.get("channel_id")).strip()
    handle = safe_str(row.get("handle")).strip()

    async with yt_sem:
        ch = None
        if channel_id:
            ch = await yt_channel_by_id(session, cfg.youtube_api_key, channel_id)
        elif handle:
            ch = await yt_channel_by_handle(session, cfg.youtube_api_key, handle)

        if not ch:
            print(f"[WARN] channel not found: id={channel_id} handle={handle}")
            return [], []

        # Resolve channel_id back to DB if we only had handle before
        real_cid = safe_str(ch.get("id")).strip()
        if real_cid and (not channel_id) and row_id is not None:
            try:
                update_channel_id_by_row_id(client, int(row_id), real_cid, cfg.dry_run)
                channel_id = real_cid
            except Exception as e:
                print(f"[WARN] cannot update channel_id for row {row_id}: {e}")
                channel_id = real_cid

        snippet = ch.get("snippet") or {}
        stats = ch.get("statistics") or {}
        content = ch.get("contentDetails") or {}
        uploads = ((content.get("relatedPlaylists") or {}).get("uploads")) or ""

        # update channel stats (optional)
        ch_up = [{
            "channel_id": channel_id,
            "title": safe_str(snippet.get("title")).strip(),
            "avatar_url": pick_thumbnail(snippet),
            "subscribers": to_int(stats.get("subscriberCount")),
            "handle": handle or None,
        }]
        upsert_channels(client, ch_up, cfg.dry_run)

    if not uploads:
        return [], []

    # get videos
    vid_ids = await yt_playlist_items_video_ids(session, cfg.youtube_api_key, uploads, limit=max(1, cfg.max_videos_per_channel))

    video_rows: List[Dict[str, Any]] = []
    snapshot_rows: List[Dict[str, Any]] = []
    captured_at = utc_now_iso()

    for i in range(0, len(vid_ids), 50):
        batch = vid_ids[i:i+50]
        items = await yt_videos_details(session, cfg.youtube_api_key, batch)

        for it in items:
            vid = safe_str(it.get("id")).strip()
            vsn = it.get("snippet") or {}
            vstat = it.get("statistics") or {}

            title = safe_str(vsn.get("title")).strip()
            desc = safe_str(vsn.get("description"))
            published_at = safe_str(vsn.get("publishedAt")).strip() or None
            tags = vsn.get("tags") or []
            tags_json = json.dumps(tags, ensure_ascii=False)

            niche = None
            sentiment = None
            if cfg.gemini_api_key:
                async with ai_sem:
                    niche, sentiment = await analyze_video_with_ai(session, cfg, title, desc)

            video_rows.append({
                "video_id": vid,
                "channel_id": channel_id,
                "published_at": published_at,
                "title": title,
                "description": desc,
                "tags_json": tags_json,
                "niche": niche,
                "sentiment": sentiment,
            })

            snapshot_rows.append({
                "video_id": vid,
                "captured_at": captured_at,
                "view_count": to_int(vstat.get("viewCount")),
                "like_count": to_int(vstat.get("likeCount")),
                "comment_count": to_int(vstat.get("commentCount")),
            })

    return video_rows, snapshot_rows


async def run_async(cfg: Cfg) -> None:
    client = supa(cfg)
    rows = list_channels(client)
    if not rows:
        print("[INFO] No channels in DB.")
        return

    yt_sem = asyncio.Semaphore(max(1, cfg.concurrency))
    ai_sem = asyncio.Semaphore(max(1, cfg.ai_concurrency))

    headers = {"User-Agent": "toolwatch-scraper/3.0", "Accept": "application/json"}
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [process_one_channel(yt_sem, ai_sem, session, cfg, client, r) for r in rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_videos: List[Dict[str, Any]] = []
        all_snaps: List[Dict[str, Any]] = []

        for r in results:
            if isinstance(r, Exception):
                print(f"[ERROR] channel task failed: {r}")
                continue
            vids, snaps = r
            all_videos.extend(vids)
            all_snaps.extend(snaps)

        print(f"[INFO] videos={len(all_videos)} snapshots={len(all_snaps)}")

        upsert_videos_schema_aligned(client, all_videos, cfg.dry_run)
        insert_snapshots_schema_aligned(client, all_snaps, cfg.dry_run)

        # ---- Auto discover once per day (UTC) ----
        if cfg.discovery_keywords:
            today = datetime.now(timezone.utc).date().isoformat()
            last = get_state(client, "auto_discover_last_run_utc")
            if last != today:
                try:
                    added = await auto_discover_channels(session, cfg, client)
                    set_state(client, "auto_discover_last_run_utc", today)
                    print(f"[DISCOVER] done added={added}")
                except Exception as e:
                    print(f"[DISCOVER_WARN] {e}")
            else:
                print("[DISCOVER] already ran today, skip")


def main() -> None:
    cfg = load_cfg()
    asyncio.run(run_async(cfg))


if __name__ == "__main__":
    main()
