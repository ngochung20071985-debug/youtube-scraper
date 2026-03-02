#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scraper.py — YouTube -> Supabase worker (Async) + Auto-Discover + AI RPM (Gemini)

✅ Không dùng SQLite
✅ Ghi 3 bảng: channels, videos, snapshots (+ scraper_state nếu có)
✅ Auto-Discover kênh mới mỗi ngày (random 1–2 keyword) ở ĐẦU main()
✅ AI (Gemini) trả về JSON thuần: niche/sentiment/country_target/estimated_rpm
✅ videos UPSERT có đủ: niche, sentiment, country_target, estimated_rpm
✅ Dedupe payload trước khi upsert (tránh lỗi ON CONFLICT DO UPDATE ... a second time)

ENV bắt buộc:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY

ENV YouTube:
- YOUTUBE_API_KEYS="key1,key2,key3"   (khuyến nghị)
  hoặc YOUTUBE_API_KEY="key1"         (fallback)

ENV AI (tuỳ chọn):
- GEMINI_API_KEY=...

Tuỳ chọn:
- CONCURRENCY=10
- MAX_VIDEOS_PER_CHANNEL=50
- MAX_CHANNELS_PER_RUN=0            (0 = tất cả)
- AI_MAX_PER_RUN=30                 (giới hạn số video gọi AI mỗi lần chạy)
- DRY_RUN=0                         (1 = không ghi DB)
"""

from __future__ import annotations

import os
import json
import re
import random
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set

import aiohttp
from supabase import create_client, Client


# =============================
# 1) Auto-Discover keywords (bạn tự sửa list này)
# =============================
TARGET_KEYWORDS = [
    "make money online",
    "faceless youtube automation",
    "scary stories animated",
    "tech gadgets review",
    "movie recap",
    "stoicism",
    "personal finance for beginners",
    "ai tools review",
    "productivity systems",
    "true crime documentary",
    "self improvement",
]

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"


# =============================
# Helpers
# =============================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def safe_str(x: Any) -> str:
    return "" if x is None else str(x)

def to_int(x: Any) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0

def to_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def derive_uploads_playlist_id(channel_id: str) -> Optional[str]:
    """
    Fallback uploads playlist:
      UCxxxxxxxxxxxxxxxxxxxxxx -> UUxxxxxxxxxxxxxxxxxxxxxx
    """
    channel_id = (channel_id or "").strip()
    if channel_id.startswith("UC") and len(channel_id) > 2:
        return "UU" + channel_id[2:]
    return None

def env_optional(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None

def env_required(name: str) -> str:
    v = env_optional(name)
    if not v:
        raise RuntimeError(f"Thiếu biến môi trường: {name}")
    return v

def env_int(name: str, default: int) -> int:
    v = env_optional(name)
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default

def chunked(xs: List[Any], n: int) -> List[List[Any]]:
    return [xs[i:i+n] for i in range(0, len(xs), n)]

def dedupe_rows(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    """Tránh lỗi Postgres: ON CONFLICT DO UPDATE cannot affect row a second time."""
    seen: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        k = safe_str(r.get(key)).strip()
        if not k:
            continue
        seen[k] = r
    return list(seen.values())


# =============================
# Config
# =============================
@dataclass
class Cfg:
    supabase_url: str
    supabase_key: str
    youtube_keys: List[str]
    concurrency: int = 10
    max_videos_per_channel: int = 50
    max_channels_per_run: int = 0
    dry_run: bool = False
    gemini_api_key: Optional[str] = None
    ai_max_per_run: int = 30

def load_cfg() -> Cfg:
    ykeys = env_optional("YOUTUBE_API_KEYS")
    if ykeys:
        keys = [k.strip() for k in ykeys.split(",") if k.strip()]
    else:
        k1 = env_optional("YOUTUBE_API_KEY")
        keys = [k1] if k1 else []

    return Cfg(
        supabase_url=env_required("SUPABASE_URL"),
        supabase_key=env_required("SUPABASE_SERVICE_ROLE_KEY"),
        youtube_keys=keys,
        concurrency=env_int("CONCURRENCY", 10),
        max_videos_per_channel=env_int("MAX_VIDEOS_PER_CHANNEL", 50),
        max_channels_per_run=env_int("MAX_CHANNELS_PER_RUN", 0),
        dry_run=(env_optional("DRY_RUN", "0") == "1"),
        gemini_api_key=env_optional("GEMINI_API_KEY"),
        ai_max_per_run=env_int("AI_MAX_PER_RUN", 30),
    )


# =============================
# Supabase
# =============================
def supa(cfg: Cfg) -> Client:
    return create_client(cfg.supabase_url, cfg.supabase_key)

def _supa_safe_upsert_scraper_state(client: Client, payload: Dict[str, Any]) -> None:
    """
    scraper_state schema có thể khác nhau giữa các project.
    -> Upsert thử đầy đủ, nếu lỗi cột không tồn tại -> retry với payload tối thiểu.
    """
    payload = dict(payload)
    payload.setdefault("id", 1)
    try:
        client.table("scraper_state").upsert(payload, on_conflict="id").execute()
        return
    except Exception as e:
        msg = safe_str(e)
        if "does not exist" in msg or "column" in msg:
            minimal = {k: payload[k] for k in payload.keys() if k in ("id", "status", "message", "updated_at", "pct", "progress", "last_run_at")}
            try:
                client.table("scraper_state").upsert(minimal, on_conflict="id").execute()
            except Exception:
                pass
        else:
            # ignore hard (don't crash)
            pass

def get_scraper_state(client: Client) -> Dict[str, Any]:
    try:
        r = client.table("scraper_state").select("*").order("updated_at", desc=True).limit(1).execute()
        row = (r.data or [None])[0] or {}
        return dict(row)
    except Exception:
        return {}

def fetch_all_channel_ids(client: Client) -> Set[str]:
    out: Set[str] = set()
    # paginate with range (1k/page)
    start = 0
    page = 1000
    while True:
        q = client.table("channels").select("channel_id").range(start, start + page - 1)
        r = q.execute()
        data = r.data or []
        for row in data:
            cid = safe_str(row.get("channel_id")).strip()
            if cid:
                out.add(cid)
        if len(data) < page:
            break
        start += page
        if start > 200000:  # safety
            break
    return out

def list_channels_to_scan(client: Client, limit: int = 0) -> List[str]:
    q = client.table("channels").select("channel_id").order("created_at", desc=False)
    if limit and limit > 0:
        q = q.limit(limit)
        r = q.execute()
        return [safe_str(x.get("channel_id")).strip() for x in (r.data or []) if safe_str(x.get("channel_id")).strip()]

    # paginate
    out: List[str] = []
    start = 0
    page = 1000
    while True:
        r = client.table("channels").select("channel_id").order("created_at", desc=False).range(start, start + page - 1).execute()
        data = r.data or []
        out.extend([safe_str(x.get("channel_id")).strip() for x in data if safe_str(x.get("channel_id")).strip()])
        if len(data) < page:
            break
        start += page
        if start > 200000:
            break
    return out

def upsert_channels(client: Client, rows: List[Dict[str, Any]], dry_run: bool) -> None:
    rows = dedupe_rows(rows, "channel_id")
    if not rows:
        return
    if dry_run:
        print(f"[DRY_RUN] upsert channels: {len(rows)}")
        return
    client.table("channels").upsert(rows, on_conflict="channel_id").execute()

def upsert_videos(client: Client, rows: List[Dict[str, Any]], dry_run: bool) -> None:
    rows = dedupe_rows(rows, "video_id")
    if not rows:
        return
    if dry_run:
        print(f"[DRY_RUN] upsert videos: {len(rows)}")
        return
    # chunk to avoid payload too large
    for ch in chunked(rows, 250):
        client.table("videos").upsert(ch, on_conflict="video_id").execute()

def insert_snapshots(client: Client, rows: List[Dict[str, Any]], dry_run: bool) -> None:
    if not rows:
        return
    if dry_run:
        print(f"[DRY_RUN] insert snapshots: {len(rows)}")
        return
    for ch in chunked(rows, 500):
        client.table("snapshots").insert(ch).execute()

def fetch_existing_video_ai_fields(client: Client, video_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Map video_id -> existing niche/sentiment/country_target/estimated_rpm (để khỏi gọi AI lại)."""
    mp: Dict[str, Dict[str, Any]] = {}
    if not video_ids:
        return mp
    for ch in chunked(video_ids, 200):
        r = client.table("videos").select("video_id,niche,sentiment,country_target,estimated_rpm").in_("video_id", ch).execute()
        for row in (r.data or []):
            vid = safe_str(row.get("video_id")).strip()
            if vid:
                mp[vid] = row
    return mp


# =============================
# YouTube API — key pool
# =============================
class QuotaExceeded(RuntimeError):
    pass

class ApiKeyPool:
    def __init__(self, keys: List[str]):
        keys = [k.strip() for k in keys if k and k.strip()]
        if not keys:
            raise RuntimeError("Không có YouTube API key (YOUTUBE_API_KEYS hoặc YOUTUBE_API_KEY).")
        self._keys = keys
        self._i = 0
        self._lock = asyncio.Lock()

    async def current(self) -> str:
        async with self._lock:
            return self._keys[self._i]

    async def rotate(self) -> str:
        async with self._lock:
            self._i = (self._i + 1) % len(self._keys)
            return self._keys[self._i]

    async def size(self) -> int:
        return len(self._keys)

def _parse_yt_error(text: str) -> Tuple[str, str]:
    try:
        j = json.loads(text)
        err = j.get("error") or {}
        msg = safe_str(err.get("message"))
        errors = err.get("errors") or []
        reason = safe_str(errors[0].get("reason")) if errors else ""
        return reason, msg
    except Exception:
        return "", (text or "")[:250]

async def fetch_json_youtube(
    session: aiohttp.ClientSession,
    pool: ApiKeyPool,
    endpoint: str,
    params: Dict[str, Any],
    *,
    retries: int = 2,
    backoff: float = 1.5,
) -> Dict[str, Any]:
    """
    Rotate key only for keyInvalid/accessNotConfigured.
    If quotaExceeded -> raise QuotaExceeded (stop safely).
    """
    last_err: Optional[Exception] = None

    for attempt in range(retries + 1):
        api_key = await pool.current()
        p = dict(params)
        p["key"] = api_key

        try:
            async with session.get(endpoint, params=p, timeout=aiohttp.ClientTimeout(total=35)) as resp:
                txt = await resp.text()
                if resp.status < 400:
                    return await resp.json()

                reason, msg = _parse_yt_error(txt)

                if resp.status == 403 and reason in ("quotaExceeded", "dailyLimitExceeded", "userRateLimitExceeded"):
                    raise QuotaExceeded(f"YouTube quota exceeded: {reason} — {msg}")

                if resp.status in (400, 403) and reason in ("keyInvalid", "accessNotConfigured"):
                    if await pool.size() > 1:
                        nxt = await pool.rotate()
                        print(f"[WARN] Key lỗi ({reason}). Đổi key -> {nxt[:6]}…")
                        await asyncio.sleep(0.2)
                        continue

                raise RuntimeError(f"HTTP {resp.status} reason={reason} msg={msg}")

        except QuotaExceeded:
            raise
        except Exception as e:
            last_err = e
            await asyncio.sleep(backoff ** attempt)

    raise RuntimeError(f"Request failed after retries: {endpoint}") from last_err


# =============================
# YouTube wrappers
# =============================
async def yt_search_channels(session: aiohttp.ClientSession, pool: ApiKeyPool, keyword: str) -> List[Dict[str, Any]]:
    url = f"{YOUTUBE_API_BASE}/search"
    params = {"part": "snippet", "q": keyword, "type": "channel", "maxResults": 50, "order": "relevance"}
    data = await fetch_json_youtube(session, pool, url, params)
    return data.get("items") or []

async def yt_channels_by_ids(session: aiohttp.ClientSession, pool: ApiKeyPool, ids: List[str]) -> List[Dict[str, Any]]:
    if not ids:
        return []
    url = f"{YOUTUBE_API_BASE}/channels"
    items: List[Dict[str, Any]] = []
    for ch in chunked(ids, 50):
        params = {"part": "snippet,statistics,contentDetails", "id": ",".join(ch), "maxResults": 50}
        data = await fetch_json_youtube(session, pool, url, params)
        items.extend(data.get("items") or [])
    return items

async def yt_playlist_items_video_ids(
    session: aiohttp.ClientSession,
    pool: ApiKeyPool,
    uploads_playlist_id: str,
    *,
    limit: int,
) -> List[str]:
    url = f"{YOUTUBE_API_BASE}/playlistItems"
    video_ids: List[str] = []
    page_token: Optional[str] = None

    while len(video_ids) < limit:
        params: Dict[str, Any] = {
            "part": "contentDetails",
            "playlistId": uploads_playlist_id,
            "maxResults": min(50, limit - len(video_ids)),
        }
        if page_token:
            params["pageToken"] = page_token

        data = await fetch_json_youtube(session, pool, url, params)
        for it in data.get("items") or []:
            cd = it.get("contentDetails") or {}
            vid = cd.get("videoId")
            if vid:
                video_ids.append(vid)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return video_ids

async def yt_videos_by_ids(session: aiohttp.ClientSession, pool: ApiKeyPool, ids: List[str]) -> List[Dict[str, Any]]:
    if not ids:
        return []
    url = f"{YOUTUBE_API_BASE}/videos"
    items: List[Dict[str, Any]] = []
    for ch in chunked(ids, 50):
        params = {"part": "snippet,statistics,contentDetails", "id": ",".join(ch), "maxResults": 50}
        data = await fetch_json_youtube(session, pool, url, params)
        items.extend(data.get("items") or [])
    return items


# =============================
# 2) Auto-Discover (run first in main)
# =============================
def _should_run_discover_daily(state: Dict[str, Any]) -> bool:
    """Chỉ chạy 1 lần / 24h để bảo vệ quota."""
    last = state.get("last_discover_at")
    if not last:
        return True
    try:
        dt = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
        return (utc_now() - dt) >= timedelta(hours=24)
    except Exception:
        return True

async def auto_discover_new_channels(
    session: aiohttp.ClientSession,
    pool: ApiKeyPool,
    client: Client,
) -> int:
    """
    - Random 1–2 keyword từ TARGET_KEYWORDS
    - search.list type=channel maxResults=50
    - Lấy channel_id/title/avatar_url
    - Nếu chưa có -> UPSERT vào channels
    - Enrich thêm subscribers/title/avatar bằng channels.list (rẻ quota)
    """
    if not TARGET_KEYWORDS:
        return 0

    existing = fetch_all_channel_ids(client)

    k = 1 if len(TARGET_KEYWORDS) == 1 else random.choice([1, 2])
    picked = random.sample(TARGET_KEYWORDS, k=k)
    print(f"[INFO] discover keywords: {picked}")

    found_ids: List[str] = []

    for kw in picked:
        items = await yt_search_channels(session, pool, kw)
        for it in items:
            cid = safe_str(((it.get("id") or {}).get("channelId"))).strip()
            if not cid or cid in existing:
                continue
            found_ids.append(cid)
            existing.add(cid)

    found_ids = list(dict.fromkeys(found_ids))
    if not found_ids:
        print("[INFO] discover: no new channels")
        return 0

    # Enrich using channels.list
    ch_items = await yt_channels_by_ids(session, pool, found_ids[:50])
    rows: List[Dict[str, Any]] = []
    for it in ch_items:
        cid = safe_str(it.get("id")).strip()
        if not cid:
            continue
        sn = it.get("snippet") or {}
        stt = it.get("statistics") or {}
        thumbs = sn.get("thumbnails") or {}
        avatar = safe_str((thumbs.get("high") or thumbs.get("default") or {}).get("url")).strip()

        rows.append({
            "channel_id": cid,
            "title": safe_str(sn.get("title")).strip(),
            "handle": safe_str(sn.get("customUrl")).strip(),
            "avatar_url": avatar,
            "subscribers": to_int(stt.get("subscriberCount")),
        })

    rows = dedupe_rows(rows, "channel_id")
    if rows:
        upsert_channels(client, rows, dry_run=False)
        print(f"[INFO] discover: inserted/upserted {len(rows)} channels")
        return len(rows)

    return 0


# =============================
# 3) Gemini AI — strict JSON
# =============================
ALLOWED_NICHE = ["Tài chính", "Công nghệ", "Giáo dục", "Giải trí", "Gaming", "Tin tức", "Đời sống", "Truyện/Phim", "Khác"]
ALLOWED_SENT = ["Tích cực", "Tiêu cực", "Trung lập", "Giật gân", "Hài hước", "Bí ẩn"]

def _sanitize_ai(niche: str, sentiment: str, country: str, rpm: Optional[float]) -> Tuple[str, str, str, Optional[float]]:
    niche = niche.strip()
    sentiment = sentiment.strip()
    country = country.strip()

    if niche not in ALLOWED_NICHE:
        niche = "Khác"
    if sentiment not in ALLOWED_SENT:
        sentiment = "Trung lập"
    if country == "":
        country = "Unknown"
    if rpm is not None:
        # clamp to sane range
        rpm = max(0.01, min(100.0, float(rpm)))
    return niche, sentiment, country, rpm

async def analyze_video_with_ai(
    session: aiohttp.ClientSession,
    gemini_key: str,
    title: str,
    description: str,
) -> Tuple[str, str, str, Optional[float]]:
    """
    Return (niche, sentiment, country_target, estimated_rpm)
    JSON strict (no markdown).
    """
    if not gemini_key:
        return "", "", "", None

    prompt = f"""
Bạn là hệ thống phân loại nội dung YouTube. Trả về JSON thuần túy (KHÔNG markdown, KHÔNG giải thích).
Chỉ trả về 1 object JSON duy nhất với 4 trường:
- niche: chọn 1 trong {ALLOWED_NICHE}
- sentiment: chọn 1 trong {ALLOWED_SENT}
- country_target: đoán quốc gia mục tiêu (vd: Vietnam, United States, Japan, ...)
- estimated_rpm: số float USD (RPM trung bình) dựa vào ngách và quốc gia

Tiêu đề: {title}

Mô tả: {description[:2500]}
""".strip()

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 256},
    }

    try:
        async with session.post(
            f"{GEMINI_ENDPOINT}?key={gemini_key}",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=45),
        ) as resp:
            if resp.status >= 400:
                return "", "", "", None
            j = await resp.json()

        cand = (j.get("candidates") or [{}])[0]
        parts = (((cand.get("content") or {}).get("parts")) or [])
        text_out = " ".join([safe_str(p.get("text")) for p in parts]).strip()
        if not text_out:
            return "", "", "", None

        # remove ```json fences if any
        text_out = re.sub(r"^```(?:json)?\s*", "", text_out.strip(), flags=re.I)
        text_out = re.sub(r"\s*```$", "", text_out.strip())

        m = re.search(r"\{.*\}", text_out, re.S)
        if not m:
            return "", "", "", None
        obj = json.loads(m.group(0))

        niche = safe_str(obj.get("niche"))
        sentiment = safe_str(obj.get("sentiment"))
        country = safe_str(obj.get("country_target"))
        rpm = obj.get("estimated_rpm", None)
        rpm_f = to_float(rpm)

        niche, sentiment, country, rpm_f = _sanitize_ai(niche, sentiment, country, rpm_f)
        return niche, sentiment, country, rpm_f
    except Exception:
        return "", "", "", None


# =============================
# Scan channel
# =============================
async def scan_one_channel(
    session: aiohttp.ClientSession,
    pool: ApiKeyPool,
    client: Client,
    channel_id: str,
    cfg: Cfg,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    # Get channel (snippet/stats/contentDetails)
    try:
        items = await yt_channels_by_ids(session, pool, [channel_id])
    except QuotaExceeded as qe:
        print(f"[FATAL][QUOTA] {qe}")
        raise
    except Exception as e:
        print(f"[ERROR] channels.list FAILED for channel_id={channel_id} -> {e}")
        raise

    if not items:
        print(f"[WARN] channels.list returned empty for channel_id={channel_id}. Bỏ qua.")
        return [], [], []
    ch_item = items[0]

    snippet = ch_item.get("snippet") or {}
    stats = ch_item.get("statistics") or {}
    content = ch_item.get("contentDetails") or {}
    rel = (content.get("relatedPlaylists") or {})
    uploads_pid = safe_str(rel.get("uploads")).strip()

    # ----- PRINT: bắt đầu xử lý kênh -----
    ch_title = safe_str(snippet.get("title")).strip() or "(No title)"
    print(f"\n[SCAN] Đang xử lý kênh: {ch_title} (ID: {channel_id})")
# ----- FALLBACK: nếu API không có uploads_pid thì tự tạo UC->UU -----
    if not uploads_pid:
        fallback = derive_uploads_playlist_id(channel_id)
        if fallback:
            uploads_pid = fallback
            print(f"[SCAN] Đã tạo Uploads Playlist ID: {uploads_pid} (fallback UC→UU)")
        else:
            print(f"[ERROR] Không lấy được uploads_playlist_id và cũng không tạo được fallback từ channel_id={channel_id}. Bỏ qua kênh này.")
            return [], [], []
    else:
        print(f"[SCAN] Uploads Playlist ID (API): {uploads_pid}")

    channels_rows = [{
        "channel_id": channel_id,
        "title": safe_str(snippet.get("title")),
        "handle": safe_str(snippet.get("customUrl")),
        "avatar_url": safe_str(((snippet.get("thumbnails") or {}).get("high") or (snippet.get("thumbnails") or {}).get("default") or {}).get("url")),
        "subscribers": to_int(stats.get("subscriberCount")),
    }]

    # ----- Gọi playlistItems.list để lấy videoIds -----
    try:
        video_ids: List[str] = await yt_playlist_items_video_ids(
            session, pool, uploads_pid, limit=cfg.max_videos_per_channel
        )
        print(f"[SCAN] Tìm thấy {len(video_ids)} video mới cho kênh này.")
    except QuotaExceeded as qe:
        print(f"[FATAL][QUOTA] {qe}")
        raise
    except Exception as e:
        print(f"[ERROR] playlistItems.list FAILED for uploads_pid={uploads_pid} channel_id={channel_id} -> {e}")
        raise

    if not video_ids:
        print(f"[WARN] Kênh {ch_title} không trả về video nào từ uploads playlist. (uploads_pid={uploads_pid})")
    try:
        v_items = await yt_videos_by_ids(session, pool, video_ids)
    except QuotaExceeded as qe:
        print(f"[FATAL][QUOTA] {qe}")
        raise
    except Exception as e:
        print(f"[ERROR] videos.list FAILED for channel_id={channel_id} (n_ids={len(video_ids)}) -> {e}")
        raise

    now_iso = utc_now().isoformat()

    videos_rows: List[Dict[str, Any]] = []
    snapshots_rows: List[Dict[str, Any]] = []

    # Skip AI if already filled
    existing_ai = fetch_existing_video_ai_fields(client, [safe_str(it.get("id")).strip() for it in v_items if safe_str(it.get("id")).strip()])
    ai_budget = cfg.ai_max_per_run

    for it in v_items:
        vid = safe_str(it.get("id")).strip()
        if not vid:
            continue

        vs = it.get("snippet") or {}
        stt = it.get("statistics") or {}

        title = safe_str(vs.get("title"))
        desc = safe_str(vs.get("description"))
        published_at = safe_str(vs.get("publishedAt"))

        # Default AI fields from DB if exists
        niche = safe_str((existing_ai.get(vid) or {}).get("niche"))
        sentiment = safe_str((existing_ai.get(vid) or {}).get("sentiment"))
        country_target = safe_str((existing_ai.get(vid) or {}).get("country_target"))
        est_rpm = (existing_ai.get(vid) or {}).get("estimated_rpm", None)

        need_ai = (not niche) or (not sentiment) or (not country_target) or (est_rpm is None)
        if need_ai and cfg.gemini_api_key and ai_budget > 0:
            n2, s2, c2, r2 = await analyze_video_with_ai(session, cfg.gemini_api_key, title, desc)
            if n2:
                niche = n2
            if s2:
                sentiment = s2
            if c2:
                country_target = c2
            if r2 is not None:
                est_rpm = r2
            ai_budget -= 1

        videos_rows.append({
            "video_id": vid,
            "channel_id": channel_id,
            "published_at": published_at,
            "title": title,
            "description": desc,
            "tags_json": json.dumps(vs.get("tags") or [], ensure_ascii=False),

            # ✅ AI fields (cũ + mới)
            "niche": niche,
            "sentiment": sentiment,
            "country_target": country_target,
            "estimated_rpm": float(est_rpm) if est_rpm is not None else None,
        })

        snapshots_rows.append({
            "video_id": vid,
            "captured_at": now_iso,
            "view_count": to_int(stt.get("viewCount")),
            "like_count": to_int(stt.get("likeCount")),
            "comment_count": to_int(stt.get("commentCount")),
        })

    return channels_rows, videos_rows, snapshots_rows


# =============================
# Main loop
# =============================
async def run_async(cfg: Cfg) -> None:
    if not cfg.youtube_keys:
        raise RuntimeError("Thiếu YouTube API key. Set YOUTUBE_API_KEYS hoặc YOUTUBE_API_KEY.")

    pool = ApiKeyPool(cfg.youtube_keys)
    client = supa(cfg)

    state = get_scraper_state(client)

    _supa_safe_upsert_scraper_state(client, {
        "id": 1,
        "status": "running",
        "message": "Starting…",
        "updated_at": utc_now().isoformat(),
        "pct": 0,
        "progress": 0,
    })

    timeout = aiohttp.ClientTimeout(total=60)
    sem = asyncio.Semaphore(cfg.concurrency)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # ✅ Auto-Discover — run FIRST (but only once per 24h)
        if _should_run_discover_daily(state):
            try:
                added = await auto_discover_new_channels(session, pool, client)
                _supa_safe_upsert_scraper_state(client, {
                    "id": 1,
                    "status": "running",
                    "message": f"Auto-discover added {added} channels",
                    "updated_at": utc_now().isoformat(),
                    "last_discover_at": utc_now().isoformat(),
                    "last_discover_added": added,
                })
            except QuotaExceeded as qe:
                # Stop safely – can't continue scanning reliably if quota is exhausted
                _supa_safe_upsert_scraper_state(client, {
                    "id": 1,
                    "status": "quota_exhausted",
                    "message": str(qe)[:500],
                    "updated_at": utc_now().isoformat(),
                })
                raise
            except Exception as e:
                print(f"[WARN] auto_discover failed (ignored): {e}")
        else:
            print("[INFO] auto_discover skipped (already ran <24h).")

        # Load channels AFTER discover
        channel_ids = list_channels_to_scan(client, cfg.max_channels_per_run)
        if not channel_ids:
            _supa_safe_upsert_scraper_state(client, {
                "id": 1,
                "status": "ok",
                "message": "No channels to scan",
                "updated_at": utc_now().isoformat(),
                "pct": 100,
                "progress": 0,
            })
            return

        all_ch: List[Dict[str, Any]] = []
        all_v: List[Dict[str, Any]] = []
        all_s: List[Dict[str, Any]] = []

        async def worker(cid: str):
            async with sem:
                return await scan_one_channel(session, pool, client, cid, cfg)

        tasks = [asyncio.create_task(worker(cid)) for cid in channel_ids]
        done = 0

        for fut in asyncio.as_completed(tasks):
            try:
                ch_rows, v_rows, s_rows = await fut
                all_ch.extend(ch_rows)
                all_v.extend(v_rows)
                all_s.extend(s_rows)
            except QuotaExceeded as qe:
                _supa_safe_upsert_scraper_state(client, {
                    "id": 1,
                    "status": "quota_exhausted",
                    "message": str(qe)[:500],
                    "updated_at": utc_now().isoformat(),
                })
                raise
            except Exception as e:
                print(f"[WARN] scan channel fail: {e}"); import traceback; traceback.print_exc()
            finally:
                done += 1
                if done % 2 == 0 or done == len(channel_ids):
                    _supa_safe_upsert_scraper_state(client, {
                        "id": 1,
                        "status": "running",
                        "message": f"Scanning {done}/{len(channel_ids)}",
                        "updated_at": utc_now().isoformat(),
                        "progress": done,
                        "pct": int(done / max(1, len(channel_ids)) * 100),
                    })

        print(f"[INFO] collected: channels={len(all_ch)} videos={len(all_v)} snapshots={len(all_s)}")

        # DB writes (dedupe inside)
        upsert_channels(client, all_ch, cfg.dry_run)
        upsert_videos(client, all_v, cfg.dry_run)        # ✅ includes niche/sentiment/country_target/estimated_rpm
        insert_snapshots(client, all_s, cfg.dry_run)

    _supa_safe_upsert_scraper_state(client, {
        "id": 1,
        "status": "ok",
        "message": "Done",
        "updated_at": utc_now().isoformat(),
        "last_run_at": utc_now().isoformat(),
        "pct": 100,
        "progress": len(channel_ids),
    })


def main() -> None:
    cfg = load_cfg()
    try:
        asyncio.run(run_async(cfg))
    except QuotaExceeded:
        print("[FATAL] quotaExceeded -> dừng an toàn (exit 2)")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
