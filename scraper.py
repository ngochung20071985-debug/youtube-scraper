#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scraper.py — Background worker quét YouTube API v3 và ghi vào Supabase (PostgreSQL)

✅ Async + aiohttp
✅ Ghi dữ liệu vào: channels, videos, snapshots (+ scraper_state nếu có)

ENV bắt buộc (GitHub Actions Secrets):
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY

ENV YouTube:
- YOUTUBE_API_KEYS="key1,key2,key3"   (ưu tiên)
  hoặc YOUTUBE_API_KEY="key1"         (fallback)

⚠️ Lưu ý quan trọng về QUOTA:
- Quota của YouTube Data API gắn với *Google Cloud Project*.
- Việc “xoay key để vượt quota” thường KHÔNG giải quyết được nếu các key nằm trong cùng project,
  và có thể vi phạm ToS tùy cách bạn triển khai.
- File này chỉ hỗ trợ:
  ✅ fallback sang key khác khi key *không hợp lệ / bị tắt dịch vụ* (keyInvalid / accessNotConfigured)
  ❌ KHÔNG tự xoay key để “vượt quota” khi gặp quotaExceeded.
  -> Khi gặp quotaExceeded, scraper sẽ dừng an toàn và in log cảnh báo.

ENV tuỳ chọn:
- CONCURRENCY=10
- MAX_VIDEOS_PER_CHANNEL=50
- MAX_CHANNELS_PER_RUN=0          (0 = không giới hạn)
- DRY_RUN=0                       (1 = chỉ in log, không ghi DB)
- DISCOVER_KEYWORDS="a,b,c"        (auto discover channels mỗi ngày 1 lần, cuối run)
- DISCOVER_MIN_SUBS=1000
- DISCOVER_MAX_SUBS=50000
- GEMINI_API_KEY=...               (nếu muốn AI classify niche/sentiment)
"""

from __future__ import annotations

import os
import json
import re
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from supabase import create_client, Client

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

# -----------------------------
# Helpers
# -----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def safe_str(x: Any) -> str:
    return "" if x is None else str(x)

def to_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0

def _env_optional(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None

def _env_required(name: str) -> str:
    v = _env_optional(name)
    if not v:
        raise RuntimeError(f"Thiếu biến môi trường: {name}")
    return v

def _env_int(name: str, default: int) -> int:
    v = _env_optional(name)
    if not v:
        return int(default)
    try:
        return int(v)
    except Exception:
        return int(default)

def chunked(xs: List[Any], n: int) -> List[List[Any]]:
    return [xs[i:i+n] for i in range(0, len(xs), n)]

def dedupe_rows(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    """Tránh lỗi Postgres: 'ON CONFLICT DO UPDATE command cannot affect row a second time'."""
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        k = safe_str(r.get(key)).strip()
        if not k:
            continue
        out[k] = r
    return list(out.values())

# -----------------------------
# API Key pool (fallback only)
# -----------------------------
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

# -----------------------------
# Config
# -----------------------------
@dataclass
class Cfg:
    supabase_url: str
    supabase_key: str
    youtube_keys: List[str]
    concurrency: int = 10
    max_videos_per_channel: int = 50
    max_channels_per_run: int = 0
    dry_run: bool = False
    discover_keywords: List[str] = None
    discover_min_subs: int = 1000
    discover_max_subs: int = 50000
    gemini_api_key: Optional[str] = None

def load_cfg() -> Cfg:
    ykeys = _env_optional("YOUTUBE_API_KEYS")
    if ykeys:
        keys = [k.strip() for k in ykeys.split(",") if k.strip()]
    else:
        k1 = _env_optional("YOUTUBE_API_KEY")
        keys = [k1] if k1 else []

    kw = _env_optional("DISCOVER_KEYWORDS")
    discover_keywords = [x.strip() for x in kw.split(",") if x.strip()] if kw else []

    return Cfg(
        supabase_url=_env_required("SUPABASE_URL"),
        supabase_key=_env_required("SUPABASE_SERVICE_ROLE_KEY"),
        youtube_keys=keys,
        concurrency=_env_int("CONCURRENCY", 10),
        max_videos_per_channel=_env_int("MAX_VIDEOS_PER_CHANNEL", 50),
        max_channels_per_run=_env_int("MAX_CHANNELS_PER_RUN", 0),
        dry_run=(_env_optional("DRY_RUN", "0") == "1"),
        discover_keywords=discover_keywords,
        discover_min_subs=_env_int("DISCOVER_MIN_SUBS", 1000),
        discover_max_subs=_env_int("DISCOVER_MAX_SUBS", 50000),
        gemini_api_key=_env_optional("GEMINI_API_KEY"),
    )

def supa(cfg: Cfg) -> Client:
    return create_client(cfg.supabase_url, cfg.supabase_key)

# -----------------------------
# YouTube fetch with fallback (NOT quota bypass)
# -----------------------------
def _parse_yt_error(text: str) -> Tuple[str, str]:
    """
    Return (reason, message)
    """
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
    backoff: float = 1.4,
) -> Dict[str, Any]:
    """
    - Inject key from ApiKeyPool.
    - Nếu keyInvalid / accessNotConfigured -> rotate và retry.
    - Nếu quotaExceeded -> raise QuotaExceeded (STOP).
    """
    last_err: Optional[Exception] = None

    for attempt in range(retries + 1):
        api_key = await pool.current()
        p = dict(params)
        p["key"] = api_key

        try:
            async with session.get(
                endpoint,
                params=p,
                timeout=aiohttp.ClientTimeout(total=35),
            ) as resp:
                txt = await resp.text()
                if resp.status < 400:
                    return await resp.json()

                reason, msg = _parse_yt_error(txt)

                # Quota => STOP (không xoay để "vượt quota")
                if resp.status == 403 and reason in ("quotaExceeded", "dailyLimitExceeded", "userRateLimitExceeded"):
                    raise QuotaExceeded(f"Quota exceeded: reason={reason} msg={msg}")

                # Key invalid / service not enabled => rotate
                if resp.status in (400, 403) and reason in ("keyInvalid", "accessNotConfigured", "forbidden"):
                    if await pool.size() > 1:
                        nxt = await pool.rotate()
                        print(f"[WARN] Key lỗi ({reason}). Đổi sang key tiếp theo. now={nxt[:6]}…")
                        await asyncio.sleep(0.2)
                        continue

                raise RuntimeError(f"HTTP {resp.status} reason={reason} msg={msg}")

        except QuotaExceeded:
            raise
        except Exception as e:
            last_err = e
            await asyncio.sleep(backoff ** attempt)

    raise RuntimeError(f"Request failed after retries: {endpoint} params={list(params.keys())}") from last_err

# -----------------------------
# YouTube API wrappers
# -----------------------------
async def yt_channels(session: aiohttp.ClientSession, pool: ApiKeyPool, channel_id: str) -> Optional[Dict[str, Any]]:
    url = f"{YOUTUBE_API_BASE}/channels"
    params = {
        "part": "snippet,statistics,contentDetails",
        "id": channel_id,
        "maxResults": 1,
    }
    data = await fetch_json_youtube(session, pool, url, params)
    items = data.get("items") or []
    return items[0] if items else None

async def yt_videos(session: aiohttp.ClientSession, pool: ApiKeyPool, video_ids: List[str]) -> List[Dict[str, Any]]:
    if not video_ids:
        return []
    url = f"{YOUTUBE_API_BASE}/videos"
    items: List[Dict[str, Any]] = []
    for chunk in chunked(video_ids, 50):
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(chunk),
            "maxResults": 50,
        }
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
    """Lấy video_ids từ uploads playlist (rẻ quota hơn search.list)."""
    url = f"{YOUTUBE_API_BASE}/playlistItems"
    video_ids: List[str] = []
    page_token: Optional[str] = None
    while len(video_ids) < limit:
        params = {
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

async def yt_search_channels_by_keyword(session: aiohttp.ClientSession, pool: ApiKeyPool, keyword: str, max_results: int = 25) -> List[str]:
    """Auto-discover: tìm channelId theo keyword (type=channel)."""
    url = f"{YOUTUBE_API_BASE}/search"
    params = {
        "part": "snippet",
        "q": keyword,
        "type": "channel",
        "maxResults": min(50, max_results),
        "order": "relevance",
    }
    data = await fetch_json_youtube(session, pool, url, params)
    out: List[str] = []
    for it in data.get("items") or []:
        cid = (((it.get("id") or {}).get("channelId")) or "")
        if cid:
            out.append(str(cid))
    return out

# -----------------------------
# Gemini AI (optional)
# -----------------------------
async def analyze_video_with_ai(session: aiohttp.ClientSession, gemini_key: str, title: str, description: str) -> Tuple[str, str]:
    """
    Return (niche, sentiment). Nếu lỗi -> ("","")
    """
    if not gemini_key:
        return "", ""

    prompt = f"""
Bạn là hệ thống phân loại nội dung YouTube.
Hãy đọc TIÊU ĐỀ và MÔ TẢ dưới đây, rồi trả về JSON DUY NHẤT theo format:
{{"niche":"...","sentiment":"..."}}

- niche: ngách chủ đề chính (ví dụ: Tài chính, Tâm lý học, Game, Giáo dục, Khoa học, Vlog, Drama, Tin tức, Review, ...).
- sentiment: phong cách/cảm xúc (ví dụ: Giật gân, Hài hước, Giáo dục, Truyền cảm hứng, Điều tra, Phẫn nộ, Chill, ...).
- Không thêm text ngoài JSON.

TIÊU ĐỀ: {title}

MÔ TẢ: {description[:2000]}
""".strip()

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 256},
    }

    try:
        async with session.post(
            f"{GEMINI_ENDPOINT}?key={gemini_key}",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=40),
        ) as resp:
            txt = await resp.text()
            if resp.status >= 400:
                return "", ""
            j = await resp.json()
            cand = (j.get("candidates") or [{}])[0]
            parts = (((cand.get("content") or {}).get("parts")) or [])
            text_out = " ".join([safe_str(p.get("text")) for p in parts]).strip()
            if not text_out:
                return "", ""
            # Extract JSON
            m = re.search(r"\{.*\}", text_out, re.S)
            if not m:
                return "", ""
            obj = json.loads(m.group(0))
            niche = safe_str(obj.get("niche")).strip()
            sentiment = safe_str(obj.get("sentiment")).strip()
            return niche[:64], sentiment[:64]
    except Exception:
        return "", ""

# -----------------------------
# Supabase writes (dedup safe)
# -----------------------------
def upsert_channels(client: Client, rows: List[Dict[str, Any]], *, dry_run: bool) -> None:
    rows = dedupe_rows(rows, "channel_id")
    if not rows:
        return
    if dry_run:
        print(f"[DRY_RUN] upsert channels: {len(rows)}")
        return
    client.table("channels").upsert(rows, on_conflict="channel_id").execute()

def upsert_videos(client: Client, rows: List[Dict[str, Any]], *, dry_run: bool) -> None:
    rows = dedupe_rows(rows, "video_id")
    if not rows:
        return
    if dry_run:
        print(f"[DRY_RUN] upsert videos: {len(rows)}")
        return
    # chunk để tránh payload quá lớn
    for chunk in chunked(rows, 250):
        client.table("videos").upsert(chunk, on_conflict="video_id").execute()

def insert_snapshots(client: Client, rows: List[Dict[str, Any]], *, dry_run: bool) -> None:
    if not rows:
        return
    if dry_run:
        print(f"[DRY_RUN] insert snapshots: {len(rows)}")
        return
    for chunk in chunked(rows, 500):
        client.table("snapshots").insert(chunk).execute()

def fetch_existing_channel_ids(client: Client) -> set:
    s = set()
    r = client.table("channels").select("channel_id").limit(10000).execute()
    for row in r.data or []:
        cid = safe_str(row.get("channel_id")).strip()
        if cid:
            s.add(cid)
    return s

def list_channels_to_scan(client: Client, *, limit: int = 0) -> List[str]:
    q = client.table("channels").select("channel_id").order("created_at", desc=False)
    if limit and limit > 0:
        q = q.limit(int(limit))
    r = q.execute()
    out = []
    for row in r.data or []:
        cid = safe_str(row.get("channel_id")).strip()
        if cid:
            out.append(cid)
    return out

def get_scraper_state(client: Client) -> Dict[str, Any]:
    try:
        r = client.table("scraper_state").select("*").order("updated_at", desc=True).limit(1).execute()
        row = (r.data or [None])[0] or {}
        return dict(row)
    except Exception:
        return {}

def upsert_scraper_state(client: Client, payload: Dict[str, Any], *, dry_run: bool) -> None:
    if dry_run:
        print("[DRY_RUN] scraper_state:", payload)
        return
    try:
        # dùng single row id=1 nếu có, else insert
        payload = dict(payload)
        payload.setdefault("id", 1)
        client.table("scraper_state").upsert(payload, on_conflict="id").execute()
    except Exception:
        pass

# -----------------------------
# Main scan logic
# -----------------------------
async def scan_one_channel(
    session: aiohttp.ClientSession,
    pool: ApiKeyPool,
    client: Client,
    channel_id: str,
    cfg: Cfg,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Return (channels_rows, videos_rows, snapshots_rows)
    """
    ch_item = await yt_channels(session, pool, channel_id)
    if not ch_item:
        return [], [], []

    snippet = ch_item.get("snippet") or {}
    stats = ch_item.get("statistics") or {}
    content = ch_item.get("contentDetails") or {}
    rel = (content.get("relatedPlaylists") or {})
    uploads_pid = safe_str(rel.get("uploads")).strip()

    channels_rows = [{
        "channel_id": channel_id,
        "title": safe_str(snippet.get("title")),
        "handle": safe_str(snippet.get("customUrl")),
        "avatar_url": safe_str(((snippet.get("thumbnails") or {}).get("default") or {}).get("url")),
        "subscribers": to_int(stats.get("subscriberCount")),
    }]

    video_ids = []
    if uploads_pid:
        video_ids = await yt_playlist_items_video_ids(session, pool, uploads_pid, limit=cfg.max_videos_per_channel)

    v_items = await yt_videos(session, pool, video_ids)

    videos_rows: List[Dict[str, Any]] = []
    snapshots_rows: List[Dict[str, Any]] = []
    now_iso = utc_now().isoformat()

    # AI optional
    gemini_key = cfg.gemini_api_key or ""

    for it in v_items:
        vid = safe_str(it.get("id")).strip()
        if not vid:
            continue
        vs = it.get("snippet") or {}
        vt = safe_str(vs.get("title"))
        vd = safe_str(vs.get("description"))
        published_at = safe_str(vs.get("publishedAt"))

        niche = ""
        sentiment = ""
        if gemini_key:
            niche, sentiment = await analyze_video_with_ai(session, gemini_key, vt, vd)

        videos_rows.append({
            "video_id": vid,
            "channel_id": channel_id,
            "published_at": published_at,
            "title": vt,
            "description": vd,
            "tags_json": json.dumps(vs.get("tags") or [], ensure_ascii=False),
            "niche": niche,
            "sentiment": sentiment,
        })

        stt = it.get("statistics") or {}
        snapshots_rows.append({
            "video_id": vid,
            "captured_at": now_iso,
            "view_count": to_int(stt.get("viewCount")),
            "like_count": to_int(stt.get("likeCount")),
            "comment_count": to_int(stt.get("commentCount")),
        })

    return channels_rows, videos_rows, snapshots_rows

async def auto_discover_channels(session: aiohttp.ClientSession, pool: ApiKeyPool, client: Client, cfg: Cfg) -> int:
    """
    Tìm kênh mới theo keywords -> lọc sub [min,max] -> insert nếu chưa có.
    Chạy 1 lần/ngày (được kiểm soát bằng scraper_state).
    """
    if not cfg.discover_keywords:
        return 0

    # Check daily gate using scraper_state
    state = get_scraper_state(client)
    last = safe_str(state.get("discover_last_date")).strip()
    today = date.today().isoformat()
    if last == today:
        print("[INFO] auto_discover: đã chạy hôm nay, skip.")
        return 0

    existing = fetch_existing_channel_ids(client)
    found: set = set()

    # search channels
    for kw in cfg.discover_keywords:
        try:
            ids = await yt_search_channels_by_keyword(session, pool, kw, max_results=25)
            for cid in ids:
                if cid and cid not in existing:
                    found.add(cid)
        except QuotaExceeded:
            raise
        except Exception as e:
            print(f"[WARN] discover search fail kw={kw}: {e}")

    if not found:
        upsert_scraper_state(client, {"id": 1, "discover_last_date": today, "updated_at": utc_now().isoformat()}, dry_run=cfg.dry_run)
        return 0

    # fetch channel stats and filter subs
    new_rows: List[Dict[str, Any]] = []
    for chunk in chunked(list(found), 50):
        url = f"{YOUTUBE_API_BASE}/channels"
        params = {"part": "snippet,statistics", "id": ",".join(chunk), "maxResults": 50}
        data = await fetch_json_youtube(session, pool, url, params)
        for it in data.get("items") or []:
            cid = safe_str(it.get("id")).strip()
            if not cid or cid in existing:
                continue
            stats = it.get("statistics") or {}
            subs = to_int(stats.get("subscriberCount"))
            if subs < cfg.discover_min_subs or subs > cfg.discover_max_subs:
                continue
            sn = it.get("snippet") or {}
            new_rows.append({
                "channel_id": cid,
                "title": safe_str(sn.get("title")),
                "handle": safe_str(sn.get("customUrl")),
                "avatar_url": safe_str(((sn.get("thumbnails") or {}).get("default") or {}).get("url")),
                "subscribers": subs,
            })

    new_rows = dedupe_rows(new_rows, "channel_id")
    if new_rows:
        print(f"[INFO] auto_discover: thêm {len(new_rows)} kênh mới.")
        upsert_channels(client, new_rows, dry_run=cfg.dry_run)

    upsert_scraper_state(client, {"id": 1, "discover_last_date": today, "updated_at": utc_now().isoformat()}, dry_run=cfg.dry_run)
    return len(new_rows)

async def run_async(cfg: Cfg) -> None:
    pool = ApiKeyPool(cfg.youtube_keys)
    client = supa(cfg)

    channel_ids = list_channels_to_scan(client, limit=cfg.max_channels_per_run)
    if not channel_ids:
        print("[INFO] Không có kênh nào trong bảng channels.")
        return

    sem = asyncio.Semaphore(cfg.concurrency)
    timeout = aiohttp.ClientTimeout(total=50)

    upsert_scraper_state(client, {
        "id": 1,
        "status": "running",
        "message": f"Scanning {len(channel_ids)} channels",
        "updated_at": utc_now().isoformat(),
        "run_started_at": utc_now().isoformat(),
        "progress": 0,
        "pct": 0,
    }, dry_run=cfg.dry_run)

    all_ch: List[Dict[str, Any]] = []
    all_v: List[Dict[str, Any]] = []
    all_s: List[Dict[str, Any]] = []

    async with aiohttp.ClientSession(timeout=timeout) as session:

        async def worker(ch_id: str, idx: int):
            async with sem:
                return await scan_one_channel(session, pool, client, ch_id, cfg)

        tasks = [asyncio.create_task(worker(cid, i)) for i, cid in enumerate(channel_ids)]
        done = 0

        for fut in asyncio.as_completed(tasks):
            try:
                ch_rows, v_rows, s_rows = await fut
                all_ch.extend(ch_rows)
                all_v.extend(v_rows)
                all_s.extend(s_rows)
            except QuotaExceeded as qe:
                upsert_scraper_state(client, {
                    "id": 1,
                    "status": "quota_exhausted",
                    "message": str(qe)[:500],
                    "updated_at": utc_now().isoformat(),
                }, dry_run=cfg.dry_run)
                raise
            except Exception as e:
                print(f"[WARN] scan channel fail: {e}")
            finally:
                done += 1
                if done % 2 == 0 or done == len(channel_ids):
                    upsert_scraper_state(client, {
                        "id": 1,
                        "status": "running",
                        "message": f"Scanning... {done}/{len(channel_ids)}",
                        "updated_at": utc_now().isoformat(),
                        "progress": done,
                        "pct": int(done / max(1, len(channel_ids)) * 100),
                    }, dry_run=cfg.dry_run)

        # Write DB
        print(f"[INFO] channels={len(all_ch)} videos={len(all_v)} snapshots={len(all_s)}")
        upsert_channels(client, all_ch, dry_run=cfg.dry_run)
        upsert_videos(client, all_v, dry_run=cfg.dry_run)
        insert_snapshots(client, all_s, dry_run=cfg.dry_run)

        # Auto discover (daily)
        try:
            added = await auto_discover_channels(session, pool, client, cfg)
            if added:
                print(f"[INFO] auto_discover added={added}")
        except QuotaExceeded as qe:
            print(f"[WARN] auto_discover stopped due quota: {qe}")

    upsert_scraper_state(client, {
        "id": 1,
        "status": "ok",
        "message": "Done",
        "updated_at": utc_now().isoformat(),
        "last_run_at": utc_now().isoformat(),
        "progress": len(channel_ids),
        "pct": 100,
    }, dry_run=cfg.dry_run)

def main():
    cfg = load_cfg()
    # Validate YouTube keys existence but allow running even if you only want auto-discover off? -> still needed for scan
    if not cfg.youtube_keys or not any(cfg.youtube_keys):
        raise RuntimeError("Thiếu YouTube API key. Set YOUTUBE_API_KEYS hoặc YOUTUBE_API_KEY.")
    try:
        asyncio.run(run_async(cfg))
    except QuotaExceeded as qe:
        print("[FATAL] Tất cả request bị quotaExceeded. Dừng an toàn.")
        print(f"[FATAL] {qe}")
        raise SystemExit(2)

if __name__ == "__main__":
    main()
