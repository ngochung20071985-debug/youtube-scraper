#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scraper.py — Background worker quét YouTube API v3 và ghi vào Supabase (PostgreSQL)

✅ Async + aiohttp
✅ Ghi dữ liệu vào: channels, videos, snapshots (+ scraper_state nếu có)
✅ Auto-Discover kênh mới ở ĐẦU mỗi run (YouTube Search API, type=channel)
✅ Multi-key: YOUTUBE_API_KEYS="k1,k2,k3" (fallback rotate khi key lỗi / service chưa bật)

ENV bắt buộc:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY

ENV YouTube:
- YOUTUBE_API_KEYS="key1,key2,key3"   (ưu tiên)
  hoặc YOUTUBE_API_KEY="key1"         (fallback)

⚠️ QUOTA:
- Khi gặp quotaExceeded/dailyLimitExceeded/userRateLimitExceeded -> scraper sẽ log cảnh báo và
  **bỏ qua bước auto-discover**, sau đó tiếp tục quét các kênh đã có (không crash).
  (Không cố “vượt quota” bằng cách đổi key để tiếp tục gọi search khi quotaExceeded.)

ENV tuỳ chọn:
- CONCURRENCY=10
- MAX_VIDEOS_PER_CHANNEL=50
- MAX_CHANNELS_PER_RUN=0          (0 = không giới hạn)
- DRY_RUN=0                       (1 = chỉ in log, không ghi DB)

AI (tuỳ chọn):
- GEMINI_API_KEY=...  (nếu muốn fill niche/sentiment vào bảng videos)

"""

from __future__ import annotations

import os
import json
import re
import random
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from supabase import create_client, Client


# =============================
# ✅ Auto-Discover keywords (bạn tự sửa list này)
# =============================
TARGET_KEYWORDS = ["kiếm tiền online", "truyện đêm khuya", "AI tools", "tóm tắt phim", "gameplay"]


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
# API Key pool (fallback)
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
    gemini_api_key: Optional[str] = None

def load_cfg() -> Cfg:
    ykeys = _env_optional("YOUTUBE_API_KEYS")
    if ykeys:
        keys = [k.strip() for k in ykeys.split(",") if k.strip()]
    else:
        k1 = _env_optional("YOUTUBE_API_KEY")
        keys = [k1] if k1 else []

    return Cfg(
        supabase_url=_env_required("SUPABASE_URL"),
        supabase_key=_env_required("SUPABASE_SERVICE_ROLE_KEY"),
        youtube_keys=keys,
        concurrency=_env_int("CONCURRENCY", 10),
        max_videos_per_channel=_env_int("MAX_VIDEOS_PER_CHANNEL", 50),
        max_channels_per_run=_env_int("MAX_CHANNELS_PER_RUN", 0),
        dry_run=(_env_optional("DRY_RUN", "0") == "1"),
        gemini_api_key=_env_optional("GEMINI_API_KEY"),
    )

def supa(cfg: Cfg) -> Client:
    return create_client(cfg.supabase_url, cfg.supabase_key)


# -----------------------------
# YouTube fetch with fallback
# -----------------------------
def _parse_yt_error(text: str) -> Tuple[str, str]:
    """Return (reason, message)."""
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
    - Rotate when: keyInvalid / accessNotConfigured (key lỗi, project chưa bật API).
    - If quotaExceeded -> raise QuotaExceeded.
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
                    raise QuotaExceeded(f"Quota exceeded: reason={reason} msg={msg}")

                if resp.status in (400, 403) and reason in ("keyInvalid", "accessNotConfigured"):
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
    params = {"part": "snippet,statistics,contentDetails", "id": channel_id, "maxResults": 1}
    data = await fetch_json_youtube(session, pool, url, params)
    items = data.get("items") or []
    return items[0] if items else None

async def yt_videos(session: aiohttp.ClientSession, pool: ApiKeyPool, video_ids: List[str]) -> List[Dict[str, Any]]:
    if not video_ids:
        return []
    url = f"{YOUTUBE_API_BASE}/videos"
    items: List[Dict[str, Any]] = []
    for chunk in chunked(video_ids, 50):
        params = {"part": "snippet,statistics,contentDetails", "id": ",".join(chunk), "maxResults": 50}
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

async def yt_search_channels(session: aiohttp.ClientSession, pool: ApiKeyPool, keyword: str) -> List[Dict[str, Any]]:
    """Search API (type=channel)."""
    url = f"{YOUTUBE_API_BASE}/search"
    params = {
        "part": "snippet",
        "q": keyword,
        "type": "channel",
        "maxResults": 50,
        "order": "relevance",
    }
    data = await fetch_json_youtube(session, pool, url, params)
    return data.get("items") or []


# -----------------------------
# Gemini AI (optional)
# -----------------------------
async def analyze_video_with_ai(session: aiohttp.ClientSession, gemini_key: str, title: str, description: str) -> Tuple[str, str]:
    """Return (niche, sentiment). Nếu lỗi -> ("","")"""
    if not gemini_key:
        return "", ""

    prompt = f"""
Bạn là hệ thống phân loại nội dung YouTube.
Trả về JSON DUY NHẤT theo format:
{{"niche":"...","sentiment":"..."}}
Không thêm text ngoài JSON.

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
            if resp.status >= 400:
                return "", ""
            j = await resp.json()
            cand = (j.get("candidates") or [{}])[0]
            parts = (((cand.get("content") or {}).get("parts")) or [])
            text_out = " ".join([safe_str(p.get("text")) for p in parts]).strip()
            if not text_out:
                return "", ""
            m = re.search(r"\{.*\}", text_out, re.S)
            if not m:
                return "", ""
            obj = json.loads(m.group(0))
            niche = safe_str(obj.get("niche")).strip()[:64]
            sentiment = safe_str(obj.get("sentiment")).strip()[:64]
            return niche, sentiment
    except Exception:
        return "", ""


# -----------------------------
# Supabase ops
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
    r = client.table("channels").select("channel_id").limit(100000).execute()
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
        payload = dict(payload)
        payload.setdefault("id", 1)
        client.table("scraper_state").upsert(payload, on_conflict="id").execute()
    except Exception:
        pass


# =============================
# ✅ Auto-Discover (run at START)
# =============================
async def auto_discover_new_channels(
    session: aiohttp.ClientSession,
    pool: ApiKeyPool,
    client: Client,
    *,
    max_insert: int = 50,
) -> int:
    """
    - Random chọn 1–2 keyword trong TARGET_KEYWORDS
    - Search type=channel maxResults=50
    - Lấy channel_id, title, avatar_url
    - Insert (upsert) vào Supabase nếu chưa có
    - Nếu gặp quotaExceeded -> skip discover (không crash)
    """
    if not TARGET_KEYWORDS:
        return 0

    k = 1 if len(TARGET_KEYWORDS) == 1 else random.choice([1, 2])
    keywords = random.sample(TARGET_KEYWORDS, k=k)

    existing = fetch_existing_channel_ids(client)
    new_rows: List[Dict[str, Any]] = []

    print(f"[INFO] auto_discover: keywords={keywords}")

    try:
        for kw in keywords:
            items = await yt_search_channels(session, pool, kw)
            for it in items:
                cid = safe_str(((it.get("id") or {}).get("channelId"))).strip()
                if not cid or cid in existing:
                    continue
                sn = it.get("snippet") or {}
                title = safe_str(sn.get("title")).strip()
                thumbs = sn.get("thumbnails") or {}
                avatar = safe_str((thumbs.get("default") or thumbs.get("high") or {}).get("url")).strip()
                new_rows.append({
                    "channel_id": cid,
                    "title": title,
                    "handle": "",
                    "avatar_url": avatar,
                    "subscribers": 0,  # scraper sẽ update khi quét channel.stats
                })
                existing.add(cid)  # tránh trùng trong cùng run
                if len(new_rows) >= max_insert:
                    break
            if len(new_rows) >= max_insert:
                break

    except QuotaExceeded as qe:
        # Không crash: skip discover
        print(f"[WARN] auto_discover: quotaExceeded -> skip. {qe}")
        return 0

    new_rows = dedupe_rows(new_rows, "channel_id")
    if not new_rows:
        print("[INFO] auto_discover: không tìm thấy kênh mới.")
        return 0

    upsert_channels(client, new_rows, dry_run=False)  # auto discover luôn ghi
    print(f"[INFO] auto_discover: inserted {len(new_rows)} channels.")
    return len(new_rows)


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

    video_ids: List[str] = []
    if uploads_pid:
        video_ids = await yt_playlist_items_video_ids(session, pool, uploads_pid, limit=cfg.max_videos_per_channel)

    v_items = await yt_videos(session, pool, video_ids)

    videos_rows: List[Dict[str, Any]] = []
    snapshots_rows: List[Dict[str, Any]] = []
    now_iso = utc_now().isoformat()
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


async def run_async(cfg: Cfg) -> None:
    pool = ApiKeyPool(cfg.youtube_keys)
    client = supa(cfg)

    timeout = aiohttp.ClientTimeout(total=55)
    sem = asyncio.Semaphore(cfg.concurrency)

    upsert_scraper_state(client, {
        "id": 1,
        "status": "running",
        "message": "Starting…",
        "updated_at": utc_now().isoformat(),
        "run_started_at": utc_now().isoformat(),
        "progress": 0,
        "pct": 0,
    }, dry_run=cfg.dry_run)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # ✅ Auto-discover ngay đầu run
        try:
            added = await auto_discover_new_channels(session, pool, client, max_insert=50)
            if added:
                upsert_scraper_state(client, {
                    "id": 1,
                    "status": "running",
                    "message": f"Auto-discover added {added} channels",
                    "updated_at": utc_now().isoformat(),
                }, dry_run=cfg.dry_run)
        except Exception as e:
            print(f"[WARN] auto_discover failed (ignored): {e}")

        # Load channels AFTER discover
        channel_ids = list_channels_to_scan(client, limit=cfg.max_channels_per_run)
        if not channel_ids:
            print("[INFO] Không có kênh nào trong bảng channels.")
            upsert_scraper_state(client, {
                "id": 1,
                "status": "ok",
                "message": "No channels to scan",
                "updated_at": utc_now().isoformat(),
            }, dry_run=cfg.dry_run)
            return

        all_ch: List[Dict[str, Any]] = []
        all_v: List[Dict[str, Any]] = []
        all_s: List[Dict[str, Any]] = []

        async def worker(ch_id: str):
            async with sem:
                return await scan_one_channel(session, pool, client, ch_id, cfg)

        tasks = [asyncio.create_task(worker(cid)) for cid in channel_ids]
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

        print(f"[INFO] channels={len(all_ch)} videos={len(all_v)} snapshots={len(all_s)}")
        upsert_channels(client, all_ch, dry_run=cfg.dry_run)
        upsert_videos(client, all_v, dry_run=cfg.dry_run)
        insert_snapshots(client, all_s, dry_run=cfg.dry_run)

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
    if not cfg.youtube_keys:
        raise RuntimeError("Thiếu YouTube API key. Set YOUTUBE_API_KEYS hoặc YOUTUBE_API_KEY.")
    try:
        asyncio.run(run_async(cfg))
    except QuotaExceeded as qe:
        print("[FATAL] quotaExceeded -> dừng an toàn.")
        print(f"[FATAL] {qe}")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
