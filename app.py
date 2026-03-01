# app.py
# -*- coding: utf-8 -*-
"""toolwatch (Streamlit Frontend-only) — Supabase

Mục tiêu:
- UI dark + bố cục giống NexLev trong giới hạn Streamlit
- Frontend only: KHÔNG gọi YouTube API
- Supabase via st.secrets
- DB trống vẫn chạy

Ghi chú quan trọng:
- Geo/Age/Gender (Audience) là ƯỚC TÍNH, không phải số thật (vì không dùng YouTube Analytics API OAuth).
- Muốn “gần đúng” hơn => scraper nên chạy thường xuyên và lưu thêm channel-level stats (khuyến nghị).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import altair as alt
import pandas as pd
import streamlit as st
from supabase import create_client, Client

APP_TITLE = "toolwatch • NexLev-style (Supabase)"
YOUTUBE_WATCH = "https://www.youtube.com/watch?v="
YOUTUBE_THUMB = "https://i.ytimg.com/vi/{vid}/hqdefault.jpg"

SCAN_INTERVAL_SECONDS = 3600  # 1 giờ


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def fmt_num(n: int) -> str:
    n = int(n or 0)
    if abs(n) >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if abs(n) >= 1_000:
        return f"{n/1_000:.2f}K"
    return str(n)


def parse_dt_any(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def time_ago_vi(iso: str) -> str:
    dt = parse_dt_any(iso)
    if not dt:
        return ""
    sec = int((utc_now() - dt).total_seconds())
    if sec < 60:
        return "vừa xong"
    if sec < 3600:
        return f"{sec//60} phút trước"
    if sec < 86400:
        return f"{sec//3600} giờ trước"
    days = sec // 86400
    if days < 30:
        return f"{days} ngày trước"
    months = days // 30
    if months < 12:
        return f"{months} tháng trước"
    years = months // 12
    return f"{years} năm trước"


def ensure_df_columns(df: pd.DataFrame, defaults: Dict[str, Any]) -> pd.DataFrame:
    for c, d in defaults.items():
        if c not in df.columns:
            df[c] = d
    return df


def coerce_int(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df


def extract_channel_input(s: str) -> Dict[str, Optional[str]]:
    s = (s or "").strip()
    if not s:
        return {"channel_id": None, "handle": None}

    if re.fullmatch(r"UC[a-zA-Z0-9_-]{20,}", s):
        return {"channel_id": s, "handle": None}

    m = re.search(r"/channel/(UC[a-zA-Z0-9_-]{20,})", s)
    if m:
        return {"channel_id": m.group(1), "handle": None}

    if s.startswith("@") and len(s) >= 2:
        return {"channel_id": None, "handle": s}

    m2 = re.search(r"youtube\.com/@([A-Za-z0-9._-]{2,})", s)
    if m2:
        return {"channel_id": None, "handle": "@" + m2.group(1)}

    return {"channel_id": None, "handle": None}


def detect_vietnamese(text: str) -> bool:
    return bool(
        re.search(
            r"[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]",
            (text or "").lower(),
        )
    )


def guess_lang_from_titles(titles: List[str]) -> str:
    if not titles:
        return "unknown"
    vi = 0
    en = 0
    for t in titles[:200]:
        t = (t or "").strip()
        if not t:
            continue
        if detect_vietnamese(t):
            vi += 1
        if re.fullmatch(r"[\x00-\x7F]+", t):
            en += 1
    if vi >= max(3, en):
        return "vi"
    if en >= max(3, vi):
        return "en"
    return "mixed"


def is_shorts_guess(title: str, tags_json: str) -> bool:
    t = (title or "").lower()
    if "#shorts" in t or "shorts" in t:
        return True
    tj = (tags_json or "").lower()
    return '"shorts"' in tj or "shorts" in tj


def clamp(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def auto_rpm_estimate(videos_df: pd.DataFrame) -> Dict[str, float]:
    """Ước tính RPM dựa trên language + engagement (heuristic, conservative)."""
    if videos_df.empty:
        return {"rpm_long": 1.5, "rpm_shorts": 0.2}

    v = videos_df.copy()
    v = ensure_df_columns(v, {"view_count": 0, "like_count": 0, "comment_count": 0, "title": ""})
    v = coerce_int(v, ["view_count", "like_count", "comment_count"])

    titles = [safe_str(x) for x in v["title"].tolist()[:200]]
    lang = guess_lang_from_titles(titles)

    views_sum = int(v["view_count"].sum())
    likes_sum = int(v["like_count"].sum())
    comm_sum = int(v["comment_count"].sum())
    eng = (likes_sum + comm_sum) / max(1, views_sum)  # ratio

    base_long = 3.2 if lang == "en" else (1.6 if lang == "vi" else 2.2)
    base_long += clamp((eng - 0.01) * 60, 0.0, 1.6)

    base_shorts = (0.12 if lang == "vi" else (0.30 if lang == "en" else 0.22))
    base_shorts += clamp((eng - 0.008) * 45, 0.0, 0.45)

    return {"rpm_long": clamp(base_long, 0.3, 10.0), "rpm_shorts": clamp(base_shorts, 0.03, 1.5)}


def audience_estimator(lang: str, niche: str = "", market_hint: str = "Auto") -> Dict[str, Any]:
    """Ước tính audience (NOT real)."""
    niche = (niche or "").lower()

    if market_hint == "VN":
        geos = [("Vietnam", 70), ("United States", 10), ("Japan", 6), ("Korea", 5), ("Australia", 3), ("Canada", 2)]
    elif market_hint == "US/EN":
        geos = [("United States", 50), ("India", 16), ("United Kingdom", 10), ("Canada", 7), ("Australia", 7), ("Germany", 4)]
    elif market_hint == "Global":
        geos = [("United States", 30), ("India", 18), ("Vietnam", 10), ("United Kingdom", 8), ("Canada", 6), ("Australia", 5)]
    else:
        if lang == "vi":
            geos = [("Vietnam", 58), ("United States", 12), ("Japan", 7), ("Korea", 6), ("Australia", 4), ("Canada", 3)]
        elif lang == "en":
            geos = [("United States", 42), ("India", 18), ("United Kingdom", 10), ("Canada", 7), ("Australia", 6), ("Germany", 4)]
        else:
            geos = [("United States", 28), ("India", 16), ("Vietnam", 10), ("United Kingdom", 8), ("Canada", 6), ("Australia", 5)]

    male = 65
    female = 35
    if any(k in niche for k in ["beauty", "makeup", "skincare", "fashion", "lifestyle"]):
        male, female = 35, 65
    elif any(k in niche for k in ["game", "gaming", "esport", "tech", "ai", "crypto"]):
        male, female = 75, 25
    elif any(k in niche for k in ["kids", "children", "nursery"]):
        male, female = 45, 55

    ages = [("13-17", 8), ("18-24", 24), ("25-34", 34), ("35-44", 20), ("45-54", 9), ("55-64", 4), ("65+", 1)]
    if any(k in niche for k in ["finance", "crypto", "business"]):
        ages = [("13-17", 3), ("18-24", 18), ("25-34", 38), ("35-44", 26), ("45-54", 10), ("55-64", 4), ("65+", 1)]
    elif any(k in niche for k in ["kids", "children"]):
        ages = [("13-17", 15), ("18-24", 35), ("25-34", 30), ("35-44", 12), ("45-54", 5), ("55-64", 2), ("65+", 1)]

    return {"geos": geos, "gender": {"Male": male, "Female": female, "User-specified": 0}, "ages": ages}


@st.cache_resource
def supa() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)


@st.cache_data(ttl=120, show_spinner=False)
def fetch_channels() -> pd.DataFrame:
    client = supa()
    res = client.table("channels").select("id,channel_id,title,handle,avatar_url,subscribers,created_at").order("subscribers", desc=True).execute()
    df = pd.DataFrame(res.data or [])
    if df.empty:
        df = pd.DataFrame(columns=["id", "channel_id", "title", "handle", "avatar_url", "subscribers", "created_at"])
    df = ensure_df_columns(df, {"id": None, "channel_id": "", "title": "", "handle": "", "avatar_url": "", "subscribers": 0, "created_at": ""})
    df = coerce_int(df, ["subscribers"])
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_videos(limit: int = 400) -> pd.DataFrame:
    client = supa()
    res = None
    for col in ("published_at", "video_id", None):
        try:
            q = client.table("videos").select("*").limit(int(limit))
            if col:
                q = q.order(col, desc=True)
            res = q.execute()
            break
        except Exception:
            res = None
    df = pd.DataFrame((res.data if res else []) or [])
    if df.empty:
        df = pd.DataFrame(columns=["video_id", "channel_id", "published_at", "title", "description", "tags_json", "niche", "sentiment"])
    df = ensure_df_columns(df, {"video_id": "", "channel_id": "", "published_at": "", "title": "", "description": "", "tags_json": "", "niche": "", "sentiment": ""})
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_latest_video_snapshots(video_ids: List[str]) -> pd.DataFrame:
    if not video_ids:
        return pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])

    client = supa()
    rows: List[Dict[str, Any]] = []
    CHUNK = 150

    for i in range(0, len(video_ids), CHUNK):
        chunk = video_ids[i : i + CHUNK]
        try:
            r = (
                client.table("snapshots")
                .select("video_id,captured_at,view_count,like_count,comment_count")
                .in_("video_id", chunk)
                .order("captured_at", desc=True)
                .limit(max(800, len(chunk) * 5))
                .execute()
            )
            rows.extend(r.data or [])
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])

    df = ensure_df_columns(df, {"video_id": "", "captured_at": "", "view_count": 0, "like_count": 0, "comment_count": 0})
    df = coerce_int(df, ["view_count", "like_count", "comment_count"])
    df["video_id"] = df["video_id"].astype(str)

    latest: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        vid = str(r["video_id"] or "")
        if not vid or vid in latest:
            continue
        latest[vid] = {
            "video_id": vid,
            "captured_at": safe_str(r.get("captured_at")),
            "view_count": int(r["view_count"]),
            "like_count": int(r["like_count"]),
            "comment_count": int(r["comment_count"]),
        }
    out = pd.DataFrame(list(latest.values()))
    if out.empty:
        out = pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])
    return out


@st.cache_data(ttl=180, show_spinner=False)
def fetch_scraper_state() -> Dict[str, Any]:
    client = supa()
    try:
        r = client.table("scraper_state").select("*").order("updated_at", desc=True).limit(1).execute()
        row = (r.data or [None])[0] or {}
        return dict(row)
    except Exception:
        return {}


@st.cache_data(ttl=120, show_spinner=False)
def fetch_snapshots_since(video_ids: List[str], since_iso: str, hard_limit: int = 200000) -> pd.DataFrame:
    if not video_ids:
        return pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])

    client = supa()
    rows: List[Dict[str, Any]] = []
    CHUNK = 120

    for i in range(0, len(video_ids), CHUNK):
        chunk = video_ids[i : i + CHUNK]
        try:
            r = (
                client.table("snapshots")
                .select("video_id,captured_at,view_count,like_count,comment_count")
                .in_("video_id", chunk)
                .gte("captured_at", since_iso)
                .order("captured_at", desc=False)
                .limit(max(5000, len(chunk) * 120))
                .execute()
            )
            rows.extend(r.data or [])
        except Exception:
            continue
        if len(rows) >= hard_limit:
            break

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])
    df = ensure_df_columns(df, {"video_id": "", "captured_at": "", "view_count": 0, "like_count": 0, "comment_count": 0})
    df = coerce_int(df, ["view_count", "like_count", "comment_count"])
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_latest_scan_time() -> Optional[str]:
    client = supa()
    try:
        r = client.table("snapshots").select("captured_at").order("captured_at", desc=True).limit(1).execute()
        row = (r.data or [None])[0]
        if row and row.get("captured_at"):
            return str(row["captured_at"])
    except Exception:
        pass
    return None


def add_channel_row(user_input: str) -> Tuple[bool, str]:
    info = extract_channel_input(user_input)
    cid = info["channel_id"]
    handle = info["handle"]
    if not cid and not handle:
        return False, "Nhập UC... hoặc URL /channel/UC... hoặc @handle."

    payload: Dict[str, Any] = {}
    if cid:
        payload["channel_id"] = cid
    if handle:
        payload["handle"] = handle

    client = supa()
    try:
        if cid:
            client.table("channels").upsert(payload, on_conflict="channel_id").execute()
        else:
            client.table("channels").insert(payload).execute()

        clear_caches()
        return True, "✅ Đã thêm kênh. Robot sẽ tự quét trong lần chạy tiếp theo."
    except Exception as e:
        return False, f"❌ Thêm kênh thất bại: {e}"


def delete_channel_by_row(row: Dict[str, Any], delete_children: bool = True) -> Tuple[bool, str]:
    client = supa()
    row_id = row.get("id")
    channel_id = safe_str(row.get("channel_id")).strip()
    try:
        if delete_children and channel_id:
            vids = []
            try:
                vres = client.table("videos").select("video_id").eq("channel_id", channel_id).limit(5000).execute()
                vids = [str(r["video_id"]) for r in (vres.data or []) if r.get("video_id")]
            except Exception:
                vids = []
            if vids:
                client.table("snapshots").delete().in_("video_id", vids).execute()
            client.table("videos").delete().eq("channel_id", channel_id).execute()

        if row_id is not None:
            client.table("channels").delete().eq("id", int(row_id)).execute()
        else:
            if channel_id:
                client.table("channels").delete().eq("channel_id", channel_id).execute()

        clear_caches()
        return True, "✅ Đã xoá kênh."
    except Exception as e:
        return False, f"❌ Xoá kênh thất bại: {e}"


def clear_caches():
    fetch_channels.clear()
    fetch_videos.clear()
    fetch_latest_video_snapshots.clear()
    fetch_scraper_state.clear()
    fetch_snapshots_since.clear()
    fetch_latest_scan_time.clear()


def inject_css():
    st.markdown(
        """
<style>
  header[data-testid="stHeader"]{ background: transparent !important; }
  div[data-testid="stToolbar"]{ display:none !important; }
  #MainMenu{ visibility:hidden; }
  footer{ visibility:hidden; }

  section[data-testid="stSidebar"]{
    transform:none !important;
    margin-left:0 !important;
    visibility:visible !important;
    opacity:1 !important;
    display:block !important;
    width: 300px !important;
    min-width: 300px !important;
  }
  section[data-testid="stSidebar"][aria-expanded="false"]{ transform:none !important; }
  div[data-testid="collapsedControl"]{ display:none !important; }
  [data-testid="stSidebarCollapseButton"]{ display:none !important; }
  button[title="Collapse sidebar"], button[aria-label="Collapse sidebar"], button[aria-label="Close sidebar"]{ display:none !important; }
  section[data-testid="stSidebar"] button[kind="headerNoPadding"]{ display:none !important; }

  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800;900&display=swap');
  html, body, [class*="css"]{ font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important; }

  .stApp{
    background: radial-gradient(1200px 500px at 30% 10%, rgba(255,255,255,0.06) 0%, rgba(0,0,0,0.0) 55%),
                linear-gradient(180deg, #0b0b0b 0%, #090909 100%);
    color: rgba(230,232,238,0.92);
  }
  .block-container{ max-width: 1500px; padding-top: 0.6rem; padding-bottom: 2rem; }

  .tw-top{
    display:flex; align-items:center; justify-content:space-between;
    padding: 10px 12px; border-radius: 16px;
    border: 1px solid rgba(255,255,255,0.10);
    background: rgba(255,255,255,0.02);
    margin-bottom: 10px;
  }
  .tw-brand{ font-weight: 900; font-size: 18px; }
  .tw-pill{
    padding: 6px 10px; border-radius: 999px;
    border: 1px solid rgba(255,255,255,0.12);
    background: rgba(255,255,255,0.03);
    font-weight: 800; font-size: 12px;
  }

  .nx-mini-wrap{ display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  .nx-mini{
    border-radius: 12px;
    border: 1px solid rgba(255,255,255,0.10);
    background: rgba(255,255,255,0.02);
    padding: 12px;
    min-height: 76px;
  }
  .nx-mini .k{ color: rgba(230,232,238,0.70); font-weight: 800; font-size: 12px; display:flex; gap:8px; align-items:center; }
  .nx-mini .v{ margin-top: 6px; font-weight: 900; font-size: 18px; }
  .nx-mini .s{ margin-top: 2px; color: rgba(230,232,238,0.55); font-size: 12px; }

  .nx-title{ font-weight: 900; font-size: 14px; display:flex; align-items:center; gap:8px; }
  .nx-big{ font-weight: 900; font-size: 40px; letter-spacing: -0.02em; margin-top: 2px; }
  .nx-sub{ color: rgba(230,232,238,0.65); font-size: 12px; margin-top: 4px; }

  .tw-card{ border-radius: 16px; border: 1px solid rgba(255,255,255,0.10); background: rgba(255,255,255,0.02); overflow:hidden; }
  .tw-thumb{ position:relative; width:100%; aspect-ratio:16/9; background: rgba(255,255,255,0.05); }
  .tw-thumb img{ width:100%; height:100%; object-fit:cover; object-position:center; display:block; }
  .tw-badge{ position:absolute; left:10px; top:10px; padding: 3px 8px; border-radius: 999px; font-weight: 900; font-size: 12px; border: 1px solid rgba(34,197,94,0.45); background: rgba(34,197,94,0.12); color: #86efac; backdrop-filter: blur(8px); }
  .tw-meta{ padding: 10px 12px 12px 12px; }
  .tw-title{ font-weight: 900; font-size: 14px; line-height: 1.25; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; min-height: 36px; }
  .tw-sub{ margin-top: 6px; color: rgba(230,232,238,0.65); font-size: 12px; white-space: nowrap; overflow:hidden; text-overflow:ellipsis; }
  .tw-badges{ margin-top: 8px; display:flex; gap:6px; flex-wrap:wrap; }
  .tw-chip{ padding: 3px 8px; border-radius: 999px; border: 1px solid rgba(255,255,255,0.10); background: rgba(255,255,255,0.03); font-size: 12px; font-weight: 800; color: rgba(230,232,238,0.82); }
  .tw-chip b{ color: rgba(230,232,238,0.95); }
  a.tw-open{ text-decoration:none; color: inherit; display:block; }
</style>
        """,
        unsafe_allow_html=True,
    )


def card_header(title: str, icon: str, reveal_key: str, default_reveal: bool = True) -> bool:
    if reveal_key not in st.session_state:
        st.session_state[reveal_key] = default_reveal

    left, right = st.columns([0.75, 0.25])
    with left:
        st.markdown(f"<div class='nx-title'>{icon} {title}</div>", unsafe_allow_html=True)
    with right:
        if st.button("✨ Tiết lộ", key=f"{reveal_key}_btn", use_container_width=True):
            st.session_state[reveal_key] = not bool(st.session_state[reveal_key])
    return bool(st.session_state[reveal_key])


def metric_card(title: str, icon: str, value: str, sub: str, reveal_key: str):
    with st.container(border=True):
        reveal = card_header(title, icon, reveal_key, default_reveal=True)
        if reveal:
            st.markdown(f"<div class='nx-big'>{value}</div><div class='nx-sub'>{sub}</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div class='nx-big'>•••</div><div class='nx-sub'>Bấm Tiết lộ</div>", unsafe_allow_html=True)


def mini_cards_block(items: List[Tuple[str, str, str, str]]):
    lines = ['<div class="nx-mini-wrap">']
    for icon, key, val, sub in items:
        lines.append('<div class="nx-mini">')
        lines.append(f'<div class="k">{icon} {key}</div>')
        lines.append(f'<div class="v">{val}</div>')
        lines.append(f'<div class="s">{sub}</div>')
        lines.append('</div>')
    lines.append('</div>')
    st.markdown("\n".join(lines), unsafe_allow_html=True)


def compute_window_deltas(videos_df: pd.DataFrame, channel_id: str, window_days: int, max_videos: int = 160) -> Dict[str, Any]:
    ch = videos_df[videos_df["channel_id"].astype(str) == str(channel_id)].copy()
    if ch.empty:
        return {"views": 0, "likes": 0, "comments": 0, "shorts_views": 0, "long_views": 0, "lang": "unknown", "niche": ""}

    ch["_dt"] = pd.to_datetime(ch["published_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
    ch = ch.sort_values("_dt", ascending=False).drop(columns=["_dt"], errors="ignore").head(int(max_videos))

    titles = [safe_str(x) for x in ch["title"].tolist()[:200]]
    lang = guess_lang_from_titles(titles)

    niche = ""
    if "niche" in ch.columns:
        vc = ch["niche"].astype(str).replace("None", "").replace("nan", "").value_counts()
        if len(vc) > 0:
            niche = vc.index[0]

    since = utc_now() - timedelta(days=int(window_days))
    vid_ids = [str(x) for x in ch["video_id"].astype(str).tolist() if str(x).strip()]
    snap = fetch_snapshots_since(vid_ids, since.isoformat(), hard_limit=220000)

    if snap.empty:
        ch = ensure_df_columns(ch, {"view_count": 0, "like_count": 0, "comment_count": 0, "tags_json": ""})
        ch = coerce_int(ch, ["view_count", "like_count", "comment_count"])
        views = int(ch["view_count"].sum())
        likes = int(ch["like_count"].sum())
        comments = int(ch["comment_count"].sum())
        return {"views": views, "likes": likes, "comments": comments, "shorts_views": 0, "long_views": views, "lang": lang, "niche": niche, "video_ids_used": len(vid_ids)}

    snap["captured_at_dt"] = pd.to_datetime(snap["captured_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
    snap = snap.dropna(subset=["captured_at_dt"]).sort_values(["video_id", "captured_at_dt"])
    first = snap.groupby("video_id").first(numeric_only=False)
    last = snap.groupby("video_id").last(numeric_only=False)

    views_delta = (last["view_count"] - first["view_count"]).clip(lower=0)
    likes_delta = (last["like_count"] - first["like_count"]).clip(lower=0)
    comments_delta = (last["comment_count"] - first["comment_count"]).clip(lower=0)

    delta = pd.DataFrame({"video_id": views_delta.index, "views": views_delta.values, "likes": likes_delta.values, "comments": comments_delta.values})
    delta["video_id"] = delta["video_id"].astype(str)

    ch = ensure_df_columns(ch, {"tags_json": "", "title": ""})
    ch["video_id"] = ch["video_id"].astype(str)
    merged = ch[["video_id", "title", "tags_json"]].merge(delta, on="video_id", how="left")
    merged = ensure_df_columns(merged, {"views": 0, "likes": 0, "comments": 0})
    merged = coerce_int(merged, ["views", "likes", "comments"])
    merged["is_shorts"] = merged.apply(lambda r: is_shorts_guess(str(r["title"]), str(r["tags_json"])), axis=1)

    shorts_views = int(merged.loc[merged["is_shorts"], "views"].sum())
    long_views = int(merged.loc[~merged["is_shorts"], "views"].sum())

    return {
        "views": int(merged["views"].sum()),
        "likes": int(merged["likes"].sum()),
        "comments": int(merged["comments"].sum()),
        "shorts_views": shorts_views,
        "long_views": long_views,
        "lang": lang,
        "niche": niche,
        "video_ids_used": len(vid_ids),
    }


def build_views_timeseries(videos_df: pd.DataFrame, channel_id: str, window: str, max_videos: int = 80) -> pd.DataFrame:
    if window == "7D":
        since = utc_now() - timedelta(days=7)
    elif window == "14D":
        since = utc_now() - timedelta(days=14)
    elif window == "30D":
        since = utc_now() - timedelta(days=30)
    elif window == "3M":
        since = utc_now() - timedelta(days=90)
    else:
        since = utc_now() - timedelta(days=365)

    ch = videos_df[videos_df["channel_id"].astype(str) == str(channel_id)].copy()
    if ch.empty:
        return pd.DataFrame(columns=["date", "views"])

    ch["_dt"] = pd.to_datetime(ch["published_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
    ch = ch.sort_values("_dt", ascending=False).drop(columns=["_dt"], errors="ignore").head(int(max_videos))

    vid_ids = [str(x) for x in ch["video_id"].astype(str).tolist() if str(x).strip()]
    snap = fetch_snapshots_since(vid_ids, since.isoformat(), hard_limit=220000)
    if snap.empty:
        return pd.DataFrame(columns=["date", "views"])

    snap["captured_at_dt"] = pd.to_datetime(snap["captured_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
    snap = snap.dropna(subset=["captured_at_dt"])
    snap["date"] = snap["captured_at_dt"].dt.floor("D")

    last_per_day = snap.sort_values("captured_at_dt").groupby(["video_id", "date"]).last(numeric_only=False).reset_index()
    daily = last_per_day.groupby("date")["view_count"].sum().reset_index().sort_values("date")

    daily["views"] = daily["view_count"].diff().fillna(0).clip(lower=0).astype(int)

    if window == "1Y":
        daily["month"] = daily["date"].dt.to_period("M").dt.to_timestamp()
        m = daily.groupby("month")["views"].sum().reset_index().rename(columns={"month": "date"})
        return m[["date", "views"]]
    return daily[["date", "views"]]


def render_channel_insights(channel_row: Dict[str, Any], videos_df: pd.DataFrame, rpm_long: float, rpm_shorts: float, market_hint: str):
    cid = safe_str(channel_row.get("channel_id")).strip()
    ch_name = safe_str(channel_row.get("title")).strip() or safe_str(channel_row.get("handle")).strip() or cid
    subs = int(channel_row.get("subscribers") or 0)

    left, right = st.columns([0.72, 0.28])
    with left:
        st.subheader(f"📊 Phân tích kênh — {ch_name}")
        st.caption("Views/Like/Comment lấy từ DB (snapshots). Audience là ước tính.")
    with right:
        window_sel = st.radio("Khoảng", ["7D", "14D", "30D", "3M", "1Y"], horizontal=True, index=2, key="ins_window")

    delta30 = compute_window_deltas(videos_df, cid, window_days=30, max_videos=180)
    views30 = int(delta30["views"])
    shorts30 = int(delta30["shorts_views"])
    long30 = int(delta30["long_views"])
    rev30 = (long30 / 1000.0) * float(rpm_long) + (shorts30 / 1000.0) * float(rpm_shorts)
    rpm_mix = (rpm_long + rpm_shorts) / 2.0
    rpm_level = "Low" if rpm_mix < 2 else ("Medium" if rpm_mix < 4 else "High")

    c1, c2, c3 = st.columns([1, 1, 1], gap="large")
    with c1:
        metric_card("Channel Revenue", "💵", f"${rev30:,.0f}", "30 ngày (ước tính)", "rev")
    with c2:
        metric_card("Views", "👁️", f"{views30:,.0f}", "30 ngày", "views")
    with c3:
        metric_card("RPM", "💲", f"${rpm_mix:.2f}", rpm_level, "rpm")

    l2, r2 = st.columns([1.65, 1.0], gap="large")
    with l2:
        with st.container(border=True):
            _ = card_header("Videos vs Shorts Views", "🎬", "vs", default_reveal=True)
            total = max(1, views30)
            st.progress(min(1.0, long30 / total), text="Video Views")
            st.write(f"• Video: **{fmt_num(long30)}** ({(long30/total)*100:.1f}%)")
            st.write(f"• Shorts: **{fmt_num(shorts30)}** ({(shorts30/total)*100:.1f}%)")
            st.caption(f"Dựa trên ~{delta30.get('video_ids_used', 0)} video gần nhất trong DB.")

    with r2:
        est = audience_estimator(delta30.get("lang", "unknown"), delta30.get("niche", ""), market_hint=market_hint)
        with st.container(border=True):
            _ = card_header("Top Geographies", "🌍", "geo", default_reveal=True)
            st.caption("Ước tính theo language/ngách (không phải YouTube Analytics).")
            for name, pct in est["geos"]:
                a, b = st.columns([0.75, 0.25])
                with a:
                    st.write(name)
                    st.progress(float(pct) / 100.0)
                with b:
                    st.write(f"{float(pct):.1f}%")

    l3, r3 = st.columns([1.65, 1.0], gap="large")
    with l3:
        with st.container(border=True):
            _ = card_header("Views & Subscribers Graph", "📈", "graph", default_reveal=True)
            metric_mode = st.radio("Chỉ số", ["Views", "Subscribers"], horizontal=True, index=0, key="graph_metric")
            window_for_graph = st.radio("Range", ["7D", "14D", "30D", "3M", "1Y"], horizontal=True, index=["7D","14D","30D","3M","1Y"].index(window_sel), key="graph_range")

            if metric_mode == "Subscribers":
                df = build_views_timeseries(videos_df, cid, window_for_graph, max_videos=60)
                if df.empty:
                    st.info("Chưa đủ dữ liệu snapshots để vẽ.")
                else:
                    df["value"] = subs
                    chart = alt.Chart(df).mark_line().encode(
                        x=alt.X("date:T", title=None),
                        y=alt.Y("value:Q", title=None),
                        tooltip=[alt.Tooltip("date:T", title="Date"), alt.Tooltip("value:Q", title="Subscribers")],
                    ).properties(height=260)
                    st.altair_chart(chart, use_container_width=True)
                    st.caption("Subscribers history chưa có → đường phẳng (khuyến nghị lưu subscriber snapshot).")
            else:
                df = build_views_timeseries(videos_df, cid, window_for_graph, max_videos=80)
                if df.empty:
                    st.info("Chưa đủ dữ liệu snapshots để vẽ.")
                else:
                    chart = alt.Chart(df).mark_area(line={"color": "#34d399", "strokeWidth": 2}, opacity=0.45).encode(
                        x=alt.X("date:T", title=None),
                        y=alt.Y("views:Q", title=None),
                        tooltip=[alt.Tooltip("date:T", title="Date"), alt.Tooltip("views:Q", title="Views")],
                    ).properties(height=260)
                    st.altair_chart(chart, use_container_width=True)
                    st.caption("Views = tăng trưởng theo ngày dựa trên snapshots (ước tính).")

            st.divider()

            ch = videos_df[videos_df["channel_id"].astype(str) == str(cid)].copy()
            ch = ensure_df_columns(ch, {"view_count": 0, "title": "", "published_at": "", "tags_json": ""})
            ch = coerce_int(ch, ["view_count"])
            total_views_db = int(ch["view_count"].sum())
            avg_views = int(ch["view_count"].mean()) if len(ch) else 0

            last_upload_iso = ""
            earliest_iso = ""
            if len(ch):
                dd = pd.to_datetime(ch["published_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
                if dd.notna().any():
                    last_upload_iso = dd.max().isoformat()
                    earliest_iso = dd.min().isoformat()

            last_upload = time_ago_vi(last_upload_iso) if last_upload_iso else "Không áp dụng"
            days_since = "—"
            if earliest_iso:
                dt0 = parse_dt_any(earliest_iso)
                if dt0:
                    days_since = str(int((utc_now() - dt0).total_seconds() / 86400))

            avg_monthly = "—"
            if len(ch):
                dd = pd.to_datetime(ch["published_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
                dd = dd.dropna()
                last90 = dd[dd >= (utc_now() - timedelta(days=90))]
                avg_monthly = f"{(len(last90)/3.0):.2f}" if len(last90) else "0.00"

            shorts_cnt = 0
            if len(ch):
                shorts_cnt = int(ch.apply(lambda r: is_shorts_guess(str(r["title"]), str(r["tags_json"])), axis=1).sum())
            has_shorts = "Đúng" if shorts_cnt > 0 else "Không"
            title_len = int(ch["title"].astype(str).str.len().mean()) if len(ch) else 0

            items = [
                ("👁️", "Tổng lượt xem", f"{total_views_db:,}", "Trong DB (video đã lưu)"),
                ("🗓️", "Lần tải lên cuối", f"{last_upload}", "Theo published_at"),
                ("⏱️", "Số ngày kể từ video đầu", f"{days_since}", "Trong DB"),
                ("📈", "Views TB / video", f"{avg_views:,}", "Trong DB"),
                ("📤", "Uploads TB/tháng", f"{avg_monthly}", "Ước tính (90 ngày)"),
                ("🔤", "Độ dài tiêu đề TB", f"{title_len} ký tự", "Trong DB"),
                ("🎞️", "Có shorts", f"{has_shorts}", f"{shorts_cnt} video shorts"),
                ("🏷️", "Ngách (AI)", safe_str(delta30.get("niche", "")) or "—", "Theo videos.niche"),
            ]
            mini_cards_block(items)

    with r3:
        est = audience_estimator(delta30.get("lang", "unknown"), delta30.get("niche", ""), market_hint=market_hint)
        with st.container(border=True):
            _ = card_header("Age and Gender", "👥", "demo", default_reveal=True)
            g = est["gender"]
            for k in ["Male", "Female", "User-specified"]:
                pct = float(g.get(k, 0))
                a, b = st.columns([0.75, 0.25])
                with a:
                    st.write(k)
                    st.progress(pct / 100.0)
                with b:
                    st.write(f"{pct:.1f}%")
            st.divider()
            for name, pct in est["ages"]:
                a, b = st.columns([0.75, 0.25])
                with a:
                    st.write(name)
                    st.progress(float(pct) / 100.0)
                with b:
                    st.write(f"{float(pct):.1f}%")
            st.caption("⚠️ Ước tính. Muốn số thật => YouTube Analytics API (OAuth).")


def render_video_cards(videos: pd.DataFrame, channels: pd.DataFrame, rpm_long: float, rpm_shorts: float, viral_rel_threshold: float, columns: int = 4):
    if videos.empty:
        st.info("Chưa có video trong bảng videos. Chờ robot scraper chạy.")
        return

    ch_map: Dict[str, Dict[str, Any]] = {}
    if not channels.empty:
        for _, rr in channels.iterrows():
            cid = safe_str(rr["channel_id"]).strip()
            name = safe_str(rr["title"]).strip() or safe_str(rr["handle"]).strip() or cid or "(unknown)"
            ch_map[cid] = {"name": name, "subs": int(rr["subscribers"])}

    cols = st.columns(int(columns), gap="large")

    for idx, (_, r) in enumerate(videos.iterrows()):
        col = cols[idx % int(columns)]
        with col:
            vid = safe_str(r["video_id"]).strip()
            cid = safe_str(r["channel_id"]).strip()
            title = safe_str(r["title"])
            published_at = safe_str(r["published_at"])
            ago = time_ago_vi(published_at)

            url = YOUTUBE_WATCH + vid if vid else "#"
            thumb = YOUTUBE_THUMB.format(vid=vid) if vid else ""

            views = int(r.get("view_count", 0) or 0)
            likes = int(r.get("like_count", 0) or 0)
            comments = int(r.get("comment_count", 0) or 0)

            ch_name = ch_map.get(cid, {}).get("name", cid or "(unknown)")
            subs = int(ch_map.get(cid, {}).get("subs", 0))
            rel = (views / max(1, subs)) if subs > 0 else 0.0
            viral = rel >= float(viral_rel_threshold)

            tags_json = safe_str(r.get("tags_json", ""))
            rpm = rpm_shorts if is_shorts_guess(title, tags_json) else rpm_long
            rev = (views / 1000.0) * float(rpm)

            dt = parse_dt_any(published_at) or utc_now()
            hours = max(1.0, (utc_now() - dt).total_seconds() / 3600.0)
            vph = views / hours
            eng = (likes + comments) / max(1, views) * 100.0

            badge = "<div class='tw-badge'>✅🔥 VIRAL</div>" if viral else ""

            lines = [
                f'<a class="tw-open" href="{url}" target="_blank" rel="noopener">',
                '<div class="tw-card">',
                '<div class="tw-thumb">',
                f'<img src="{thumb}" />',
                f"{badge}",
                '</div>',
                '<div class="tw-meta">',
                f'<div class="tw-title">{title}</div>',
                f'<div class="tw-sub">{ch_name} • {fmt_num(views)} lượt xem • {ago}</div>',
                '<div class="tw-badges">',
                f'<span class="tw-chip">👁️ <b>{fmt_num(views)}</b></span>',
                f'<span class="tw-chip">👍 <b>{fmt_num(likes)}</b></span>',
                f'<span class="tw-chip">💬 <b>{fmt_num(comments)}</b></span>',
                f'<span class="tw-chip">⚡ <b>{fmt_num(int(vph))}</b>/giờ</span>',
                f'<span class="tw-chip">💡 <b>{eng:.1f}%</b></span>',
                f'<span class="tw-chip">💵 <b>≈${rev:,.2f}</b></span>',
                '</div>',
                '</div>',
                '</div>',
                '</a>',
            ]
            st.markdown("\n".join(lines), unsafe_allow_html=True)


def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide", initial_sidebar_state="expanded")
    inject_css()

    st.markdown(
        """
<div class="tw-top">
  <div class="tw-brand">toolwatch • NexLev-style (Supabase)</div>
  <div style="display:flex; gap:10px;">
    <div class="tw-pill">Frontend only</div>
    <div class="tw-pill">No YouTube API</div>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )

    st.sidebar.header("⚙️ Điều khiển")
    if st.sidebar.button("🔄 Refresh dữ liệu", use_container_width=True):
        clear_caches()
        st.rerun()

    st.sidebar.subheader("🛰️ Trạng thái robot")
    last_scan = fetch_latest_scan_time()
    state = fetch_scraper_state()

    if last_scan:
        st.sidebar.caption(f"Lần cập nhật snapshots gần nhất: **{time_ago_vi(last_scan)}**")
        dt = parse_dt_any(last_scan)
        if dt:
            elapsed = max(0.0, (utc_now() - dt).total_seconds())
            pct = min(1.0, elapsed / float(SCAN_INTERVAL_SECONDS))
            st.sidebar.progress(pct, text=f"Chu kỳ 1 giờ: {int(pct*100)}%")
            if elapsed > 5400:
                st.sidebar.warning("⚠️ Dữ liệu lâu chưa cập nhật. Kiểm tra GitHub Actions.")
    else:
        st.sidebar.caption("Chưa thấy dữ liệu scan (snapshots rỗng).")

    if state:
        st.sidebar.caption("scraper_state (best effort):")
        for k in ["status", "message", "updated_at", "last_run_at", "run_started_at", "progress", "pct"]:
            if k in state and state.get(k) not in (None, ""):
                st.sidebar.write(f"- **{k}**: {state.get(k)}")

    st.sidebar.divider()
    st.sidebar.subheader("➕ Thêm kênh")
    ch_in = st.sidebar.text_input("UC... hoặc @handle", placeholder="UCxxxx… hoặc @MrBeast", key="add_channel_input")
    if st.sidebar.button("Thêm kênh", use_container_width=True):
        ok, msg = add_channel_row(ch_in)
        (st.sidebar.success if ok else st.sidebar.error)(msg)

    st.sidebar.divider()
    st.sidebar.subheader("🗑️ Xoá kênh")
    ch_df = fetch_channels()
    sel_row: Optional[Dict[str, Any]] = None
    if ch_df.empty:
        st.sidebar.info("Chưa có kênh.")
    else:
        options = []
        row_map: Dict[str, Dict[str, Any]] = {}
        for _, rr in ch_df.iterrows():
            row = rr.to_dict()
            rid = row.get("id")
            cid = safe_str(row.get("channel_id")).strip() or "None"
            label = safe_str(row.get("title")).strip() or safe_str(row.get("handle")).strip() or cid
            key = f"{label} • {cid} • id={rid}"
            options.append(key)
            row_map[key] = row
        pick = st.sidebar.selectbox("Chọn kênh", options=options, key="delete_channel_pick")
        sel_row = row_map.get(pick)

    delete_children = st.sidebar.toggle("Xoá kèm videos/snapshots", value=True, key="delete_children_toggle")
    if sel_row and st.sidebar.button("Xoá kênh", use_container_width=True, type="primary"):
        ok, msg = delete_channel_by_row(sel_row, delete_children=delete_children)
        (st.sidebar.success if ok else st.sidebar.error)(msg)

    st.sidebar.divider()
    st.sidebar.subheader("💵 RPM (ước tính)")
    auto_rpm = st.sidebar.toggle("Tự động gợi ý RPM", value=True, key="auto_rpm_toggle")
    rpm_long = st.sidebar.slider("RPM Long ($/1000 views)", 0.1, 30.0, 1.5, 0.1, key="rpm_long_val")
    rpm_shorts = st.sidebar.slider("RPM Shorts ($/1000 views)", 0.01, 5.0, 0.2, 0.01, key="rpm_shorts_val")
    viral_rel_threshold = st.sidebar.slider("Ngưỡng viral (Views/Subs ≥)", 1.0, 20.0, 3.0, 0.5, key="viral_threshold")

    st.sidebar.divider()
    st.sidebar.subheader("🌍 Audience (ước tính)")
    market_hint = st.sidebar.selectbox("Thị trường chính", ["Auto", "VN", "US/EN", "Global"], index=0, key="market_hint")

    videos_df = fetch_videos(limit=420)
    videos_df = ensure_df_columns(videos_df, {"video_id": "", "channel_id": "", "published_at": "", "title": "", "description": "", "tags_json": "", "niche": "", "sentiment": ""})

    vid_ids_all = [str(x) for x in videos_df["video_id"].astype(str).tolist() if str(x).strip()]
    latest_snap = fetch_latest_video_snapshots(vid_ids_all) if vid_ids_all else pd.DataFrame()

    videos_df = ensure_df_columns(videos_df, {"view_count": 0, "like_count": 0, "comment_count": 0})

    if not latest_snap.empty:
        latest_snap = ensure_df_columns(latest_snap, {"video_id": "", "view_count": 0, "like_count": 0, "comment_count": 0})
        latest_snap = coerce_int(latest_snap, ["view_count", "like_count", "comment_count"])

        base = videos_df.drop(columns=["view_count", "like_count", "comment_count"], errors="ignore").copy()
        base["video_id"] = base["video_id"].astype(str)
        latest_snap["video_id"] = latest_snap["video_id"].astype(str)

        merged = base.merge(latest_snap[["video_id", "view_count", "like_count", "comment_count"]], on="video_id", how="left")
        merged = ensure_df_columns(merged, {"view_count": 0, "like_count": 0, "comment_count": 0})
        merged = coerce_int(merged, ["view_count", "like_count", "comment_count"])
        videos_df = merged
    else:
        videos_df = coerce_int(videos_df, ["view_count", "like_count", "comment_count"])

    if auto_rpm:
        sug = auto_rpm_estimate(videos_df)
        st.sidebar.info(f"Gợi ý: Long ≈ ${sug['rpm_long']:.2f} | Shorts ≈ ${sug['rpm_shorts']:.2f}")

    tab1, tab2, tab3 = st.tabs(["📺 Tổng quan Video", "🚀 Outlier Finder", "👥 Kênh Đối thủ"])

    with tab1:
        if ch_df.empty:
            st.info("Chưa có kênh. Thêm kênh ở sidebar.")
        else:
            label_map = []
            row_map = {}
            for _, rr in ch_df.iterrows():
                row = rr.to_dict()
                cid = safe_str(row.get("channel_id")).strip()
                name = safe_str(row.get("title")).strip() or safe_str(row.get("handle")).strip() or cid
                key = f"{name} ({fmt_num(int(row.get('subscribers') or 0))} subs)"
                label_map.append(key)
                row_map[key] = row
            pick = st.selectbox("Chọn kênh để xem phân tích", options=label_map, index=0, key="ins_channel_pick")
            render_channel_insights(row_map[pick], videos_df, rpm_long=rpm_long, rpm_shorts=rpm_shorts, market_hint=market_hint)

        st.divider()
        c1, c2, c3 = st.columns(3)
        c1.metric("Tổng kênh", f"{len(ch_df):,}")
        c2.metric("Tổng video (đang hiển thị)", f"{len(videos_df):,}")
        c3.metric("Tổng subscribers", fmt_num(int(ch_df["subscribers"].sum()) if not ch_df.empty else 0))
        st.divider()

        f1, f2, f3 = st.columns([0.55, 0.22, 0.23])
        q = f1.text_input("Tìm theo tiêu đề", value="", placeholder="Search…", key="search_title")
        sort_mode = f2.selectbox("Sắp xếp", ["Mới nhất", "Nhiều view"], index=0, key="sort_mode")
        show_n = f3.selectbox("Hiển thị", [24, 48, 72, 120], index=1, key="show_n")

        df_show = videos_df.copy()
        if q.strip():
            df_show = df_show[df_show["title"].astype(str).str.contains(q.strip(), case=False, na=False)]

        if sort_mode == "Nhiều view":
            df_show = df_show.sort_values("view_count", ascending=False)
        else:
            df_show["_dt"] = pd.to_datetime(df_show["published_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
            df_show = df_show.sort_values("_dt", ascending=False).drop(columns=["_dt"], errors="ignore")

        df_show = df_show.head(int(show_n))
        render_video_cards(df_show, ch_df, rpm_long=rpm_long, rpm_shorts=rpm_shorts, viral_rel_threshold=viral_rel_threshold, columns=4)

    with tab2:
        st.caption("Lọc video trong N ngày qua có **Views ≥ 3× Subscribers**.")
        days = st.slider("Khoảng ngày", 1, 30, 7, key="outlier_days")
        ratio = st.slider("Ngưỡng Views/Subs", 1.0, 20.0, 3.0, 0.5, key="outlier_ratio")

        if videos_df.empty or ch_df.empty:
            st.info("Chưa đủ dữ liệu (cần channels + videos + snapshots).")
        else:
            df_out = videos_df.copy()
            df_out["_p"] = pd.to_datetime(df_out["published_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
            df_out = df_out.dropna(subset=["_p"])
            df_out = df_out[df_out["_p"] >= (utc_now() - timedelta(days=int(days)))].copy()

            subs_map = {safe_str(rr["channel_id"]).strip(): int(rr["subscribers"]) for _, rr in ch_df.iterrows()}
            name_map = {
                safe_str(rr["channel_id"]).strip(): (safe_str(rr["title"]).strip() or safe_str(rr["handle"]).strip() or safe_str(rr["channel_id"]).strip())
                for _, rr in ch_df.iterrows()
            }

            df_out["subs"] = df_out["channel_id"].astype(str).map(subs_map).fillna(0).astype(int)
            df_out["ratio"] = df_out["view_count"] / df_out["subs"].clip(lower=1)
            df_out = df_out[(df_out["subs"] > 0) & (df_out["ratio"] >= float(ratio))].copy()
            df_out = df_out.sort_values(["ratio", "view_count"], ascending=[False, False])
            df_out["channel_title"] = df_out["channel_id"].astype(str).map(name_map).fillna(df_out["channel_id"].astype(str))

            show = df_out.rename(columns={"channel_title": "Kênh", "title": "Video", "view_count": "Views", "subs": "Subscribers", "ratio": "Views/Subs"})[
                ["Kênh", "Video", "Views", "Subscribers", "Views/Subs"]
            ]
            show["Link"] = (YOUTUBE_WATCH + df_out["video_id"].astype(str)).values
            st.dataframe(show, use_container_width=True, height=520)

    with tab3:
        if ch_df.empty:
            st.info("Chưa có kênh trong bảng channels.")
        else:
            st.metric("Tổng subscribers", fmt_num(int(ch_df["subscribers"].sum())))
            show = ch_df.copy()
            show["Tên hiển thị"] = show["title"].astype(str).str.strip()
            show.loc[show["Tên hiển thị"].eq(""), "Tên hiển thị"] = show["handle"].astype(str).str.strip()
            show.loc[show["Tên hiển thị"].eq(""), "Tên hiển thị"] = show["channel_id"].astype(str)
            st.dataframe(
                show[["Tên hiển thị", "handle", "channel_id", "subscribers", "created_at"]].rename(
                    columns={"handle": "Handle", "channel_id": "Channel ID", "subscribers": "Subscribers", "created_at": "Ngày thêm"}
                ),
                use_container_width=True,
                height=560,
            )


if __name__ == "__main__":
    main()
