# app.py
# -*- coding: utf-8 -*-
"""
toolwatch (Streamlit Frontend-only) — Supabase

Mục tiêu: UI dark + bố cục "giống NexLev" trong giới hạn Streamlit.
✅ Không sqlite3
✅ Không gọi YouTube API (chỉ SELECT/INSERT/DELETE Supabase)
✅ Sidebar luôn hiện, ẩn nút << (collapse)
✅ DB trống vẫn chạy
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


# -------------------------
# Helpers
# -------------------------
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
    """Ước tính RPM dựa trên language + engagement (heuristic)."""
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

    base_long = 3.5 if lang == "en" else (1.8 if lang == "vi" else 2.4)
    base_long += clamp((eng - 0.01) * 80, 0.0, 2.0)

    base_shorts = (0.15 if lang == "vi" else (0.35 if lang == "en" else 0.25))
    base_shorts += clamp((eng - 0.008) * 60, 0.0, 0.6)

    return {"rpm_long": clamp(base_long, 0.3, 12.0), "rpm_shorts": clamp(base_shorts, 0.03, 2.0)}


def audience_estimator(lang: str, niche: str = "") -> Dict[str, Any]:
    """
    Ước tính audience (NOT real).
    Output: geos list, gender dict, ages list.
    """
    niche = (niche or "").lower()

    # Base by language
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

    return {
        "geos": geos,
        "gender": {"Male": male, "Female": female, "User-specified": 0},
        "ages": ages,
    }


# -------------------------
# Supabase
# -------------------------
@st.cache_resource
def supa() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)


# -------------------------
# Data fetch
# -------------------------
@st.cache_data(ttl=120, show_spinner=False)
def fetch_channels() -> pd.DataFrame:
    client = supa()
    res = (
        client.table("channels")
        .select("id,channel_id,title,handle,avatar_url,subscribers,created_at")
        .order("subscribers", desc=True)
        .execute()
    )
    df = pd.DataFrame(res.data or [])
    if df.empty:
        df = pd.DataFrame(columns=["id", "channel_id", "title", "handle", "avatar_url", "subscribers", "created_at"])
    df = ensure_df_columns(
        df,
        {"id": None, "channel_id": "", "title": "", "handle": "", "avatar_url": "", "subscribers": 0, "created_at": ""},
    )
    df = coerce_int(df, ["subscribers"])
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_videos(limit: int = 300) -> pd.DataFrame:
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
        df = pd.DataFrame(
            columns=["video_id", "channel_id", "published_at", "title", "description", "tags_json", "niche", "sentiment"]
        )
    df = ensure_df_columns(
        df,
        {"video_id": "", "channel_id": "", "published_at": "", "title": "", "description": "", "tags_json": "", "niche": "", "sentiment": ""},
    )
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


@st.cache_data(ttl=120, show_spinner=False)
def fetch_snapshots_since(video_ids: List[str], since_iso: str, hard_limit: int = 200000) -> pd.DataFrame:
    """Fetch snapshots >= since for a set of video ids (cap rows)."""
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


# -------------------------
# Mutations
# -------------------------
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

        fetch_channels.clear()
        fetch_videos.clear()
        fetch_latest_video_snapshots.clear()
        fetch_snapshots_since.clear()
        fetch_latest_scan_time.clear()
        return True, "✅ Đã thêm. Robot sẽ tự quét và đổ data vào videos/snapshots."
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

        fetch_channels.clear()
        fetch_videos.clear()
        fetch_latest_video_snapshots.clear()
        fetch_snapshots_since.clear()
        fetch_latest_scan_time.clear()
        return True, "✅ Đã xoá kênh."
    except Exception as e:
        return False, f"❌ Xoá kênh thất bại: {e}"


# -------------------------
# CSS (NexLev-ish)
# -------------------------
def inject_css():
    st.markdown(
        """
<style>
  header[data-testid="stHeader"]{ background: transparent !important; }
  div[data-testid="stToolbar"]{ display:none !important; }
  #MainMenu{ visibility:hidden; }
  footer{ visibility:hidden; }

  /* Sidebar: always expanded + remove collapse button */
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

  /* Generic card */
  .nx-card{
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,0.10);
    background: rgba(255,255,255,0.02);
    padding: 12px 12px;
  }
  .nx-head{
    display:flex; align-items:center; justify-content:space-between;
    margin-bottom: 8px;
  }
  .nx-title{
    font-weight: 900;
    font-size: 14px;
    display:flex; align-items:center; gap:8px;
  }
  .nx-reveal{
    padding: 6px 10px;
    border-radius: 10px;
    border: 1px solid rgba(255,255,255,0.12);
    background: rgba(255,255,255,0.03);
    font-weight: 900;
    font-size: 12px;
    color: rgba(230,232,238,0.9);
  }
  .nx-big{
    font-weight: 900;
    font-size: 40px;
    letter-spacing: -0.02em;
    margin-top: 2px;
  }
  .nx-sub{
    color: rgba(230,232,238,0.65);
    font-size: 12px;
    margin-top: 4px;
  }

  /* Video card */
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

  /* Altair container */
  .stAltairChart{ border-radius: 14px; overflow:hidden; }
</style>
        """,
        unsafe_allow_html=True,
    )


# -------------------------
# NexLev-like insight block
# -------------------------
def card_start(title: str, icon: str, reveal_key: str, default_reveal: bool = True) -> bool:
    """Render a header similar to NexLev and return reveal state."""
    if reveal_key not in st.session_state:
        st.session_state[reveal_key] = default_reveal
    col_l, col_r = st.columns([0.78, 0.22])
    with col_l:
        st.markdown(f"<div class='nx-title'>{icon} {title}</div>", unsafe_allow_html=True)
    with col_r:
        btn = st.button("✨ Reveal", key=f"{reveal_key}_btn", use_container_width=True)
        if btn:
            st.session_state[reveal_key] = not bool(st.session_state[reveal_key])
    return bool(st.session_state[reveal_key])


def nx_metric_card(title: str, icon: str, value: str, sub: str, reveal_key: str, reveal_default: bool = True):
    with st.container(border=True):
        reveal = card_start(title, icon, reveal_key, default_reveal=reveal_default)
        if reveal:
            st.markdown(f"<div class='nx-big'>{value}</div><div class='nx-sub'>{sub}</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div class='nx-big'>•••</div><div class='nx-sub'>Bấm Reveal</div>", unsafe_allow_html=True)


def nx_bar_list(title: str, icon: str, rows: List[Tuple[str, float]], reveal_key: str, note: str):
    with st.container(border=True):
        reveal = card_start(title, icon, reveal_key, default_reveal=True)
        if not reveal:
            st.markdown("<div class='nx-sub'>Bấm Reveal</div>", unsafe_allow_html=True)
            return
        if note:
            st.caption(note)
        if not rows:
            st.info("Chưa có dữ liệu.")
            return
        df = pd.DataFrame(rows, columns=["name", "pct"])
        # progress bars
        for _, r in df.iterrows():
            left, right = st.columns([0.75, 0.25])
            with left:
                st.write(r["name"])
                st.progress(float(r["pct"]) / 100.0)
            with right:
                st.write(f"{float(r['pct']):.1f}%")


def compute_channel_window_metrics(
    videos_df: pd.DataFrame,
    channel_id: str,
    rpm_long: float,
    rpm_shorts: float,
    window_days: int,
    max_videos: int = 120,
) -> Dict[str, Any]:
    """Compute views/likes/comments deltas for last N days based on snapshots."""
    ch_vids = videos_df[videos_df["channel_id"].astype(str) == str(channel_id)].copy()
    if ch_vids.empty:
        return {"views": 0, "likes": 0, "comments": 0, "rev": 0.0, "shorts_views": 0, "long_views": 0, "lang": "unknown", "niche": ""}

    # limit to most recent N videos to cap snapshot load
    ch_vids["_dt"] = pd.to_datetime(ch_vids["published_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
    ch_vids = ch_vids.sort_values("_dt", ascending=False).drop(columns=["_dt"], errors="ignore").head(int(max_videos))

    titles = [safe_str(x) for x in ch_vids["title"].tolist()[:200]]
    lang = guess_lang_from_titles(titles)

    # pick a "dominant niche" if exists
    niche = ""
    if "niche" in ch_vids.columns:
        vc = ch_vids["niche"].astype(str).value_counts()
        if len(vc) > 0:
            niche = vc.index[0]

    since_dt = utc_now() - timedelta(days=int(window_days))
    since_iso = since_dt.isoformat()
    vid_ids = [str(x) for x in ch_vids["video_id"].astype(str).tolist() if str(x).strip()]
    snap = fetch_snapshots_since(vid_ids, since_iso)

    if snap.empty:
        # fallback: use latest view_count already merged (still ok)
        ch_vids = ensure_df_columns(ch_vids, {"view_count": 0, "like_count": 0, "comment_count": 0, "tags_json": ""})
        ch_vids = coerce_int(ch_vids, ["view_count", "like_count", "comment_count"])
        views = int(ch_vids["view_count"].sum())
        likes = int(ch_vids["like_count"].sum())
        comments = int(ch_vids["comment_count"].sum())
        return {"views": views, "likes": likes, "comments": comments, "rev": (views/1000.0)*rpm_long, "shorts_views": 0, "long_views": views, "lang": lang, "niche": niche}

    snap["captured_at_dt"] = pd.to_datetime(snap["captured_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
    snap = snap.dropna(subset=["captured_at_dt"]).sort_values(["video_id", "captured_at_dt"])

    # delta per video: last - first within window
    first = snap.groupby("video_id").first(numeric_only=False)
    last = snap.groupby("video_id").last(numeric_only=False)

    views_delta = (last["view_count"] - first["view_count"]).clip(lower=0)
    likes_delta = (last["like_count"] - first["like_count"]).clip(lower=0)
    comments_delta = (last["comment_count"] - first["comment_count"]).clip(lower=0)

    delta = pd.DataFrame({"video_id": views_delta.index, "views": views_delta.values, "likes": likes_delta.values, "comments": comments_delta.values})
    delta["video_id"] = delta["video_id"].astype(str)

    # attach shorts/long for rpm blend
    ch_vids = ensure_df_columns(ch_vids, {"tags_json": "", "title": ""})
    ch_vids["video_id"] = ch_vids["video_id"].astype(str)
    merged = ch_vids[["video_id", "title", "tags_json"]].merge(delta, on="video_id", how="left")
    merged = ensure_df_columns(merged, {"views": 0, "likes": 0, "comments": 0})
    merged = coerce_int(merged, ["views", "likes", "comments"])

    merged["is_shorts"] = merged.apply(lambda r: is_shorts_guess(str(r["title"]), str(r["tags_json"])), axis=1)
    shorts_views = int(merged.loc[merged["is_shorts"], "views"].sum())
    long_views = int(merged.loc[~merged["is_shorts"], "views"].sum())

    rev = (long_views / 1000.0) * float(rpm_long) + (shorts_views / 1000.0) * float(rpm_shorts)

    return {
        "views": int(merged["views"].sum()),
        "likes": int(merged["likes"].sum()),
        "comments": int(merged["comments"].sum()),
        "rev": float(rev),
        "shorts_views": shorts_views,
        "long_views": long_views,
        "lang": lang,
        "niche": niche,
        "video_ids_used": len(vid_ids),
    }


def build_views_timeseries(
    videos_df: pd.DataFrame,
    channel_id: str,
    window: str,
    max_videos: int = 80,
) -> pd.DataFrame:
    """Return df with columns: date, total_views (delta within day cumulative approximation)."""
    # time range
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

    ch_vids = videos_df[videos_df["channel_id"].astype(str) == str(channel_id)].copy()
    if ch_vids.empty:
        return pd.DataFrame(columns=["date", "views"])

    ch_vids["_dt"] = pd.to_datetime(ch_vids["published_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
    ch_vids = ch_vids.sort_values("_dt", ascending=False).drop(columns=["_dt"], errors="ignore").head(int(max_videos))

    vid_ids = [str(x) for x in ch_vids["video_id"].astype(str).tolist() if str(x).strip()]
    snap = fetch_snapshots_since(vid_ids, since.isoformat(), hard_limit=200000)
    if snap.empty:
        return pd.DataFrame(columns=["date", "views"])

    snap["captured_at_dt"] = pd.to_datetime(snap["captured_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
    snap = snap.dropna(subset=["captured_at_dt"])
    snap["date"] = snap["captured_at_dt"].dt.floor("D")

    # For each video+date get last view_count, then sum across videos
    last_per_day = snap.sort_values("captured_at_dt").groupby(["video_id", "date"]).last(numeric_only=False).reset_index()
    daily = last_per_day.groupby("date")["view_count"].sum().reset_index()
    daily = daily.sort_values("date")

    # Convert to delta per day (growth)
    daily["views"] = daily["view_count"].diff().fillna(0).clip(lower=0).astype(int)
    return daily[["date", "views"]]


def render_channel_insights(
    channel_row: Dict[str, Any],
    videos_df: pd.DataFrame,
    rpm_long: float,
    rpm_shorts: float,
):
    cid = safe_str(channel_row.get("channel_id")).strip()
    ch_name = safe_str(channel_row.get("title")).strip() or safe_str(channel_row.get("handle")).strip() or cid
    subs = int(channel_row.get("subscribers") or 0)

    # time range selection
    left, right = st.columns([0.75, 0.25])
    with left:
        st.subheader(f"📈 Channel Insights — {ch_name}")
        st.caption("Bố cục mô phỏng NexLev. Một số mục là **ước tính** (không phải số thật).")
    with right:
        window_key = st.radio("Khoảng", ["7D", "14D", "30D", "3M", "1Y"], horizontal=True, index=2, key="ins_window")
    window_days = {"7D": 7, "14D": 14, "30D": 30, "3M": 90, "1Y": 365}[window_key]

    m30 = compute_channel_window_metrics(videos_df, cid, rpm_long, rpm_shorts, window_days=30, max_videos=140)
    rev30 = m30["rev"]
    views30 = m30["views"]

    # RPM card label
    rpm_level = "Low"
    rpm_show = (rpm_long + rpm_shorts) / 2.0
    if rpm_show >= 4:
        rpm_level = "High"
    elif rpm_show >= 2:
        rpm_level = "Medium"

    # Top row (3 cards)
    c1, c2, c3 = st.columns([1, 1, 1], gap="large")
    with c1:
        nx_metric_card("Channel Revenue", "👁️", f"${rev30:,.0f}", "Last 30 Days (ước tính)", "reveal_rev")
    with c2:
        nx_metric_card("Views", "👁️", f"{views30:,.0f}", "Last 30 Days", "reveal_views")
    with c3:
        nx_metric_card("RPM", "💲", f"${rpm_show:.2f}", rpm_level, "reveal_rpm")

    # Second row
    l2, r2 = st.columns([1.65, 1.0], gap="large")
    with l2:
        with st.container(border=True):
            _ = card_start("Videos vs Shorts Views", "🎬", "reveal_vs", default_reveal=True)
            total = max(1, m30["views"])
            long_v = int(m30["long_views"])
            short_v = int(m30["shorts_views"])
            st.progress(min(1.0, long_v / total), text="Video Views")
            st.write(f"• Video: **{fmt_num(long_v)}** ({(long_v/total)*100:.1f}%)")
            st.write(f"• Shorts: **{fmt_num(short_v)}** ({(short_v/total)*100:.1f}%)")
            st.caption(f"Dựa trên ~{m30.get('video_ids_used', 0)} video gần nhất trong DB.")
    with r2:
        # Estimated geos
        lang = m30.get("lang", "unknown")
        niche = m30.get("niche", "")
        est = audience_estimator(lang=lang, niche=niche)
        nx_bar_list("Top Geographies", "🌍", est["geos"], "reveal_geo", note="Ước tính theo ngôn ngữ/ngách (không phải YouTube Analytics).")

    # Third row
    l3, r3 = st.columns([1.65, 1.0], gap="large")
    with l3:
        with st.container(border=True):
            _ = card_start("Views & Subscribers Graph", "📈", "reveal_graph", default_reveal=True)

            metric_mode = st.radio("Chỉ số", ["Views", "Subscribers"], horizontal=True, index=0, key="graph_metric")
            # Use same window buttons as NexLev
            window_sel = st.radio("Range", ["7D", "14D", "30D", "3M", "1Y"], horizontal=True, index=["7D","14D","30D","3M","1Y"].index(window_key), key="graph_range")

            if metric_mode == "Subscribers":
                # No history => constant line
                df = build_views_timeseries(videos_df, cid, window_sel, max_videos=60)
                if df.empty:
                    st.info("Chưa đủ dữ liệu snapshots để vẽ.")
                else:
                    df2 = df.copy()
                    df2["value"] = subs
                    chart = (
                        alt.Chart(df2)
                        .mark_line()
                        .encode(
                            x=alt.X("date:T", title=None),
                            y=alt.Y("value:Q", title=None),
                            tooltip=[alt.Tooltip("date:T", title="Date"), alt.Tooltip("value:Q", title="Subscribers")],
                        )
                        .properties(height=260)
                    )
                    st.altair_chart(chart, use_container_width=True)
                    st.caption("Subscribers history chưa có → hiển thị đường phẳng (cần lưu snapshot subscribers).")
            else:
                df = build_views_timeseries(videos_df, cid, window_sel, max_videos=80)
                if df.empty:
                    st.info("Chưa đủ dữ liệu snapshots để vẽ.")
                else:
                    # Agg to monthly if 1Y
                    if window_sel == "1Y":
                        dfm = df.copy()
                        dfm["month"] = dfm["date"].dt.to_period("M").dt.to_timestamp()
                        dfm = dfm.groupby("month")["views"].sum().reset_index().rename(columns={"month": "date"})
                        df = dfm
                    chart = (
                        alt.Chart(df)
                        .mark_area(line={"color": "#34d399", "strokeWidth": 2}, opacity=0.45)
                        .encode(
                            x=alt.X("date:T", title=None),
                            y=alt.Y("views:Q", title=None),
                            tooltip=[alt.Tooltip("date:T", title="Date"), alt.Tooltip("views:Q", title="Views")],
                        )
                        .properties(height=260)
                    )
                    st.altair_chart(chart, use_container_width=True)
                    st.caption("Views = tăng trưởng theo ngày dựa trên snapshots (ước tính).")
    with r3:
        # Age/Gender estimated
        lang = m30.get("lang", "unknown")
        niche = m30.get("niche", "")
        est = audience_estimator(lang=lang, niche=niche)
        with st.container(border=True):
            _ = card_start("Age and Gender", "👥", "reveal_demo", default_reveal=True)
            g = est["gender"]
            # gender bars
            for k in ["Male", "Female", "User-specified"]:
                pct = float(g.get(k, 0))
                left, right = st.columns([0.75, 0.25])
                with left:
                    st.write(k)
                    st.progress(pct / 100.0)
                with right:
                    st.write(f"{pct:.1f}%")
            st.divider()
            for name, pct in est["ages"]:
                left, right = st.columns([0.75, 0.25])
                with left:
                    st.write(name)
                    st.progress(float(pct) / 100.0)
                with right:
                    st.write(f"{float(pct):.1f}%")
            st.caption("⚠️ Đây là ước tính theo language/ngách, không phải số thật.")


def render_video_cards(
    videos: pd.DataFrame,
    channels: pd.DataFrame,
    rpm_long: float,
    rpm_shorts: float,
    viral_rel_threshold: float,
    columns: int = 4,
):
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

            views = int(r["view_count"])
            likes = int(r["like_count"])
            comments = int(r["comment_count"])

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
            html = "\n".join([ln for ln in lines if ln and ln != "None"])
            st.markdown(html, unsafe_allow_html=True)


# -------------------------
# Main
# -------------------------
def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide", initial_sidebar_state="expanded")
    inject_css()

    # Top bar
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

    # Sidebar
    st.sidebar.header("⚙️ Điều khiển")
    if st.sidebar.button("🔄 Refresh dữ liệu", use_container_width=True):
        fetch_channels.clear()
        fetch_videos.clear()
        fetch_latest_video_snapshots.clear()
        fetch_snapshots_since.clear()
        fetch_latest_scan_time.clear()
        st.rerun()

    st.sidebar.subheader("🛰️ Trạng thái robot")
    last_scan = fetch_latest_scan_time()
    if last_scan:
        st.sidebar.caption(f"Lần cập nhật gần nhất: **{time_ago_vi(last_scan)}**")
        dt = parse_dt_any(last_scan)
        if dt:
            elapsed = max(0.0, (utc_now() - dt).total_seconds())
            cycle = 4 * 3600.0
            pct = min(1.0, elapsed / cycle)
            st.sidebar.progress(pct, text=f"Chu kỳ 4 giờ: {int(pct*100)}%")
    else:
        st.sidebar.caption("Chưa thấy dữ liệu scan (snapshots rỗng).")

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

    # Load data
    videos_df = fetch_videos(limit=320)
    videos_df = ensure_df_columns(
        videos_df,
        {"video_id": "", "channel_id": "", "published_at": "", "title": "", "description": "", "tags_json": "", "niche": "", "sentiment": ""},
    )

    # Attach latest snapshot metrics to videos_df
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

    # Auto rpm suggestion
    if auto_rpm:
        sug = auto_rpm_estimate(videos_df)
        st.sidebar.info(f"Gợi ý: Long ≈ ${sug['rpm_long']:.2f} | Shorts ≈ ${sug['rpm_shorts']:.2f}")

    # Tabs
    tab1, tab2, tab3 = st.tabs(["📺 Tổng quan Video", "🚀 Outlier Finder", "👥 Kênh Đối thủ"])

    with tab1:
        # choose channel for insights
        if ch_df.empty:
            st.info("Chưa có kênh. Thêm kênh ở sidebar.")
        else:
            # channel selector like NexLev context
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
            render_channel_insights(row_map[pick], videos_df, rpm_long=rpm_long, rpm_shorts=rpm_shorts)

        # quick counters
        st.divider()
        c1, c2, c3 = st.columns(3)
        c1.metric("Tổng kênh", f"{len(ch_df):,}")
        c2.metric("Tổng video (đang hiển thị)", f"{len(videos_df):,}")
        c3.metric("Tổng subscribers", fmt_num(int(ch_df["subscribers"].sum()) if not ch_df.empty else 0))
        st.divider()

        # controls
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
