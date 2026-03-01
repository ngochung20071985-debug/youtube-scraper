# app.py
# -*- coding: utf-8 -*-
"""
toolwatch (Streamlit Frontend-only) — Supabase

✅ Không dùng sqlite3
✅ Không gọi YouTube API (chỉ SELECT/INSERT/DELETE Supabase)
✅ Sidebar luôn hiện (không collapse, không có nút <<)
✅ Chạy mượt kể cả DB trống
✅ Hiển thị:
  - Tab 1: Tổng quan Video (grid/card)
  - Tab 2: Outlier Finder (7 ngày: views >= 3x subs)
  - Tab 3: Kênh đối thủ (list kênh + subs)
"""

from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

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


def to_int(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


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
    """
    Accept:
    - UC... (channel_id)
    - https://youtube.com/channel/UC...
    - @handle
    - https://youtube.com/@handle
    """
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


def is_shorts_guess(title: str, tags_json: str) -> bool:
    t = (title or "").lower()
    if "#shorts" in t or "shorts" in t:
        return True
    tj = (tags_json or "").lower()
    return '"shorts"' in tj or "shorts" in tj


def detect_vietnamese(text: str) -> bool:
    return bool(
        re.search(r"[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]", (text or "").lower())
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


def auto_rpm_estimate(videos_df: pd.DataFrame) -> Dict[str, float]:
    """
    Heuristic RPM auto estimate (không phải số thật).
    """
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
    eng = (likes_sum + comm_sum) / max(1, views_sum)

    if lang == "en":
        base_long = 3.5
    elif lang == "vi":
        base_long = 1.8
    else:
        base_long = 2.4

    base_long += min(2.0, max(0.0, (eng - 0.01) * 80))
    base_shorts = (0.15 if lang == "vi" else (0.35 if lang == "en" else 0.25)) + min(0.6, max(0.0, (eng - 0.008) * 60))

    rpm_long = float(max(0.3, min(12.0, base_long)))
    rpm_shorts = float(max(0.03, min(2.0, base_shorts)))
    return {"rpm_long": rpm_long, "rpm_shorts": rpm_shorts}


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
    df = ensure_df_columns(df, {"id": None, "channel_id": "", "title": "", "handle": "", "avatar_url": "", "subscribers": 0, "created_at": ""})
    df = coerce_int(df, ["subscribers"])
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_videos(limit: int = 120) -> pd.DataFrame:
    """
    Schema:
    videos: video_id, channel_id, published_at, title, description, tags_json, niche, sentiment (optional)
    """
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
    df = ensure_df_columns(df, {
        "video_id": "", "channel_id": "", "published_at": "", "title": "", "description": "", "tags_json": "", "niche": "", "sentiment": ""
    })
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_latest_video_snapshots(video_ids: List[str]) -> pd.DataFrame:
    if not video_ids:
        return pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])

    client = supa()
    rows: List[Dict[str, Any]] = []
    CHUNK = 150

    for i in range(0, len(video_ids), CHUNK):
        chunk = video_ids[i:i+CHUNK]
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
def fetch_latest_scan_time() -> Optional[str]:
    client = supa()
    try:
        r = client.table("snapshots").select("captured_at").order("captured_at", desc=True).limit(1).execute()
        row = (r.data or [None])[0]
        if row and row.get("captured_at"):
            return str(row["captured_at"])
    except Exception:
        pass

    try:
        r = client.table("videos").select("published_at").order("published_at", desc=True).limit(1).execute()
        row = (r.data or [None])[0]
        if row and row.get("published_at"):
            return str(row["published_at"])
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
        return False, "Nhập UC... hoặc URL /channel/UC... hoặc @handle (vd: @MrBeast)."

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
        fetch_latest_scan_time.clear()
        return True, "✅ Đã thêm. Robot sẽ tự quét và đổ dữ liệu vào videos/snapshots."
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
        fetch_latest_scan_time.clear()
        return True, "✅ Đã xoá kênh."
    except Exception as e:
        return False, f"❌ Xoá kênh thất bại: {e}"


# -------------------------
# CSS / UI
# -------------------------
def inject_css():
    st.markdown(
        """
        <style>
          header[data-testid="stHeader"]{ background: transparent !important; }
          div[data-testid="stToolbar"]{ display:none !important; }
          #MainMenu{ visibility:hidden; }
          footer{ visibility:hidden; }

          /* FORCE SIDEBAR ALWAYS VISIBLE + REMOVE << */
          section[data-testid="stSidebar"]{
            transform:none !important;
            margin-left:0 !important;
            visibility:visible !important;
            opacity:1 !important;
            display:block !important;
          }
          section[data-testid="stSidebar"][aria-expanded="false"]{ transform:none !important; }
          div[data-testid="collapsedControl"]{ display:none !important; } /* remove << */

          @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800;900&display=swap');
          html, body, [class*="css"]{ font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important; }

          .stApp{
            background: linear-gradient(180deg, #0b0b0b 0%, #090909 100%);
            color: rgba(230,232,238,0.92);
          }
          .block-container{ max-width: 1500px; padding-top: 0.8rem; padding-bottom: 2rem; }

          .tw-top{
            display:flex; align-items:center; justify-content:space-between;
            padding: 10px 12px; border-radius: 16px;
            border: 1px solid rgba(255,255,255,0.10);
            background: rgba(255,255,255,0.02);
            margin-bottom: 14px;
          }
          .tw-brand{ font-weight: 900; font-size: 18px; }
          .tw-pill{
            padding: 6px 10px; border-radius: 999px;
            border: 1px solid rgba(255,255,255,0.12);
            background: rgba(255,255,255,0.03);
            font-weight: 800; font-size: 12px;
          }

          .tw-grid{
            display:grid;
            grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
            gap: 16px;
          }
          .tw-card{ border-radius: 16px; }
          .tw-thumb{
            position:relative; width:100%; aspect-ratio:16/9;
            border-radius: 16px; overflow:hidden;
            border:1px solid rgba(255,255,255,0.10);
            background: rgba(255,255,255,0.04);
          }
          .tw-thumb img{ width:100%; height:100%; object-fit:cover; display:block; }
          .tw-badge{
            position:absolute; left:10px; top:10px;
            padding: 3px 8px; border-radius: 999px;
            font-weight: 900; font-size: 12px;
            border: 1px solid rgba(34,197,94,0.45);
            background: rgba(34,197,94,0.12);
            color: #86efac;
          }
          .tw-meta{ padding: 8px 2px 0 2px; }
          .tw-title{
            font-weight: 900; font-size: 14px; line-height: 1.25;
            display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical;
            overflow:hidden; min-height: 36px;
          }
          .tw-sub{
            margin-top: 6px;
            color: rgba(230,232,238,0.65);
            font-size: 12px;
            white-space: nowrap; overflow:hidden; text-overflow:ellipsis;
          }
          .tw-metrics{
            margin-top: 6px;
            color: rgba(230,232,238,0.78);
            font-size: 12px;
            display:flex; gap:10px; flex-wrap:wrap;
          }
          .tw-metrics b{ color: rgba(230,232,238,0.92); }
          .tw-open{ text-decoration:none; color: inherit; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_video_grid(
    videos: pd.DataFrame,
    channels: pd.DataFrame,
    rpm_long: float,
    rpm_shorts: float,
    viral_rel_threshold: float,
):
    if videos.empty:
        st.info("Chưa có video trong bảng videos. (Chờ robot scraper chạy và đổ data)")
        return

    ch_map: Dict[str, Dict[str, Any]] = {}
    if not channels.empty:
        for _, r in channels.iterrows():
            cid = safe_str(r["channel_id"]).strip()
            name = safe_str(r["title"]).strip() or safe_str(r["handle"]).strip() or cid or "(unknown)"
            ch_map[cid] = {"name": name, "subs": int(r["subscribers"])}

    parts = ["<div class='tw-grid'>"]
    for _, r in videos.iterrows():
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

        badge = "<div class='tw-badge'>✅🔥 VIRAL</div>" if viral else ""

        parts.append(
            f"""
            <div class="tw-card">
              <a class="tw-open" href="{url}" target="_blank" rel="noopener">
                <div class="tw-thumb">
                  <img src="{thumb}" />
                  {badge}
                </div>
                <div class="tw-meta">
                  <div class="tw-title">{title}</div>
                  <div class="tw-sub">{ch_name} • {fmt_num(views)} lượt xem • {ago}</div>
                  <div class="tw-metrics">
                    <span>👁️ <b>{views:,}</b></span>
                    <span>👍 <b>{likes:,}</b></span>
                    <span>💬 <b>{comments:,}</b></span>
                    <span>💵 <b>≈${rev:,.2f}</b></span>
                  </div>
                </div>
              </a>
            </div>
            """
        )

    parts.append("</div>")
    st.markdown("\n".join(parts), unsafe_allow_html=True)


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

    # Sidebar
    st.sidebar.header("⚙️ Điều khiển")

    if st.sidebar.button("🔄 Refresh dữ liệu", use_container_width=True):
        fetch_channels.clear()
        fetch_videos.clear()
        fetch_latest_video_snapshots.clear()
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
        st.sidebar.caption("Chưa thấy dữ liệu scan (snapshots/videos rỗng).")

    if st.sidebar.button("🧪 Test DB (đếm rows)", use_container_width=True):
        client = supa()
        try:
            c = client.table("channels").select("id", count="exact").execute().count
        except Exception:
            c = "?"
        try:
            v = client.table("videos").select("video_id", count="exact").execute().count
        except Exception:
            v = "?"
        try:
            s = client.table("snapshots").select("video_id", count="exact").execute().count
        except Exception:
            s = "?"
        st.sidebar.write({"channels": c, "videos": v, "snapshots": s})

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
        for _, r in ch_df.iterrows():
            row = r.to_dict()
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

    # RPM
    st.sidebar.divider()
    st.sidebar.subheader("💵 RPM (ước tính)")
    auto_rpm = st.sidebar.toggle("Tự động gợi ý RPM", value=True, key="auto_rpm_toggle")
    rpm_long = st.sidebar.slider("RPM Long ($/1000 views)", 0.1, 30.0, float(st.session_state.get("rpm_long_val", 1.5)), 0.1, key="rpm_long_val")
    rpm_shorts = st.sidebar.slider("RPM Shorts ($/1000 views)", 0.01, 5.0, float(st.session_state.get("rpm_shorts_val", 0.2)), 0.01, key="rpm_shorts_val")
    viral_rel_threshold = st.sidebar.slider("Ngưỡng viral (Views/Subs ≥)", 1.0, 20.0, 3.0, 0.5, key="viral_threshold")

    # Load data
    videos_df = fetch_videos(limit=int(st.secrets.get("DEFAULT_VIDEO_LIMIT", 120)))
    if videos_df.empty:
        videos_df = pd.DataFrame(columns=["video_id", "channel_id", "published_at", "title", "description", "tags_json", "niche", "sentiment"])
    videos_df = ensure_df_columns(videos_df, {"video_id": "", "channel_id": "", "published_at": "", "title": "", "description": "", "tags_json": "", "niche": "", "sentiment": ""})

    # Gắn metrics mới nhất từ snapshots vào videos (view/like/comment)
    vid_ids = [str(x) for x in videos_df["video_id"].astype(str).tolist() if str(x).strip()]
    snap_df = fetch_latest_video_snapshots(vid_ids) if vid_ids else pd.DataFrame()

    # Luôn đảm bảo có 3 cột metrics để UI không crash
    videos_df = ensure_df_columns(videos_df, {"view_count": 0, "like_count": 0, "comment_count": 0})

    if not snap_df.empty:
        snap_df = ensure_df_columns(snap_df, {"video_id": "", "view_count": 0, "like_count": 0, "comment_count": 0})
        snap_df = coerce_int(snap_df, ["view_count", "like_count", "comment_count"])

        base = videos_df.drop(columns=["view_count", "like_count", "comment_count"], errors="ignore").copy()
        base["video_id"] = base["video_id"].astype(str)
        snap_df["video_id"] = snap_df["video_id"].astype(str)

        merged = base.merge(
            snap_df[["video_id", "view_count", "like_count", "comment_count"]],
            on="video_id",
            how="left",
        )

        # Nếu thiếu cột (trường hợp DB lạ), tạo fallback 0
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
        c1, c2, c3 = st.columns(3)
        c1.metric("Tổng kênh", f"{len(ch_df):,}")
        c2.metric("Tổng video (đang hiển thị)", f"{len(videos_df):,}")
        c3.metric("Tổng subscribers", fmt_num(int(ch_df["subscribers"].sum()) if not ch_df.empty else 0))
        st.divider()

        f1, f2, f3 = st.columns([0.45, 0.25, 0.30])
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
        render_video_grid(df_show, ch_df, rpm_long=rpm_long, rpm_shorts=rpm_shorts, viral_rel_threshold=viral_rel_threshold)

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

            subs_map = {safe_str(r["channel_id"]).strip(): int(r["subscribers"]) for _, r in ch_df.iterrows()}
            name_map = {
                safe_str(r["channel_id"]).strip(): (safe_str(r["title"]).strip() or safe_str(r["handle"]).strip() or safe_str(r["channel_id"]).strip())
                for _, r in ch_df.iterrows()
            }

            df_out["subs"] = df_out["channel_id"].astype(str).map(subs_map).fillna(0).astype(int)
            df_out["ratio"] = df_out["view_count"] / df_out["subs"].clip(lower=1)
            df_out = df_out[(df_out["subs"] > 0) & (df_out["ratio"] >= float(ratio))].copy()
            df_out = df_out.sort_values(["ratio", "view_count"], ascending=[False, False])
            df_out["channel_title"] = df_out["channel_id"].astype(str).map(name_map).fillna(df_out["channel_id"].astype(str))

            show = df_out.rename(
                columns={"channel_title": "Kênh", "title": "Video", "view_count": "Views", "subs": "Subscribers", "ratio": "Views/Subs"}
            )[["Kênh", "Video", "Views", "Subscribers", "Views/Subs"]]
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
