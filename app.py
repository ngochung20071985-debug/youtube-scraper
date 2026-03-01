# app.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from supabase import create_client, Client


APP_TITLE = "ToolWatch • NexLev-style (Supabase)"
YOUTUBE_WATCH = "https://www.youtube.com/watch?v="


# =========================
# Helpers
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def time_ago_vi(iso: str) -> str:
    iso = (iso or "").strip()
    if not iso:
        return ""
    try:
        if iso.endswith("Z"):
            iso = iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        sec = int((now - dt.astimezone(timezone.utc)).total_seconds())
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
    except Exception:
        return ""


def extract_channel_id(user_input: str) -> Optional[str]:
    """
    Frontend KHÔNG gọi YouTube API => chỉ chấp nhận:
    - UC... (channel_id)
    - URL dạng /channel/UC...
    (Nếu muốn @handle => để scraper resolve và upsert vào DB.)
    """
    s = (user_input or "").strip()
    if not s:
        return None
    if re.fullmatch(r"UC[a-zA-Z0-9_-]{20,}", s):
        return s
    m = re.search(r"/channel/(UC[a-zA-Z0-9_-]{20,})", s)
    if m:
        return m.group(1)
    return None


def ensure_columns(df: pd.DataFrame, defaults: Dict[str, Any]) -> pd.DataFrame:
    """Bảo đảm các cột tồn tại, thiếu thì tạo với default."""
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def coerce_int_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            df[c] = df[c].fillna(0).astype(int)
    return df


def is_shorts(duration_sec: Any) -> bool:
    try:
        return int(duration_sec or 0) <= 60
    except Exception:
        return False


# =========================
# Supabase
# =========================
@st.cache_resource
def supa() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)


# =========================
# SELECT (robust even if empty)
# =========================
@st.cache_data(ttl=120, show_spinner=False)
def fetch_channels() -> pd.DataFrame:
    """
    channels schema (theo bạn):
    id, channel_id, title, handle, avatar_url, subscribers, created_at
    """
    client = supa()
    res = (
        client.table("channels")
        .select("id,channel_id,title,handle,avatar_url,subscribers,created_at")
        .order("subscribers", desc=True)
        .execute()
    )
    df = pd.DataFrame(res.data or [])

    # nếu DB trống -> df rỗng nhưng có cột chuẩn
    if df.empty:
        df = pd.DataFrame(columns=["id", "channel_id", "title", "handle", "avatar_url", "subscribers", "created_at"])

    df = ensure_columns(
        df,
        {
            "id": None,
            "channel_id": "",
            "title": "",
            "handle": "",
            "avatar_url": "",
            "subscribers": 0,
            "created_at": "",
        },
    )
    df = coerce_int_cols(df, ["subscribers"])
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_latest_videos(limit: int = 120) -> pd.DataFrame:
    """
    Videos schema có thể khác nhau tùy bạn → SELECT '*' để không crash vì thiếu cột.
    Sau đó normalize về bộ cột frontend cần.
    """
    client = supa()

    # Thử order theo published_at trước, fail thì fallback created_at, fail nữa thì không order
    res = None
    for col in ("published_at", "created_at", None):
        try:
            q = client.table("videos").select("*").limit(int(limit))
            if col:
                q = q.order(col, desc=True)
            res = q.execute()
            break
        except Exception:
            res = None

    df = pd.DataFrame((res.data if res else []) or [])

    # DB trống
    if df.empty:
        df = pd.DataFrame(
            columns=["video_id", "channel_id", "title", "published_at", "created_at", "thumb_url", "duration_sec", "url"]
        )

    df = ensure_columns(
        df,
        {
            "video_id": "",
            "channel_id": "",
            "title": "",
            "published_at": "",
            "created_at": "",
            "thumb_url": "",
            "duration_sec": 0,
            "url": "",
        },
    )
    df = coerce_int_cols(df, ["duration_sec"])

    # url fallback
    empty_url = df["url"].astype(str).str.strip().eq("") | df["url"].isna()
    df.loc[empty_url, "url"] = YOUTUBE_WATCH + df["video_id"].astype(str)

    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_latest_snapshots_for_videos(video_ids: List[str]) -> pd.DataFrame:
    """
    snapshots (video metrics) giả định có:
    video_id, captured_at, view_count, like_count, comment_count
    Nhưng vẫn code chịu được thiếu cột / DB trống.
    """
    if not video_ids:
        return pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])

    client = supa()
    rows: List[Dict[str, Any]] = []

    # chunk để tránh query URL quá dài
    CHUNK = 120
    for i in range(0, len(video_ids), CHUNK):
        chunk = video_ids[i : i + CHUNK]
        try:
            r = (
                client.table("snapshots")
                .select("*")
                .in_("video_id", chunk)
                .order("captured_at", desc=True)
                .limit(max(500, len(chunk) * 5))
                .execute()
            )
            rows.extend(r.data or [])
        except Exception:
            # nếu snapshots không có video_id (schema khác) => trả empty, không crash
            continue

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])

    # normalize columns (chấp nhận synonyms)
    df = ensure_columns(
        df,
        {
            "video_id": "",
            "captured_at": "",
            "view_count": None,
            "views": None,
            "like_count": None,
            "likes": None,
            "comment_count": None,
            "comments": None,
        },
    )

    # tạo canonical metrics
    # (không dùng df.get(...) để tránh scalar)
    if "view_count" not in df.columns:
        df["view_count"] = 0
    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce")
    df["view_count"] = df["view_count"].fillna(pd.to_numeric(df["views"], errors="coerce")).fillna(0)

    if "like_count" not in df.columns:
        df["like_count"] = 0
    df["like_count"] = pd.to_numeric(df["like_count"], errors="coerce")
    df["like_count"] = df["like_count"].fillna(pd.to_numeric(df["likes"], errors="coerce")).fillna(0)

    if "comment_count" not in df.columns:
        df["comment_count"] = 0
    df["comment_count"] = pd.to_numeric(df["comment_count"], errors="coerce")
    df["comment_count"] = df["comment_count"].fillna(pd.to_numeric(df["comments"], errors="coerce")).fillna(0)

    df["view_count"] = df["view_count"].fillna(0).astype(int)
    df["like_count"] = df["like_count"].fillna(0).astype(int)
    df["comment_count"] = df["comment_count"].fillna(0).astype(int)

    # Lấy bản ghi mới nhất mỗi video_id
    df["captured_at"] = df["captured_at"].astype(str)
    df["video_id"] = df["video_id"].astype(str)

    latest: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        vid = r["video_id"]
        if not vid or vid in latest:
            continue
        latest[vid] = {
            "video_id": vid,
            "captured_at": r["captured_at"],
            "view_count": int(r["view_count"]),
            "like_count": int(r["like_count"]),
            "comment_count": int(r["comment_count"]),
        }

    return pd.DataFrame(list(latest.values()))


# =========================
# Mutations (INSERT / DELETE)
# =========================
def add_channel(channel_input: str) -> Tuple[bool, str]:
    cid = extract_channel_id(channel_input)
    if not cid:
        return False, "Nhập channel_id UC... hoặc URL dạng /channel/UC... (frontend không resolve @handle)."

    client = supa()
    try:
        client.table("channels").upsert({"channel_id": cid}, on_conflict="channel_id").execute()
        fetch_channels.clear()
        fetch_latest_videos.clear()
        fetch_latest_snapshots_for_videos.clear()
        return True, f"✅ Đã thêm kênh: {cid}. (Scraper sẽ cập nhật title/handle/subscribers ở lần chạy tới)"
    except Exception as e:
        return False, f"❌ Lỗi thêm kênh: {e}"


def delete_channel(channel_id: str, delete_children: bool) -> Tuple[bool, str]:
    client = supa()
    try:
        if delete_children:
            # xoá snapshots theo video_id của kênh
            try:
                vres = client.table("videos").select("video_id").eq("channel_id", channel_id).limit(5000).execute()
                vids = [r["video_id"] for r in (vres.data or []) if r.get("video_id")]
            except Exception:
                vids = []

            if vids:
                # snapshots may or may not support video_id; wrap
                try:
                    client.table("snapshots").delete().in_("video_id", vids).execute()
                except Exception:
                    pass

            # xoá videos
            try:
                client.table("videos").delete().eq("channel_id", channel_id).execute()
            except Exception:
                pass

        # xoá channel
        client.table("channels").delete().eq("channel_id", channel_id).execute()

        fetch_channels.clear()
        fetch_latest_videos.clear()
        fetch_latest_snapshots_for_videos.clear()
        return True, f"✅ Đã xoá kênh: {channel_id}"
    except Exception as e:
        return False, f"❌ Lỗi xoá kênh: {e}"


# =========================
# UI / CSS (NexLev-ish)
# =========================
def inject_css():
    st.markdown(
        """
        <style>
          header[data-testid="stHeader"]{ background: transparent !important; }
          div[data-testid="stToolbar"]{ display:none !important; }
          #MainMenu{ visibility:hidden; }
          footer{ visibility:hidden; }

          @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800;900&display=swap');
          html, body, [class*="css"]{
            font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important;
          }
          .stApp{
            background: linear-gradient(180deg, #0b0b0b 0%, #0a0a0a 100%);
            color: rgba(230,232,238,0.92);
          }
          .block-container{
            max-width: 1500px;
            padding-top: 0.8rem;
            padding-bottom: 2rem;
          }

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
        st.info("Chưa có video trong bảng videos.")
        return

    # channel map
    ch_map: Dict[str, Dict[str, Any]] = {}
    if not channels.empty:
        for _, r in channels.iterrows():
            cid = str(r["channel_id"])
            ch_map[cid] = {
                "title": (str(r["title"]).strip() or str(r["handle"]).strip() or cid),
                "subs": int(r["subscribers"]),
            }

    parts = ["<div class='tw-grid'>"]
    for _, r in videos.iterrows():
        vid = str(r["video_id"])
        cid = str(r["channel_id"])
        title = str(r["title"])
        url = str(r["url"])
        thumb = str(r["thumb_url"])
        published_at = str(r["published_at"] or r["created_at"])
        ago = time_ago_vi(published_at)

        views = int(r["view_count"])
        likes = int(r["like_count"])
        comments = int(r["comment_count"])

        ch_title = ch_map.get(cid, {}).get("title", cid)
        subs = int(ch_map.get(cid, {}).get("subs", 0))
        rel = (views / max(1, subs)) if subs > 0 else 0.0
        viral = rel >= float(viral_rel_threshold)

        rpm = rpm_shorts if is_shorts(r["duration_sec"]) else rpm_long
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
                  <div class="tw-sub">{ch_title} • {fmt_num(views)} lượt xem • {ago}</div>
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


# =========================
# Main
# =========================
def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide")
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
        fetch_latest_videos.clear()
        fetch_latest_snapshots_for_videos.clear()
        st.rerun()

    st.sidebar.subheader("➕ Thêm kênh")
    ch_in = st.sidebar.text_input("Channel ID / URL", placeholder="UCxxxx… hoặc https://youtube.com/channel/UCxxxx")
    if st.sidebar.button("Thêm", use_container_width=True):
        ok, msg = add_channel(ch_in)
        (st.sidebar.success if ok else st.sidebar.error)(msg)

    st.sidebar.divider()
    st.sidebar.subheader("🗑️ Xoá kênh")
    ch_df = fetch_channels()

    if ch_df.empty:
        st.sidebar.info("Chưa có kênh.")
        picked = None
    else:
        options = []
        for _, r in ch_df.iterrows():
            label = (str(r["title"]).strip() or str(r["handle"]).strip() or str(r["channel_id"]))
            options.append(f"{label}  •  {r['channel_id']}")
        pick = st.sidebar.selectbox("Chọn kênh", options=options)
        picked = pick.split("•")[-1].strip()

    delete_children = st.sidebar.toggle("Xoá kèm videos/snapshots", value=True)
    if picked and st.sidebar.button("Xoá", use_container_width=True, type="primary"):
        ok, msg = delete_channel(picked, delete_children=delete_children)
        (st.sidebar.success if ok else st.sidebar.error)(msg)

    st.sidebar.divider()
    st.sidebar.subheader("💵 RPM (ước tính)")
    st.sidebar.caption("Chỉ để ước tính doanh thu (không phải số thật).")
    rpm_long = st.sidebar.slider("RPM Long-form ($/1000 views)", 0.1, 30.0, 1.5, 0.1)
    rpm_shorts = st.sidebar.slider("RPM Shorts ($/1000 views)", 0.01, 5.0, 0.2, 0.01)
    viral_rel_threshold = st.sidebar.slider("Ngưỡng viral (Views/Subs ≥)", 1.0, 20.0, 3.0, 0.5)

    # Tabs
    tab1, tab2, tab3 = st.tabs(["📺 Tổng quan Video", "🚀 Outlier Finder", "👥 Kênh Đối thủ"])

    # Load data
    video_limit = int(st.secrets.get("DEFAULT_VIDEO_LIMIT", 120))
    videos_df = fetch_latest_videos(limit=video_limit)

    # Nếu videos rỗng -> tạo bảng chuẩn để UI không crash
    if videos_df.empty:
        videos_df = pd.DataFrame(
            columns=["video_id", "channel_id", "title", "published_at", "created_at", "thumb_url", "duration_sec", "url"]
        )
        videos_df = ensure_columns(
            videos_df,
            {
                "video_id": "",
                "channel_id": "",
                "title": "",
                "published_at": "",
                "created_at": "",
                "thumb_url": "",
                "duration_sec": 0,
                "url": "",
            },
        )

    # Pull latest snapshots for currently loaded videos
    snap_df = pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])
    if not videos_df.empty and "video_id" in videos_df.columns:
        vid_ids = [str(x) for x in videos_df["video_id"].astype(str).tolist() if str(x).strip()]
        snap_df = fetch_latest_snapshots_for_videos(vid_ids)

    # Merge: canonical metrics columns ALWAYS exist for rendering
    videos_df = ensure_columns(
        videos_df,
        {
            "video_id": "",
            "channel_id": "",
            "title": "",
            "published_at": "",
            "created_at": "",
            "thumb_url": "",
            "duration_sec": 0,
            "url": "",
        },
    )

    # create metrics columns default 0 before merge
    videos_df["view_count"] = 0
    videos_df["like_count"] = 0
    videos_df["comment_count"] = 0

    if not snap_df.empty:
        snap_df = ensure_columns(
            snap_df,
            {"video_id": "", "view_count": 0, "like_count": 0, "comment_count": 0, "captured_at": ""},
        )
        snap_df = coerce_int_cols(snap_df, ["view_count", "like_count", "comment_count"])
        videos_df["video_id"] = videos_df["video_id"].astype(str)
        snap_df["video_id"] = snap_df["video_id"].astype(str)

        videos_df = videos_df.merge(
            snap_df[["video_id", "view_count", "like_count", "comment_count"]],
            on="video_id",
            how="left",
            suffixes=("", "_snap"),
        )

        # overwrite with snapshot values if present
        videos_df["view_count"] = pd.to_numeric(videos_df["view_count"], errors="coerce").fillna(0).astype(int)
        videos_df["like_count"] = pd.to_numeric(videos_df["like_count"], errors="coerce").fillna(0).astype(int)
        videos_df["comment_count"] = pd.to_numeric(videos_df["comment_count"], errors="coerce").fillna(0).astype(int)

    videos_df = coerce_int_cols(videos_df, ["duration_sec", "view_count", "like_count", "comment_count"])

    # ========= Tab 1: Overview Videos
    with tab1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Tổng kênh", f"{len(ch_df):,}")
        with c2:
            st.metric("Tổng video (đang hiển thị)", f"{len(videos_df):,}")
        with c3:
            st.metric("Tổng subscribers", fmt_num(int(ch_df["subscribers"].sum()) if not ch_df.empty else 0))

        st.divider()

        f1, f2, f3 = st.columns([0.45, 0.25, 0.30])
        with f1:
            q = st.text_input("Tìm theo tiêu đề", value="", placeholder="Search…")
        with f2:
            sort_mode = st.selectbox("Sắp xếp", ["Mới nhất", "Nhiều view"], index=0)
        with f3:
            show_n = st.selectbox("Hiển thị", [24, 48, 72, 120], index=1)

        df_show = videos_df.copy()
        if q.strip():
            df_show = df_show[df_show["title"].astype(str).str.contains(q.strip(), case=False, na=False)]

        if sort_mode == "Nhiều view":
            df_show = df_show.sort_values("view_count", ascending=False)
        else:
            sort_col = "published_at" if "published_at" in df_show.columns else "created_at"
            dt = pd.to_datetime(df_show[sort_col].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
            df_show["_dt"] = dt
            df_show = df_show.sort_values("_dt", ascending=False).drop(columns=["_dt"], errors="ignore")

        df_show = df_show.head(int(show_n))

        render_video_grid(df_show, ch_df, rpm_long=rpm_long, rpm_shorts=rpm_shorts, viral_rel_threshold=viral_rel_threshold)

        st.divider()
        st.download_button(
            "⬇️ Tải CSV (videos hiển thị)",
            data=df_show.to_csv(index=False).encode("utf-8"),
            file_name="videos_overview.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # ========= Tab 2: Outlier Finder
    with tab2:
        st.caption("Lọc video trong N ngày qua có **Views ≥ 3× Subscribers** (Views lấy từ snapshots mới nhất).")

        days = st.slider("Khoảng ngày", 1, 30, 7)
        ratio = st.slider("Ngưỡng Views/Subs", 1.0, 20.0, 3.0, 0.5)

        if videos_df.empty or ch_df.empty:
            st.info("Chưa đủ dữ liệu (cần channels + videos + snapshots).")
        else:
            df_out = videos_df.copy()

            # filter by time
            sort_col = "published_at" if "published_at" in df_out.columns else "created_at"
            df_out["_p"] = pd.to_datetime(df_out[sort_col].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
            df_out = df_out.dropna(subset=["_p"])
            df_out = df_out[df_out["_p"] >= (datetime.now(timezone.utc) - timedelta(days=int(days)))].copy()

            # join subs
            subs_map = {str(r["channel_id"]): int(r["subscribers"]) for _, r in ch_df.iterrows()}
            name_map = {
                str(r["channel_id"]): (str(r["title"]).strip() or str(r["handle"]).strip() or str(r["channel_id"]))
                for _, r in ch_df.iterrows()
            }

            df_out["subs"] = df_out["channel_id"].astype(str).map(subs_map).fillna(0).astype(int)
            df_out["ratio"] = df_out["view_count"] / df_out["subs"].clip(lower=1)

            df_out = df_out[(df_out["subs"] > 0) & (df_out["ratio"] >= float(ratio))].copy()
            df_out = df_out.sort_values(["ratio", "view_count"], ascending=[False, False])

            df_out["channel_title"] = df_out["channel_id"].astype(str).map(name_map).fillna(df_out["channel_id"].astype(str))

            show = df_out.rename(
                columns={
                    "channel_title": "Kênh",
                    "title": "Video",
                    "view_count": "Views",
                    "subs": "Subscribers",
                    "ratio": "Views/Subs",
                    sort_col: "Đăng lúc",
                    "url": "Link",
                }
            )[["Kênh", "Video", "Views", "Subscribers", "Views/Subs", "Link"]]

            st.dataframe(show, use_container_width=True, height=520)
            st.download_button(
                "⬇️ Tải CSV (outliers)",
                data=show.to_csv(index=False).encode("utf-8"),
                file_name="outliers.csv",
                mime="text/csv",
                use_container_width=True,
            )

    # ========= Tab 3: Channels
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
                    columns={
                        "handle": "Handle",
                        "channel_id": "Channel ID",
                        "subscribers": "Subscribers",
                        "created_at": "Ngày thêm",
                    }
                ),
                use_container_width=True,
                height=560,
            )


if __name__ == "__main__":
    main()
