# app.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from supabase import create_client, Client


APP_TITLE = "ToolWatch • Supabase Dashboard"
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
    Frontend không gọi YouTube API => chỉ chấp nhận:
    - channel_id dạng UC...
    - URL /channel/UC...
    (Nếu muốn nhập @handle => phải để scraper resolve và upsert lại.)
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


def is_shorts(duration_sec: Any) -> bool:
    try:
        return int(duration_sec or 0) <= 60
    except Exception:
        return False


# =========================
# Supabase client
# =========================
@st.cache_resource
def supa() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)


# =========================
# Detect snapshots schema
# =========================
@st.cache_data(ttl=300, show_spinner=False)
def detect_snapshots_schema() -> Dict[str, bool]:
    """
    Detect whether snapshots is VIDEO snapshots or CHANNEL snapshots.

    VIDEO snapshots typically have: video_id, captured_at, view_count, like_count, comment_count
    CHANNEL snapshots typically have: channel_id, captured_at, subscribers
    """
    client = supa()
    try:
        res = client.table("snapshots").select("*").limit(1).execute()
        row = (res.data or [None])[0] or {}
    except Exception:
        row = {}
    keys = set(row.keys())

    is_video = ("video_id" in keys) and (("view_count" in keys) or ("views" in keys))
    is_channel = ("channel_id" in keys) and ("subscribers" in keys)
    return {"is_video": bool(is_video), "is_channel": bool(is_channel)}


# =========================
# SELECTs
# =========================
@st.cache_data(ttl=120, show_spinner=False)
def fetch_channels() -> pd.DataFrame:
    """
    ✅ FIXED theo schema bạn đưa:
    channels(id, channel_id, title, handle, avatar_url, subscribers, created_at)
    """
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
    df["subscribers"] = pd.to_numeric(df.get("subscribers", 0), errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_latest_videos(limit: int = 120) -> pd.DataFrame:
    """
    Frontend only: chỉ SELECT từ videos.
    Để tránh lỗi schema, select('*') và fallback order nếu thiếu published_at.
    """
    client = supa()
    try:
        res = (
            client.table("videos")
            .select("*")
            .order("published_at", desc=True)
            .limit(int(limit))
            .execute()
        )
    except Exception:
        # fallback nếu videos không có published_at
        res = (
            client.table("videos")
            .select("*")
            .order("created_at", desc=True)
            .limit(int(limit))
            .execute()
        )

    df = pd.DataFrame(res.data or [])
    if df.empty:
        df = pd.DataFrame(columns=["video_id", "channel_id", "title", "published_at", "thumb_url", "duration_sec", "url"])
    for c in ["duration_sec", "views", "likes", "comments", "view_count", "like_count", "comment_count"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # normalize url
    if "url" not in df.columns:
        df["url"] = ""
    if "video_id" in df.columns:
        df.loc[df["url"].isna() | (df["url"].astype(str).str.strip() == ""), "url"] = YOUTUBE_WATCH + df["video_id"].astype(str)

    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_latest_video_metrics(video_ids: List[str]) -> pd.DataFrame:
    """
    Lấy snapshot mới nhất cho từng video_id (nếu snapshots là video snapshots).
    Dùng view_count/like_count/comment_count.
    """
    schema = detect_snapshots_schema()
    if not schema["is_video"] or not video_ids:
        return pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])

    client = supa()
    res = (
        client.table("snapshots")
        .select("*")
        .in_("video_id", video_ids)
        .order("captured_at", desc=True)
        .limit(max(500, len(video_ids) * 5))
        .execute()
    )
    rows = res.data or []
    if not rows:
        return pd.DataFrame(columns=["video_id", "captured_at", "view_count", "like_count", "comment_count"])

    latest: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        vid = str(r.get("video_id") or "")
        if not vid or vid in latest:
            continue
        latest[vid] = {
            "video_id": vid,
            "captured_at": r.get("captured_at"),
            "view_count": to_int(r.get("view_count") or r.get("views") or 0),
            "like_count": to_int(r.get("like_count") or r.get("likes") or 0),
            "comment_count": to_int(r.get("comment_count") or r.get("comments") or 0),
        }
    return pd.DataFrame(list(latest.values()))


# =========================
# Mutations (INSERT/DELETE)
# =========================
def add_channel_to_supabase(channel_input: str) -> Tuple[bool, str]:
    cid = extract_channel_id(channel_input)
    if not cid:
        return False, "Nhập channel_id dạng UC... hoặc URL /channel/UC... (frontend không resolve @handle)."

    client = supa()
    payload = {"channel_id": cid}  # created_at có default thì không cần set
    try:
        client.table("channels").upsert(payload, on_conflict="channel_id").execute()
        fetch_channels.clear()
        fetch_latest_videos.clear()
        fetch_latest_video_metrics.clear()
        return True, f"Đã thêm kênh: {cid}. (Scraper sẽ tự cập nhật title/handle/subs trong lần chạy tới)"
    except Exception as e:
        return False, f"Lỗi thêm kênh: {e}"


def delete_channel_from_supabase(channel_id: str, *, delete_children: bool = True) -> Tuple[bool, str]:
    client = supa()
    try:
        if delete_children:
            # delete snapshots(video) -> delete videos -> delete channel
            try:
                vres = client.table("videos").select("video_id").eq("channel_id", channel_id).limit(5000).execute()
                vids = [r["video_id"] for r in (vres.data or []) if r.get("video_id")]
            except Exception:
                vids = []

            schema = detect_snapshots_schema()
            if schema["is_video"] and vids:
                client.table("snapshots").delete().in_("video_id", vids).execute()

            if schema["is_channel"]:
                client.table("snapshots").delete().eq("channel_id", channel_id).execute()

            client.table("videos").delete().eq("channel_id", channel_id).execute()

        client.table("channels").delete().eq("channel_id", channel_id).execute()

        fetch_channels.clear()
        fetch_latest_videos.clear()
        fetch_latest_video_metrics.clear()
        return True, f"Đã xoá kênh: {channel_id}"
    except Exception as e:
        return False, f"Lỗi xoá kênh: {e}"


# =========================
# UI
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
            border: 1px solid rgba(34,197,94,0.35);
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

    # map channel info
    ch_map: Dict[str, Dict[str, Any]] = {}
    if not channels.empty:
        for _, r in channels.iterrows():
            cid = str(r.get("channel_id") or "")
            ch_map[cid] = {
                "title": (r.get("title") or r.get("handle") or cid),
                "subs": int(r.get("subscribers") or 0),
            }

    parts = ["<div class='tw-grid'>"]
    for _, r in videos.iterrows():
        vid = str(r.get("video_id") or "")
        cid = str(r.get("channel_id") or "")
        title = str(r.get("title") or "")
        url = str(r.get("url") or (YOUTUBE_WATCH + vid))
        thumb = str(r.get("thumb_url") or "")

        published_at = str(r.get("published_at") or r.get("created_at") or "")
        ago = time_ago_vi(published_at)

        # prefer snapshot metrics if already merged
        views = int(r.get("views") or r.get("view_count") or 0)
        likes = int(r.get("likes") or r.get("like_count") or 0)
        comments = int(r.get("comments") or r.get("comment_count") or 0)

        ch_title = ch_map.get(cid, {}).get("title", cid)
        subs = int(ch_map.get(cid, {}).get("subs", 0))
        rel = (views / max(1, subs)) if subs > 0 else 0.0
        viral = rel >= float(viral_rel_threshold)

        rpm = rpm_shorts if is_shorts(r.get("duration_sec")) else rpm_long
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


def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide")
    inject_css()

    st.markdown(
        """
        <div class="tw-top">
          <div class="tw-brand">toolwatch • Supabase Dashboard</div>
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
        fetch_latest_video_metrics.clear()
        detect_snapshots_schema.clear()
        st.rerun()

    st.sidebar.subheader("➕ Thêm kênh")
    ch_in = st.sidebar.text_input(
        "Nhập Channel ID / URL",
        placeholder="UCxxxx... hoặc https://youtube.com/channel/UCxxxx",
    )
    if st.sidebar.button("Thêm", use_container_width=True):
        ok, msg = add_channel_to_supabase(ch_in)
        (st.sidebar.success if ok else st.sidebar.error)(msg)

    st.sidebar.divider()
    st.sidebar.subheader("🗑️ Xoá kênh")
    ch_df = fetch_channels()

    ch_options: List[str] = []
    if not ch_df.empty:
        for _, r in ch_df.iterrows():
            label = (r.get("title") or r.get("handle") or r.get("channel_id"))
            ch_options.append(f"{label}  •  {r.get('channel_id')}")

    pick = st.sidebar.selectbox("Chọn kênh", options=["(chưa có kênh)"] if not ch_options else ch_options)
    delete_children = st.sidebar.toggle("Xoá kèm videos/snapshots", value=True)

    if ch_options and st.sidebar.button("Xoá", use_container_width=True, type="primary"):
        cid = pick.split("•")[-1].strip()
        ok, msg = delete_channel_from_supabase(cid, delete_children=delete_children)
        (st.sidebar.success if ok else st.sidebar.error)(msg)

    st.sidebar.divider()
    st.sidebar.subheader("💵 RPM (ước tính)")
    st.sidebar.caption("Chỉ để ước tính doanh thu (không phải số thật).")
    rpm_long = st.sidebar.slider("RPM Long-form ($/1000 views)", 0.1, 30.0, 1.5, 0.1)
    rpm_shorts = st.sidebar.slider("RPM Shorts ($/1000 views)", 0.01, 5.0, 0.2, 0.01)
    viral_rel_threshold = st.sidebar.slider("Ngưỡng viral (Views/Subs ≥)", 1.0, 20.0, 3.0, 0.5)

    # Tabs
    tab1, tab2, tab3 = st.tabs(["📺 Tổng quan Video", "🚀 Outlier Finder", "👥 Kênh Đối thủ"])

    # preload videos
    video_limit = int(st.secrets.get("DEFAULT_VIDEO_LIMIT", 120))
    videos_df = fetch_latest_videos(limit=video_limit)

    # join snapshots metrics if snapshots is video snapshots
    if not videos_df.empty and "video_id" in videos_df.columns:
        video_ids = videos_df["video_id"].astype(str).tolist()
        metrics_df = fetch_latest_video_metrics(video_ids)
    else:
        metrics_df = pd.DataFrame()

    if not metrics_df.empty:
        videos_df["video_id"] = videos_df["video_id"].astype(str)
        metrics_df["video_id"] = metrics_df["video_id"].astype(str)
        videos_df = videos_df.merge(metrics_df, on="video_id", how="left")

        # normalize to views/likes/comments (prefer snapshot)
        videos_df["views"] = pd.to_numeric(videos_df.get("view_count", 0), errors="coerce").fillna(0).astype(int)
        videos_df["likes"] = pd.to_numeric(videos_df.get("like_count", 0), errors="coerce").fillna(0).astype(int)
        videos_df["comments"] = pd.to_numeric(videos_df.get("comment_count", 0), errors="coerce").fillna(0).astype(int)
    else:
        # fallback to any columns existing in videos table
        if "views" not in videos_df.columns:
            videos_df["views"] = pd.to_numeric(videos_df.get("view_count", 0), errors="coerce").fillna(0).astype(int)
        if "likes" not in videos_df.columns:
            videos_df["likes"] = pd.to_numeric(videos_df.get("like_count", 0), errors="coerce").fillna(0).astype(int)
        if "comments" not in videos_df.columns:
            videos_df["comments"] = pd.to_numeric(videos_df.get("comment_count", 0), errors="coerce").fillna(0).astype(int)

    # Tab 1
    with tab1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Tổng kênh", f"{len(ch_df):,}")
        with c2:
            st.metric("Tổng video (đang đọc)", f"{len(videos_df):,}")
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
        if q.strip() and "title" in df_show.columns:
            df_show = df_show[df_show["title"].astype(str).str.contains(q.strip(), case=False, na=False)]

        if sort_mode == "Nhiều view":
            df_show = df_show.sort_values("views", ascending=False)
        else:
            # sort by published_at if exists, else created_at
            sort_col = "published_at" if "published_at" in df_show.columns else ("created_at" if "created_at" in df_show.columns else None)
            if sort_col:
                df_show["_p"] = pd.to_datetime(df_show[sort_col].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
                df_show = df_show.sort_values("_p", ascending=False).drop(columns=["_p"], errors="ignore")

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

    # Tab 2: Outlier Finder
    with tab2:
        st.caption("Lọc video trong N ngày qua có **Views ≥ 3× Subscribers** (Views lấy từ snapshots nếu có).")

        days = st.slider("Khoảng ngày", 1, 30, 7)
        ratio = st.slider("Ngưỡng Views/Subs", 1.0, 20.0, 3.0, 0.5)

        if videos_df.empty:
            st.info("Chưa có video.")
        else:
            df_out = videos_df.copy()

            sort_col = "published_at" if "published_at" in df_out.columns else ("created_at" if "created_at" in df_out.columns else None)
            if sort_col:
                df_out["_p"] = pd.to_datetime(df_out[sort_col].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
                df_out = df_out.dropna(subset=["_p"])
                df_out = df_out[df_out["_p"] >= (datetime.now(timezone.utc) - timedelta(days=int(days)))].copy()
            else:
                # nếu thiếu cả thời gian, vẫn cho chạy nhưng không lọc ngày
                df_out["_p"] = pd.NaT

            subs_map = {str(r["channel_id"]): int(r.get("subscribers") or 0) for _, r in ch_df.iterrows()} if not ch_df.empty else {}
            name_map = {str(r["channel_id"]): (r.get("title") or r.get("handle") or r["channel_id"]) for _, r in ch_df.iterrows()} if not ch_df.empty else {}

            df_out["subs"] = df_out.get("channel_id", "").astype(str).map(subs_map).fillna(0).astype(int)
            df_out["ratio"] = df_out["views"] / df_out["subs"].clip(lower=1)
            df_out = df_out[(df_out["subs"] > 0) & (df_out["ratio"] >= float(ratio))].copy()
            df_out = df_out.sort_values(["ratio", "views"], ascending=[False, False])

            df_out["channel_title"] = df_out.get("channel_id", "").astype(str).map(name_map).fillna(df_out.get("channel_id", "").astype(str))
            if "url" not in df_out.columns:
                df_out["url"] = ""
            if "video_id" in df_out.columns:
                df_out.loc[df_out["url"].isna() | (df_out["url"].astype(str).str.strip() == ""), "url"] = YOUTUBE_WATCH + df_out["video_id"].astype(str)

            show = df_out.rename(
                columns={
                    "channel_title": "Kênh",
                    "title": "Video",
                    "views": "Views",
                    "subs": "Subscribers",
                    "ratio": "Views/Subs",
                    sort_col: "Đăng lúc" if sort_col else "Đăng lúc",
                    "url": "Link",
                }
            )

            cols = ["Kênh", "Video", "Views", "Subscribers", "Views/Subs", "Link"]
            if sort_col:
                cols.insert(5, "Đăng lúc")

            st.dataframe(show[cols], use_container_width=True, height=520)

            st.download_button(
                "⬇️ Tải CSV (outliers)",
                data=show[cols].to_csv(index=False).encode("utf-8"),
                file_name="outliers.csv",
                mime="text/csv",
                use_container_width=True,
            )

    # Tab 3: Channels list
    with tab3:
        if ch_df.empty:
            st.info("Chưa có kênh trong bảng channels.")
        else:
            st.metric("Tổng subscribers", fmt_num(int(ch_df["subscribers"].sum())))
            show = ch_df.copy()
            show["Tên hiển thị"] = show["title"].fillna("").astype(str).str.strip()
            show.loc[show["Tên hiển thị"] == "", "Tên hiển thị"] = show["handle"].fillna("").astype(str).replace("", pd.NA)
            show["Tên hiển thị"] = show["Tên hiển thị"].fillna(show["channel_id"])

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
