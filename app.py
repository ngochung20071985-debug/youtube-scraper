# app.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from supabase import create_client, Client


# =========================
# Config / Secrets
# =========================
APP_TITLE = "ToolWatch • Supabase Dashboard"

# Required in Streamlit Cloud secrets:
# SUPABASE_URL="https://xxxx.supabase.co"
# SUPABASE_SERVICE_ROLE_KEY="xxx"
#
# Optional:
# DEFAULT_VIDEO_LIMIT=120


# =========================
# Utils
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
    Frontend KHÔNG gọi YouTube API => chỉ chấp nhận UC... hoặc URL /channel/UC...
    Nếu bạn muốn nhập @handle / tên kênh => phải update scraper.py để resolve handle.
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
# Schema detection (snapshots table)
# =========================
@st.cache_data(ttl=300, show_spinner=False)
def detect_snapshots_schema() -> Dict[str, bool]:
    """
    Detect whether snapshots table is VIDEO snapshots or CHANNEL snapshots.

    VIDEO snapshot expected columns:
      - video_id
      - captured_at
      - view_count/views
      - like_count/likes
      - comment_count/comments

    CHANNEL snapshot expected columns:
      - channel_id
      - captured_at
      - subscribers
      - total_views
    """
    client = supa()
    try:
        res = client.table("snapshots").select("*").limit(1).execute()
        row = (res.data or [None])[0] or {}
    except Exception:
        row = {}

    keys = set(row.keys())
    is_video = ("video_id" in keys) and (("view_count" in keys) or ("views" in keys) or ("view" in keys))
    is_channel = ("channel_id" in keys) and (("total_views" in keys) or ("subscribers" in keys))
    return {"is_video": bool(is_video), "is_channel": bool(is_channel)}


# =========================
# Data fetch (SELECT)
# =========================
@st.cache_data(ttl=120, show_spinner=False)
def fetch_channels() -> pd.DataFrame:
    client = supa()
    res = (
        client.table("channels")
        .select("channel_id,title,avatar_url,subscribers,total_views,video_count,country,default_language,default_audio_language,last_scanned_at,created_at")
        .order("subscribers", desc=True)
        .execute()
    )
    df = pd.DataFrame(res.data or [])
    if df.empty:
        df = pd.DataFrame(columns=["channel_id", "title", "subscribers"])
    for c in ["subscribers", "total_views", "video_count"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_latest_videos(limit: int = 120) -> pd.DataFrame:
    """
    Lấy videos mới nhất. Nếu bảng videos có views/likes/comments thì dùng làm fallback.
    """
    client = supa()
    res = (
        client.table("videos")
        .select("video_id,channel_id,title,published_at,thumb_url,duration_sec,url,views,likes,comments,updated_at")
        .order("published_at", desc=True)
        .limit(int(limit))
        .execute()
    )
    df = pd.DataFrame(res.data or [])
    if df.empty:
        df = pd.DataFrame(
            columns=["video_id", "channel_id", "title", "published_at", "thumb_url", "duration_sec", "url", "views", "likes", "comments"]
        )
    for c in ["duration_sec", "views", "likes", "comments"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_latest_video_metrics(video_ids: List[str]) -> pd.DataFrame:
    """
    Lấy snapshot mới nhất cho từng video_id (nếu snapshots là video snapshots).
    Fallback: trả DF rỗng nếu snapshots không phải video snapshots.
    """
    schema = detect_snapshots_schema()
    if not schema["is_video"]:
        return pd.DataFrame(columns=["video_id", "captured_at", "views", "likes", "comments"])

    if not video_ids:
        return pd.DataFrame(columns=["video_id", "captured_at", "views", "likes", "comments"])

    client = supa()

    # PostgREST "in" list sẽ dài nếu quá nhiều; limit UI default <= 120 nên ổn.
    # Dùng select order desc rồi group python lấy bản ghi đầu tiên mỗi video.
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
        return pd.DataFrame(columns=["video_id", "captured_at", "views", "likes", "comments"])

    # normalize column names
    def pick(row: Dict[str, Any], keys: List[str]) -> int:
        for k in keys:
            if k in row and row[k] is not None:
                return to_int(row[k])
        return 0

    latest: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        vid = str(r.get("video_id") or "")
        if not vid or vid in latest:
            continue
        latest[vid] = {
            "video_id": vid,
            "captured_at": r.get("captured_at"),
            "views": pick(r, ["view_count", "views", "view"]),
            "likes": pick(r, ["like_count", "likes"]),
            "comments": pick(r, ["comment_count", "comments"]),
        }

    return pd.DataFrame(list(latest.values()))


# =========================
# Mutations (INSERT/DELETE)
# =========================
def add_channel_to_supabase(channel_input: str) -> Tuple[bool, str]:
    cid = extract_channel_id(channel_input)
    if not cid:
        return False, "Bạn phải nhập channel_id dạng UC... hoặc URL /channel/UC... (frontend không resolve @handle)."

    client = supa()
    # upsert để không lỗi nếu đã tồn tại
    payload = {"channel_id": cid, "created_at": utc_now_iso()}
    try:
        client.table("channels").upsert(payload, on_conflict="channel_id").execute()
        # clear caches
        fetch_channels.clear()
        fetch_latest_videos.clear()
        fetch_latest_video_metrics.clear()
        return True, f"Đã thêm kênh: {cid}. (Scraper sẽ tự cập nhật title/sub/views trong lần chạy tới)"
    except Exception as e:
        return False, f"Lỗi thêm kênh: {e}"


def delete_channel_from_supabase(channel_id: str, *, delete_children: bool = True) -> Tuple[bool, str]:
    client = supa()

    try:
        if delete_children:
            # delete videos + snapshots first (nếu không có ON DELETE CASCADE)
            # 1) lấy video_ids theo channel_id
            vres = client.table("videos").select("video_id").eq("channel_id", channel_id).limit(2000).execute()
            vids = [r["video_id"] for r in (vres.data or []) if r.get("video_id")]
            schema = detect_snapshots_schema()

            if schema["is_video"] and vids:
                # delete snapshots where video_id IN vids
                client.table("snapshots").delete().in_("video_id", vids).execute()

            if schema["is_channel"]:
                # snapshots là channel snapshots
                client.table("snapshots").delete().eq("channel_id", channel_id).execute()

            # delete videos
            client.table("videos").delete().eq("channel_id", channel_id).execute()

        # delete channel
        client.table("channels").delete().eq("channel_id", channel_id).execute()

        fetch_channels.clear()
        fetch_latest_videos.clear()
        fetch_latest_video_metrics.clear()
        return True, f"Đã xoá kênh: {channel_id}"
    except Exception as e:
        return False, f"Lỗi xoá kênh: {e}"


# =========================
# UI helpers
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


def render_video_grid(videos: pd.DataFrame, channels: pd.DataFrame, rpm_long: float, rpm_shorts: float, viral_rel_threshold: float):
    if videos.empty:
        st.info("Chưa có video trong bảng videos.")
        return

    ch_map = {}
    if not channels.empty:
        for _, r in channels.iterrows():
            ch_map[str(r.get("channel_id"))] = {
                "title": r.get("title") or str(r.get("channel_id")),
                "subs": int(r.get("subscribers") or 0),
            }

    # Build cards HTML
    parts = ["<div class='tw-grid'>"]
    for _, r in videos.iterrows():
        vid = str(r.get("video_id") or "")
        cid = str(r.get("channel_id") or "")
        title = str(r.get("title") or "")
        url = str(r.get("url") or f"https://www.youtube.com/watch?v={vid}")
        thumb = str(r.get("thumb_url") or "")
        published_at = str(r.get("published_at") or "")
        ago = time_ago_vi(published_at)

        views = int(r.get("views") or 0)
        likes = int(r.get("likes") or 0)
        comments = int(r.get("comments") or 0)

        ch_title = ch_map.get(cid, {}).get("title", cid)
        subs = int(ch_map.get(cid, {}).get("subs", 0))
        rel = (views / max(1, subs)) if subs > 0 else 0.0
        viral = rel >= float(viral_rel_threshold)

        rpm = rpm_shorts if is_shorts(r.get("duration_sec")) else rpm_long
        rev = (views / 1000.0) * float(rpm)

        badge = "<div class='tw-badge'>✅🔥 VIRAL</div>" if viral else ""

        card = f"""
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
        parts.append(card)

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
          <div class="tw-brand">toolwatch • Supabase Dashboard</div>
          <div style="display:flex; gap:10px;">
            <div class="tw-pill">Frontend only</div>
            <div class="tw-pill">No YouTube API</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---- Sidebar: add/delete channels + RPM sliders
    st.sidebar.header("⚙️ Điều khiển")

    if st.sidebar.button("🔄 Refresh dữ liệu", use_container_width=True):
        fetch_channels.clear()
        fetch_latest_videos.clear()
        fetch_latest_video_metrics.clear()
        st.rerun()

    st.sidebar.subheader("➕ Thêm kênh")
    ch_in = st.sidebar.text_input("Nhập Channel ID / URL", placeholder="UCxxxx... hoặc https://youtube.com/channel/UCxxxx")
    if st.sidebar.button("Thêm", use_container_width=True):
        ok, msg = add_channel_to_supabase(ch_in)
        (st.sidebar.success if ok else st.sidebar.error)(msg)

    st.sidebar.divider()
    st.sidebar.subheader("🗑️ Xoá kênh")

    ch_df = fetch_channels()
    ch_options = []
    if not ch_df.empty:
        for _, r in ch_df.iterrows():
            ch_options.append(f"{r.get('title') or r.get('channel_id')}  •  {r.get('channel_id')}")

    pick = st.sidebar.selectbox("Chọn kênh", options=["(chưa có kênh)"] if not ch_options else ch_options)
    delete_children = st.sidebar.toggle("Xoá kèm videos/snapshots", value=True)

    if ch_options and st.sidebar.button("Xoá", use_container_width=True, type="primary"):
        cid = pick.split("•")[-1].strip()
        ok, msg = delete_channel_from_supabase(cid, delete_children=delete_children)
        (st.sidebar.success if ok else st.sidebar.error)(msg)

    st.sidebar.divider()
    st.sidebar.subheader("💵 RPM (ước tính)")
    st.sidebar.caption("Tuỳ chỉnh để ra doanh thu ước tính (không phải số thật).")
    rpm_long = st.sidebar.slider("RPM Long-form ($/1000 views)", 0.1, 30.0, 1.5, 0.1)
    rpm_shorts = st.sidebar.slider("RPM Shorts ($/1000 views)", 0.01, 5.0, 0.2, 0.01)
    viral_rel_threshold = st.sidebar.slider("Ngưỡng viral (Views/Subs ≥)", 1.0, 20.0, 3.0, 0.5)

    # ---- Tabs
    tab1, tab2, tab3 = st.tabs(["📺 Tổng quan Video", "🚀 Outlier Finder", "👥 Kênh Đối thủ"])

    # Preload
    video_limit = int(st.secrets.get("DEFAULT_VIDEO_LIMIT", 120))
    videos_df = fetch_latest_videos(limit=video_limit)

    # Join with latest snapshots (if video snapshots exist)
    metrics_df = fetch_latest_video_metrics(videos_df["video_id"].astype(str).tolist()) if not videos_df.empty else pd.DataFrame()
    if not metrics_df.empty:
        # merge overrides into videos_df
        metrics_df["video_id"] = metrics_df["video_id"].astype(str)
        videos_df["video_id"] = videos_df["video_id"].astype(str)

        videos_df = videos_df.merge(metrics_df[["video_id", "views", "likes", "comments"]], on="video_id", how="left", suffixes=("", "_snap"))
        for col in ["views", "likes", "comments"]:
            if f"{col}_snap" in videos_df.columns:
                videos_df[col] = videos_df[f"{col}_snap"].fillna(videos_df[col]).fillna(0).astype(int)
                videos_df.drop(columns=[f"{col}_snap"], inplace=True)

    # ===== Tab 1: video overview
    with tab1:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Tổng kênh", f"{len(ch_df):,}")
        with c2:
            st.metric("Tổng video (đang hiển thị)", f"{len(videos_df):,}")
        with c3:
            st.metric("Tổng subs", fmt_num(int(ch_df["subscribers"].sum()) if not ch_df.empty else 0))

        st.divider()

        # Filters
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

        if sort_mode == "Nhiều view" and "views" in df_show.columns:
            df_show = df_show.sort_values("views", ascending=False)
        else:
            if "published_at" in df_show.columns:
                df_show["_p"] = pd.to_datetime(df_show["published_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
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

    # ===== Tab 2: Outlier Finder
    with tab2:
        st.caption("Lọc video trong 7 ngày qua có **Views ≥ 3× Subscribers** (dùng views mới nhất từ snapshots nếu có, fallback sang videos.views).")

        days = st.slider("Khoảng ngày", 1, 30, 7)
        ratio = st.slider("Ngưỡng Views/Subs", 1.0, 20.0, 3.0, 0.5)

        df_out = videos_df.copy()
        if df_out.empty:
            st.info("Chưa có video.")
        else:
            # filter date
            df_out["_p"] = pd.to_datetime(df_out["published_at"].astype(str).str.replace("Z", "+00:00"), utc=True, errors="coerce")
            df_out = df_out.dropna(subset=["_p"])
            df_out = df_out[df_out["_p"] >= (datetime.now(timezone.utc) - timedelta(days=int(days)))].copy()

            # join subs
            subs_map = {str(r["channel_id"]): int(r.get("subscribers") or 0) for _, r in ch_df.iterrows()} if not ch_df.empty else {}
            df_out["subs"] = df_out["channel_id"].astype(str).map(subs_map).fillna(0).astype(int)
            df_out["views"] = pd.to_numeric(df_out.get("views", 0), errors="coerce").fillna(0).astype(int)
            df_out["ratio"] = df_out["views"] / df_out["subs"].clip(lower=1)

            df_out = df_out[(df_out["subs"] > 0) & (df_out["ratio"] >= float(ratio))].copy()
            df_out = df_out.sort_values(["ratio", "views"], ascending=[False, False])

            # add channel title + url
            title_map = {str(r["channel_id"]): (r.get("title") or r["channel_id"]) for _, r in ch_df.iterrows()} if not ch_df.empty else {}
            df_out["channel_title"] = df_out["channel_id"].astype(str).map(title_map).fillna(df_out["channel_id"].astype(str))
            df_out["url"] = df_out.get("url", "").fillna("").astype(str)
            if "video_id" in df_out.columns:
                df_out.loc[df_out["url"].eq(""), "url"] = "https://www.youtube.com/watch?v=" + df_out["video_id"].astype(str)

            show = df_out[["channel_title", "title", "views", "subs", "ratio", "published_at", "url"]].rename(
                columns={
                    "channel_title": "Kênh",
                    "title": "Video",
                    "views": "Views",
                    "subs": "Subscribers",
                    "ratio": "Views/Subs",
                    "published_at": "Đăng lúc",
                    "url": "Link",
                }
            )

            st.dataframe(show, use_container_width=True, height=520)

            st.download_button(
                "⬇️ Tải CSV (outliers)",
                data=show.to_csv(index=False).encode("utf-8"),
                file_name="outliers.csv",
                mime="text/csv",
                use_container_width=True,
            )

    # ===== Tab 3: Channels
    with tab3:
        if ch_df.empty:
            st.info("Chưa có kênh trong bảng channels.")
        else:
            st.metric("Tổng subscribers", fmt_num(int(ch_df["subscribers"].sum())))
            st.dataframe(
                ch_df.rename(
                    columns={
                        "channel_id": "Channel ID",
                        "title": "Tên kênh",
                        "subscribers": "Subscribers",
                        "total_views": "Total Views",
                        "video_count": "Số video",
                        "country": "Quốc gia",
                        "last_scanned_at": "Scan gần nhất",
                        "created_at": "Ngày thêm",
                    }
                ),
                use_container_width=True,
                height=560,
            )


if __name__ == "__main__":
    main()
