# app.py
# Streamlit frontend (Supabase only) — NexLev-style layout
# ✅ NO YouTube API calls here. Only SELECT/INSERT/DELETE via Supabase.
# ✅ Handles empty DB safely (no .get(...).fillna() traps).

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from supabase import create_client, Client


# -----------------------------
# Config / Page
# -----------------------------
APP_TITLE = "toolwatch • NexLev-style (Supabase)"
st.set_page_config(page_title=APP_TITLE, page_icon="📊", layout="wide", initial_sidebar_state="expanded")


# -----------------------------
# Styling (NexLev-ish dark)
# -----------------------------
CSS = """
<style>
:root{
  --bg0:#090a0b;
  --bg1:#0c0d0f;
  --panel:#121417;
  --panel2:#171a1f;
  --stroke:rgba(255,255,255,.08);
  --muted:rgba(255,255,255,.65);
  --muted2:rgba(255,255,255,.50);
  --text:#f4f6f8;
  --accent:#e11d48;     /* red-ish */
  --good:#22c55e;       /* green */
  --blue:#3b82f6;       /* blue */
  --card-radius:16px;
  --shadow: 0 12px 32px rgba(0,0,0,.45);
}

/* App background */
.stApp{
  background:
    radial-gradient(1200px 600px at 10% 10%, rgba(59,130,246,.13), transparent 60%),
    radial-gradient(900px 520px at 90% 10%, rgba(34,197,94,.10), transparent 55%),
    radial-gradient(900px 600px at 20% 90%, rgba(225,29,72,.08), transparent 60%),
    linear-gradient(180deg, var(--bg0), var(--bg1));
  color: var(--text);
}

/* Sidebar */
section[data-testid="stSidebar"]{
  background: linear-gradient(180deg, rgba(255,255,255,.04), rgba(255,255,255,.03));
  border-right: 1px solid var(--stroke);
}
section[data-testid="stSidebar"] *{ color: var(--text) !important; }

/* Hide Streamlit collapse control (prevents "ẩn hẳn" bug) */
button[data-testid="stSidebarCollapseButton"]{ display:none !important; }
div[data-testid="collapsedControl"]{ display:none !important; }

/* Top title pill */
.tw-top{
  display:flex; align-items:center; justify-content:space-between;
  gap: 12px;
  background: rgba(255,255,255,.04);
  border: 1px solid var(--stroke);
  border-radius: 999px;
  padding: 10px 14px;
  box-shadow: var(--shadow);
}
.tw-pill{
  display:inline-flex; align-items:center; gap:8px;
  font-weight:700;
  letter-spacing:.2px;
}
.tw-badges{ display:flex; gap:8px; align-items:center; }
.tw-badge{
  border:1px solid var(--stroke);
  background: rgba(255,255,255,.04);
  padding:6px 10px;
  border-radius: 999px;
  font-size: 12px;
  color: var(--muted);
}

/* KPI strip */
.tw-kpi{
  border-top: 1px solid var(--stroke);
  border-bottom: 1px solid var(--stroke);
  padding: 18px 0;
  margin-top: 14px;
}
.tw-kpi .kpi-label{ color: var(--muted2); font-size: 12px; }
.tw-kpi .kpi-value{ font-size: 34px; font-weight: 800; }

/* Section divider */
.tw-divider{
  border-top: 1px solid var(--stroke);
  margin: 18px 0;
}

/* Cards */
.tw-card{
  border: 1px solid var(--stroke);
  background: rgba(255,255,255,.035);
  border-radius: var(--card-radius);
  box-shadow: var(--shadow);
  overflow: hidden;
}
.tw-card .body{ padding: 12px 12px 10px 12px; }
.tw-title{
  font-weight: 800;
  font-size: 14px;
  line-height: 1.25;
  margin: 0 0 6px 0;
}
.tw-sub{
  color: var(--muted2);
  font-size: 12px;
  margin-bottom: 8px;
}
.tw-metrics{
  display:flex;
  flex-wrap: wrap;
  gap: 6px;
}
.tw-chip{
  display:inline-flex;
  align-items:center;
  gap:6px;
  border: 1px solid var(--stroke);
  background: rgba(0,0,0,.25);
  padding: 5px 8px;
  border-radius: 999px;
  font-size: 12px;
  color: rgba(255,255,255,.80);
}
.tw-chip.good{
  border-color: rgba(34,197,94,.4);
  background: rgba(34,197,94,.12);
}
.tw-chip.hot{
  border-color: rgba(245,158,11,.45);
  background: rgba(245,158,11,.10);
}
.tw-thumb-wrap{ position: relative; }
.tw-badge-top{
  position:absolute; top:10px; left:10px;
  display:inline-flex; align-items:center; gap:8px;
  padding: 6px 10px;
  border-radius: 999px;
  border: 1px solid rgba(34,197,94,.5);
  background: rgba(34,197,94,.16);
  font-weight: 800;
  font-size: 12px;
}
.tw-badge-top span{ color: rgba(255,255,255,.92); }

/* Small metric tiles (Tab2 gap filler) */
.tw-tiles{
  display:grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}
.tw-tile{
  border:1px solid var(--stroke);
  background: rgba(255,255,255,.03);
  border-radius: 12px;
  padding: 10px 12px;
}
.tw-tile .t{ color: var(--muted2); font-size: 12px; }
.tw-tile .v{ font-size: 18px; font-weight: 800; margin-top: 2px; }

/* Tables */
[data-testid="stDataFrame"]{
  border: 1px solid var(--stroke);
  border-radius: 12px;
  overflow:hidden;
}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


# -----------------------------
# Supabase
# -----------------------------
@st.cache_resource(show_spinner=False)
def get_client() -> Client:
    url = st.secrets.get("SUPABASE_URL")
    key = st.secrets.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("Thiếu SUPABASE_URL hoặc SUPABASE_SERVICE_ROLE_KEY trong st.secrets")
    return create_client(url, key)


# -----------------------------
# Helpers
# -----------------------------
def fmt_int(n: Any) -> str:
    try:
        n = int(n)
    except Exception:
        return "0"
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.2f}K"
    return str(n)

def fmt_money(n: Any) -> str:
    try:
        n = float(n)
    except Exception:
        n = 0.0
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 1_000_000:
        return f"{sign}${n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{sign}${n/1_000:.2f}K"
    return f"{sign}${n:.2f}"

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def to_dt(s: Any) -> Optional[datetime]:
    if s is None or s == "":
        return None
    try:
        return pd.to_datetime(s, utc=True).to_pydatetime()
    except Exception:
        return None

def ensure_df(df: Optional[pd.DataFrame], cols: Dict[str, Any]) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame({k: pd.Series(dtype="object") for k in cols.keys()})
    for c, default in cols.items():
        if c not in df.columns:
            df[c] = default
    return df

def yt_thumb(video_id: str) -> str:
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

def yt_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"

def parse_channel_input(s: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (channel_id, handle) based on user input.
    UI/frontend doesn't call YouTube API => must provide UC... to be scannable.
    """
    s = (s or "").strip()
    if not s:
        return None, None

    # Direct UC...
    m = re.search(r"(UC[a-zA-Z0-9_-]{10,})", s)
    if m:
        return m.group(1), None

    # Handle @...
    m = re.search(r"@([A-Za-z0-9_.-]{2,})", s)
    if m:
        return None, "@"+m.group(1)

    return None, None


# -----------------------------
# DB Fetch (cached)
# -----------------------------
@st.cache_data(ttl=30, show_spinner=False)
def fetch_channels() -> pd.DataFrame:
    client = get_client()
    resp = client.table("channels").select("id,channel_id,title,handle,avatar_url,subscribers,created_at").order("created_at", desc=False).execute()
    df = pd.DataFrame(resp.data or [])
    df = ensure_df(df, {
        "id": None,
        "channel_id": "",
        "title": "",
        "handle": "",
        "avatar_url": "",
        "subscribers": 0,
        "created_at": "",
    })
    df["subscribers"] = pd.to_numeric(df["subscribers"], errors="coerce").fillna(0).astype(int)
    return df

@st.cache_data(ttl=30, show_spinner=False)
def fetch_videos(limit: int = 5000) -> pd.DataFrame:
    client = get_client()
    resp = client.table("videos").select(
        "video_id,channel_id,published_at,title,description,tags_json,niche,sentiment"
    ).order("published_at", desc=True).limit(limit).execute()
    df = pd.DataFrame(resp.data or [])
    df = ensure_df(df, {
        "video_id": "",
        "channel_id": "",
        "published_at": None,
        "title": "",
        "description": "",
        "tags_json": "",
        "niche": "",
        "sentiment": "",
    })
    df["published_at"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce")
    return df

@st.cache_data(ttl=30, show_spinner=False)
def fetch_snapshots_recent(limit: int = 50000) -> pd.DataFrame:
    client = get_client()
    resp = client.table("snapshots").select(
        "id,video_id,captured_at,view_count,like_count,comment_count"
    ).order("captured_at", desc=True).limit(limit).execute()
    df = pd.DataFrame(resp.data or [])
    df = ensure_df(df, {
        "id": None,
        "video_id": "",
        "captured_at": None,
        "view_count": 0,
        "like_count": 0,
        "comment_count": 0,
    })
    df["captured_at"] = pd.to_datetime(df["captured_at"], utc=True, errors="coerce")
    for c in ["view_count", "like_count", "comment_count"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df

@st.cache_data(ttl=15, show_spinner=False)
def fetch_scraper_state() -> pd.DataFrame:
    client = get_client()
    try:
        r = client.table("scraper_state").select("*").order("updated_at", desc=True).limit(1).execute()
        df = pd.DataFrame(r.data or [])
        return df
    except Exception:
        return pd.DataFrame([])


def latest_snapshot_per_video(snaps: pd.DataFrame) -> pd.DataFrame:
    snaps = ensure_df(snaps, {
        "video_id": "",
        "captured_at": None,
        "view_count": 0,
        "like_count": 0,
        "comment_count": 0,
    })
    if snaps.empty:
        return snaps[["video_id", "captured_at", "view_count", "like_count", "comment_count"]].copy()
    snaps = snaps.sort_values("captured_at", ascending=False)
    out = snaps.drop_duplicates(subset=["video_id"], keep="first").copy()
    return out[["video_id", "captured_at", "view_count", "like_count", "comment_count"]]


# -----------------------------
# Sidebar (always visible)
# -----------------------------
def sidebar_controls(ch_df: pd.DataFrame):
    with st.sidebar:
        st.markdown("### ⚙️ Điều khiển")
        if st.button("🔄 Refresh dữ liệu", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown("### 🤖 Trạng thái robot")
        st_state = fetch_scraper_state()
        if st_state.empty:
            st.caption("Chưa có scraper_state hoặc chưa chạy lần nào.")
        else:
            row = st_state.iloc[0].to_dict()
            updated = to_dt(row.get("updated_at"))
            status = row.get("status", "unknown")
            msg = str(row.get("message", ""))[:120]
            pct = row.get("pct", None)
            st.caption(f"Lần cập nhật gần nhất: **{updated.strftime('%H:%M %d/%m/%Y') if updated else 'N/A'}**")
            st.caption(f"Trạng thái: **{status}**")
            if msg:
                st.caption(msg)
            # Scan mỗi 1 giờ (hiển thị progress theo chu kỳ)
            if updated:
                elapsed = (now_utc() - updated).total_seconds()
                cycle = 3600.0
                progress = min(0.999, max(0.0, elapsed / cycle))
                st.progress(progress, text=f"Chu kỳ 1 giờ: {int(progress*100)}%")
                if elapsed > 7200:
                    st.warning("Dữ liệu có vẻ đã quá lâu chưa cập nhật (>2 giờ). Kiểm tra GitHub Actions.")
            if pct is not None:
                try:
                    st.progress(min(1.0, max(0.0, float(pct)/100.0)), text=f"Tiến trình job: {pct}%")
                except Exception:
                    pass

        st.divider()

        st.markdown("### ➕ Thêm kênh")
        raw = st.text_input("UC... hoặc URL /channel/UC...", placeholder="UCxxxx... hoặc https://youtube.com/channel/UC...")
        if st.button("Thêm kênh", use_container_width=True):
            cid, handle = parse_channel_input(raw)
            if not cid:
                st.error("Frontend không gọi YouTube API nên **bắt buộc nhập Channel ID bắt đầu bằng UC...**")
            else:
                try:
                    client = get_client()
                    # Insert minimal row. Scraper sẽ cập nhật title/avatar/subscribers sau.
                    client.table("channels").upsert(
                        {"channel_id": cid, "handle": handle or "", "title": "", "avatar_url": "", "subscribers": 0},
                        on_conflict="channel_id"
                    ).execute()
                    st.success(f"Đã thêm kênh: {cid}")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Không thêm được kênh: {e}")

        st.divider()

        st.markdown("### 🗑️ Xóa kênh")
        if ch_df.empty:
            st.caption("Chưa có kênh.")
        else:
            options = [f"{r.get('title') or r.get('handle') or r.get('channel_id')} • {r.get('channel_id')}" for _, r in ch_df.iterrows()]
            pick = st.selectbox("Chọn kênh", options=options)
            del_with_related = st.toggle("Xóa kèm videos/snapshots", value=False)
            if st.button("Xóa kênh", use_container_width=True, type="primary"):
                try:
                    client = get_client()
                    channel_id = pick.split("•")[-1].strip()
                    if del_with_related:
                        vids = client.table("videos").select("video_id").eq("channel_id", channel_id).limit(10000).execute().data or []
                        vid_ids = [v.get("video_id") for v in vids if v.get("video_id")]
                        # delete snapshots by video ids (chunked)
                        for chunk in [vid_ids[i:i+200] for i in range(0, len(vid_ids), 200)]:
                            client.table("snapshots").delete().in_("video_id", chunk).execute()
                        client.table("videos").delete().eq("channel_id", channel_id).execute()
                    client.table("channels").delete().eq("channel_id", channel_id).execute()
                    st.success("Đã xóa.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Xóa thất bại: {e}")

        st.divider()

        st.markdown("### 💵 RPM (ước tính)")
        # Avoid SessionState warning by initializing once.
        if "rpm_long" not in st.session_state:
            st.session_state["rpm_long"] = 1.2
        if "rpm_shorts" not in st.session_state:
            st.session_state["rpm_shorts"] = 0.25
        rpm_long = st.slider("RPM video dài", 0.1, 30.0, float(st.session_state["rpm_long"]), 0.1, key="rpm_long")
        rpm_shorts = st.slider("RPM shorts", 0.01, 10.0, float(st.session_state["rpm_shorts"]), 0.01, key="rpm_shorts")
        st.caption("RPM chỉ là **ước tính**, phụ thuộc quốc gia/ngách/độ dài video.")

        return rpm_long, rpm_shorts


# -----------------------------
# Tab 1 — Outlier Radar (new)
# -----------------------------
def tab_outlier_radar(ch_df: pd.DataFrame, vid_df: pd.DataFrame, snap_latest: pd.DataFrame):
    st.subheader("🏠 Trang chủ (Outlier Radar)")

    if ch_df.empty or vid_df.empty or snap_latest.empty:
        st.info("DB chưa đủ dữ liệu để tính Outlier Radar (cần channels + videos + snapshots).")
        return

    # Merge 3 tables: videos + channels + latest snapshot per video
    merged = vid_df.merge(ch_df[["channel_id", "title", "handle", "avatar_url", "subscribers"]].rename(columns={"title": "channel_title"}), on="channel_id", how="left")
    merged = merged.merge(snap_latest, on="video_id", how="left")

    merged = ensure_df(merged, {
        "published_at": pd.NaT,
        "view_count": 0,
        "subscribers": 0,
        "title": "",
        "channel_title": "",
    })

    merged["published_at"] = pd.to_datetime(merged["published_at"], utc=True, errors="coerce")
    merged["view_count"] = pd.to_numeric(merged["view_count"], errors="coerce").fillna(0).astype(int)
    merged["subscribers"] = pd.to_numeric(merged["subscribers"], errors="coerce").fillna(0).astype(int)

    days30 = now_utc() - timedelta(days=30)
    cond = (
        (merged["published_at"].notna()) &
        (merged["published_at"] >= pd.Timestamp(days30)) &
        (merged["view_count"] > 0) &
        (merged["subscribers"] > 0)
    )
    merged = merged.loc[cond].copy()
    if merged.empty:
        st.info("Không có video nào trong 30 ngày qua đủ điều kiện (view>0, subs>0).")
        return

    merged["viral_score"] = merged["view_count"] / merged["subscribers"].replace({0: math.nan})
    merged = merged[merged["viral_score"] >= 3.0].copy()
    merged = merged.sort_values("viral_score", ascending=False)

    topn = st.slider("Hiển thị Top", 10, 50, 20, 5)
    show = merged.head(topn).copy()

    st.caption("Công thức: Viral_Score = view_count / subscribers (lọc 30 ngày, view>0, Viral_Score ≥ 3.0)")

    cols_per_row = 4
    cols = st.columns(cols_per_row)
    for i, row in enumerate(show.to_dict("records")):
        c = cols[i % cols_per_row]
        with c:
            vid = row["video_id"]
            title = (row.get("title") or "").strip()
            channel_title = (row.get("channel_title") or row.get("handle") or row.get("channel_id") or "").strip()
            subs = int(row.get("subscribers") or 0)
            views = int(row.get("view_count") or 0)
            score = float(row.get("viral_score") or 0.0)

            thumb = yt_thumb(vid)
            url = yt_url(vid)

            st.markdown('<div class="tw-card">', unsafe_allow_html=True)
            st.markdown(f'<div class="tw-thumb-wrap"><img src="{thumb}" style="width:100%; display:block; aspect-ratio:16/9; object-fit:cover;">'
                        f'<div class="tw-badge-top"><span>🔥 {score:.1f}x</span></div></div>', unsafe_allow_html=True)
            safe_title = (title[:68] + "…") if len(title) > 69 else title
            st.markdown('<div class="body">', unsafe_allow_html=True)
            st.markdown(f'<div class="tw-title">{safe_title}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="tw-sub">{channel_title} • {fmt_int(subs)} subs</div>', unsafe_allow_html=True)
            st.markdown('<div class="tw-metrics">', unsafe_allow_html=True)
            st.markdown(f'<span class="tw-chip good">👁️ {fmt_int(views)} views</span>', unsafe_allow_html=True)
            st.markdown(f'<span class="tw-chip">🆔 {vid}</span>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            st.markdown(f'<div style="margin-top:10px;"><a href="{url}" target="_blank" style="color:#93c5fd; text-decoration:none; font-weight:700;">Mở video →</a></div>', unsafe_allow_html=True)
            st.markdown('</div></div>', unsafe_allow_html=True)


# -----------------------------
# Tab 2 — Detailed Channel Analysis (old UI moved here)
# -----------------------------
def tab_channel_detail(ch_df: pd.DataFrame, vid_df: pd.DataFrame, snaps_df: pd.DataFrame, rpm_long: float, rpm_shorts: float):
    st.subheader("📊 Phân tích Kênh chi tiết")

    if ch_df.empty:
        st.info("Chưa có kênh trong bảng channels. Hãy thêm kênh ở Sidebar.")
        return

    # Channel picker (Sidebar selection moved logically here but still in sidebar is fine)
    options = [f"{r.get('title') or r.get('handle') or r.get('channel_id')} • {r.get('channel_id')}" for _, r in ch_df.iterrows()]
    pick = st.selectbox("Chọn kênh để phân tích", options=options)
    channel_id = pick.split("•")[-1].strip()

    ch_row = ch_df[ch_df["channel_id"] == channel_id]
    ch_title = (ch_row["title"].iloc[0] if not ch_row.empty else "") or channel_id
    ch_subs = int(ch_row["subscribers"].iloc[0] if not ch_row.empty else 0)

    # Filter videos
    vch = vid_df[vid_df["channel_id"] == channel_id].copy()
    if vch.empty:
        st.warning("Kênh này chưa có video trong bảng videos (scraper chưa ghi).")
        return

    # Pull snapshots for these video_ids (from snaps_df already fetched recent)
    vids = vch["video_id"].dropna().astype(str).tolist()
    ss = snaps_df[snaps_df["video_id"].isin(vids)].copy()
    if ss.empty:
        st.warning("Chưa có snapshots cho các video của kênh này.")
        return

    # Latest per video
    ss_latest = latest_snapshot_per_video(ss)

    # Merge
    m = vch.merge(ss_latest, on="video_id", how="left")
    m["view_count"] = pd.to_numeric(m["view_count"], errors="coerce").fillna(0).astype(int)
    m["like_count"] = pd.to_numeric(m["like_count"], errors="coerce").fillna(0).astype(int)
    m["comment_count"] = pd.to_numeric(m["comment_count"], errors="coerce").fillna(0).astype(int)

    # 30-day deltas (approx): per video latest - earliest in last 30 days
    cutoff = pd.Timestamp(now_utc() - timedelta(days=30))
    ss30 = ss[ss["captured_at"].notna() & (ss["captured_at"] >= cutoff)].copy()
    delta_views = 0
    delta_likes = 0
    delta_comments = 0
    if not ss30.empty:
        # for each video, earliest and latest within 30 days
        ss30_sorted = ss30.sort_values(["video_id", "captured_at"])
        first = ss30_sorted.groupby("video_id", as_index=False).first()[["video_id", "view_count", "like_count", "comment_count"]]
        last = ss30_sorted.groupby("video_id", as_index=False).last()[["video_id", "view_count", "like_count", "comment_count"]]
        dv = last.merge(first, on="video_id", suffixes=("_last", "_first"), how="inner")
        dv["dv"] = (dv["view_count_last"] - dv["view_count_first"]).clip(lower=0)
        dv["dl"] = (dv["like_count_last"] - dv["like_count_first"]).clip(lower=0)
        dv["dc"] = (dv["comment_count_last"] - dv["comment_count_first"]).clip(lower=0)
        delta_views = int(dv["dv"].sum())
        delta_likes = int(dv["dl"].sum())
        delta_comments = int(dv["dc"].sum())

    total_views_now = int(m["view_count"].sum())
    total_likes_now = int(m["like_count"].sum())
    total_comments_now = int(m["comment_count"].sum())

    # Revenue estimate (simple): delta_views/1000 * rpm_long (we don't truly know shorts split)
    est_revenue_30d = (delta_views / 1000.0) * float(rpm_long)

    # Header
    st.markdown(
        f"""
        <div class="tw-top">
          <div class="tw-pill">📌 {ch_title}</div>
          <div class="tw-badges">
            <div class="tw-badge">subs: <b>{fmt_int(ch_subs)}</b></div>
            <div class="tw-badge">videos(DB): <b>{len(vch)}</b></div>
            <div class="tw-badge">snapshots(DB): <b>{len(ss)}</b></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # KPI Row (like NexLev tiles)
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown('<div class="tw-card"><div class="body">', unsafe_allow_html=True)
        st.markdown(f'<div class="kpi-label">Doanh thu ước tính (30 ngày)</div><div class="kpi-value">{fmt_money(est_revenue_30d)}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="tw-sub">RPM video dài: {rpm_long:.2f}</div>', unsafe_allow_html=True)
        st.markdown('</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="tw-card"><div class="body">', unsafe_allow_html=True)
        st.markdown(f'<div class="kpi-label">Views tăng (30 ngày, trong DB)</div><div class="kpi-value">{fmt_int(delta_views)}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="tw-sub">Tổng views hiện tại (trong DB): {fmt_int(total_views_now)}</div>', unsafe_allow_html=True)
        st.markdown('</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown('<div class="tw-card"><div class="body">', unsafe_allow_html=True)
        st.markdown(f'<div class="kpi-label">RPM (ước tính)</div><div class="kpi-value">{fmt_money(rpm_long)}</div>', unsafe_allow_html=True)
        st.markdown('<div class="tw-sub">Shorts RPM chỉ để tham khảo.</div>', unsafe_allow_html=True)
        st.markdown('</div></div>', unsafe_allow_html=True)

    st.markdown('<div class="tw-divider"></div>', unsafe_allow_html=True)

    # Gap filler tiles (the "empty space" you pointed out)
    st.markdown("#### Thông số nhanh (tính từ DB)")
    st.markdown('<div class="tw-tiles">', unsafe_allow_html=True)
    tiles = [
        ("👁️ Tổng views (DB)", fmt_int(total_views_now)),
        ("👍 Tổng likes (DB)", fmt_int(total_likes_now)),
        ("💬 Tổng comments (DB)", fmt_int(total_comments_now)),
        ("📈 Views tăng 30 ngày", fmt_int(delta_views)),
        ("👍 Likes tăng 30 ngày", fmt_int(delta_likes)),
        ("💬 Comments tăng 30 ngày", fmt_int(delta_comments)),
        ("💵 Doanh thu ước tính 30 ngày", fmt_money(est_revenue_30d)),
        ("🧾 Số video (DB)", str(len(vch))),
    ]
    for t, v in tiles:
        st.markdown(f'<div class="tw-tile"><div class="t">{t}</div><div class="v">{v}</div></div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="tw-divider"></div>', unsafe_allow_html=True)

    # Time series chart: sum views across videos per captured_at
    st.markdown("#### Biểu đồ tăng trưởng (tổng views theo snapshot)")
    ts = ss.copy()
    ts = ts[ts["captured_at"].notna()].copy()
    if ts.empty:
        st.info("Không đủ dữ liệu snapshots để vẽ chart.")
    else:
        ts = ts.groupby("captured_at", as_index=False)["view_count"].sum().sort_values("captured_at")
        st.line_chart(ts.set_index("captured_at")["view_count"])

    st.markdown('<div class="tw-divider"></div>', unsafe_allow_html=True)

    # Video list like NexLev card grid (smaller)
    st.markdown("#### Video (trong DB)")
    q = st.text_input("Tìm theo tiêu đề", value="", placeholder="Search…")
    sort_key = st.selectbox("Sắp xếp", ["Mới nhất", "Nhiều view"], index=0)
    per_page = st.selectbox("Hiển thị", [12, 24, 48], index=2)

    mm = m.copy()
    if q.strip():
        mm = mm[mm["title"].str.contains(q.strip(), case=False, na=False)]
    if sort_key == "Nhiều view":
        mm = mm.sort_values("view_count", ascending=False)
    else:
        mm = mm.sort_values("published_at", ascending=False)

    show = mm.head(int(per_page)).copy()

    if show.empty:
        st.info("Không có video khớp filter.")
        return

    cols = st.columns(4)
    for i, row in enumerate(show.to_dict("records")):
        c = cols[i % 4]
        with c:
            vid = row["video_id"]
            title = (row.get("title") or "").strip()
            views = int(row.get("view_count") or 0)
            likes = int(row.get("like_count") or 0)
            cmts = int(row.get("comment_count") or 0)

            # Viral badge uses channel subs (current)
            score = (views / ch_subs) if ch_subs > 0 else 0.0
            badge = f"🔥 {score:.1f}x" if score >= 3.0 else "✅"

            st.markdown('<div class="tw-card">', unsafe_allow_html=True)
            st.markdown(f'<div class="tw-thumb-wrap"><img src="{yt_thumb(vid)}" style="width:100%; display:block; aspect-ratio:16/9; object-fit:cover;">'
                        f'<div class="tw-badge-top" style="border-color:rgba(34,197,94,.45); background:rgba(34,197,94,.12)"><span>{badge}</span></div></div>', unsafe_allow_html=True)
            safe_title = (title[:60] + "…") if len(title) > 61 else title
            st.markdown('<div class="body">', unsafe_allow_html=True)
            st.markdown(f'<div class="tw-title">{safe_title}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="tw-sub">{fmt_int(views)} views • {fmt_int(likes)} likes • {fmt_int(cmts)} cmts</div>', unsafe_allow_html=True)
            st.markdown(f'<div><a href="{yt_url(vid)}" target="_blank" style="color:#93c5fd; text-decoration:none; font-weight:700;">Mở video →</a></div>', unsafe_allow_html=True)
            st.markdown('</div></div>', unsafe_allow_html=True)


# -----------------------------
# Main
# -----------------------------
def main():
    # Top header
    st.markdown(
        f"""
        <div class="tw-top">
          <div class="tw-pill">toolwatch • NexLev-style</div>
          <div class="tw-badges">
            <div class="tw-badge">Frontend only</div>
            <div class="tw-badge">No YouTube API</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # Load data once
    ch_df = fetch_channels()
    vid_df = fetch_videos(limit=5000)
    snaps_df = fetch_snapshots_recent(limit=50000)
    snap_latest = latest_snapshot_per_video(snaps_df)

    rpm_long, rpm_shorts = sidebar_controls(ch_df)

    # Two big tabs
    tab1, tab2 = st.tabs(["🏠 Trang chủ (Outlier Radar)", "📊 Phân tích Kênh chi tiết"])

    with tab1:
        tab_outlier_radar(ch_df, vid_df, snap_latest)

    with tab2:
        tab_channel_detail(ch_df, vid_df, snaps_df, rpm_long=rpm_long, rpm_shorts=rpm_shorts)


if __name__ == "__main__":
    main()
