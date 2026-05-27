# dashboard/app.py
import os
import json
import time
from collections import deque
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import altair as alt
from kafka import KafkaConsumer

# ---------------- Config ----------------
BOOTSTRAP       = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = os.environ.get("KAFKA_TOPIC", "ml.pred.alert.eta")
GROUP_ID        = os.environ.get("KAFKA_GROUP_ID", "streamlit-eta")
BUFFER_HOURS    = int(os.environ.get("BUFFER_HOURS", "24"))
PLOT_MAX_ROWS   = int(os.environ.get("PLOT_MAX_ROWS", "2000"))

DEFAULT_CONF_THRESH = float(os.environ.get("TYPE_CONF_THRESHOLD", "0.6"))
DEFAULT_HORIZON_MIN = int(os.environ.get("HORIZON_MIN", "60"))
DEFAULT_REFRESH_SEC = int(os.environ.get("REFRESH_SEC", "10"))

# Statuses that are NOT actionable — hide from dashboard
HIDE_STATUSES = {"no work", "run", "no_work"}  # include sanitized variant for safety

TYPE_COLORS = {
    "alarm":    "#e74c3c",   # red
    "fullwork": "#f39c12",   # amber/orange
    "m/c stop": "#8e44ad",   # purple
    "uncertain":"#bdc3c7",   # light gray
}


# ---------------- Helpers ----------------
def parse_ts(ts_str: str) -> pd.Timestamp:
    return pd.to_datetime(ts_str, utc=True, errors="coerce")


def get_local_tz():
    return datetime.now().astimezone().tzinfo


def to_display_tz(ts: pd.Series, use_local: bool) -> pd.Series:
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize("UTC")
    return ts.dt.tz_convert(get_local_tz() if use_local else "UTC")


# def make_consumer(bootstrap, topic, group_id, offset_mode: str):
#     return KafkaConsumer(
#         topic,
#         bootstrap_servers=bootstrap,
#         group_id=group_id,
#         enable_auto_commit=True,
#         auto_offset_reset=offset_mode,
#         value_deserializer=lambda v: json.loads(v.decode("utf-8")),
#         key_deserializer=lambda k: (k.decode("utf-8") if k is not None else None),
#         consumer_timeout_ms=1000,
#     )

def make_consumer(bootstrap, topic, group_id, offset_mode: str):
    import sys
    sys.setrecursionlimit(10000)  # safety net
    
    c = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=None,                    # no consumer group = no offset commits
        enable_auto_commit=False,         # disable commits entirely
        auto_offset_reset=offset_mode,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: (k.decode("utf-8") if k is not None else None),
        consumer_timeout_ms=1000,
    )
    return c


# @st.cache_resource(show_spinner=False)
# def get_consumer(bootstrap, topic, group_id, offset_mode):
#     return make_consumer(bootstrap, topic, group_id, offset_mode)

@st.cache_resource(show_spinner=False)
def get_consumer(bootstrap, topic, _group_id, offset_mode):
    return make_consumer(bootstrap, topic, _group_id, offset_mode)


def is_actionable(next_type) -> bool:
    """Return True if this prediction should be shown to operators."""
    if next_type is None:
        return True   # type hidden by confidence → still show ETA with "uncertain"
    return str(next_type).strip().lower() not in HIDE_STATUSES


def ingest_from_consumer(consumer, buf_deque, seen_ids, max_age_hours=24):
    msgs = consumer.poll(timeout_ms=300, max_records=1000)
    added = 0
    for _tp, records in msgs.items():
        for rec in records:
            try:
                val = rec.value
                pred_id = val.get("pred_id")
                if pred_id and pred_id in seen_ids:
                    continue

                p50_ts = val.get("eta_p50_ts")
                p90_ts = val.get("eta_p90_ts")
                mc_no  = val.get("mc_no")
                nxt    = val.get("next_type")
                conf   = val.get("type_conf")
                now_ts = val.get("now_ts")
                mc_status = val.get("mc_status")   # raw status from Flink

                if not (p50_ts and p90_ts and mc_no):
                    continue

                # ── Filter non-actionable predictions ──
                # If next_type is explicitly "no work" or "run", skip entirely
                if nxt is not None and not is_actionable(nxt):
                    continue

                row = {
                    "pred_id":   pred_id or f"{mc_no}-{rec.offset}",
                    "mc_no":     mc_no,
                    "eta_p50_ts": p50_ts,
                    "eta_p90_ts": p90_ts,
                    "next_type": nxt,
                    "type_conf": conf,
                    "now_ts":    now_ts,
                    "mc_status": mc_status,
                }
                buf_deque.append(row)
                if pred_id:
                    seen_ids.add(pred_id)
                added += 1
            except Exception:
                continue

    # Prune old entries
    cutoff = pd.Timestamp.now(tz=timezone.utc) - pd.Timedelta(hours=max_age_hours)
    while buf_deque and parse_ts(buf_deque[0]["eta_p50_ts"]) < cutoff:
        oldest = buf_deque.popleft()
        seen_ids.discard(oldest.get("pred_id"))

    return added


def color_badge(text: str, color_hex: str) -> str:
    return f"""
    <span style="
        background-color:{color_hex};
        color:white;
        padding:3px 8px;
        border-radius:12px;
        font-size:0.85rem;
        font-weight:600;
        ">
        {text.upper()}
    </span>
    """


def format_time(ts: pd.Timestamp) -> str:
    if ts is None or pd.isna(ts):
        return "—"
    now = pd.Timestamp.now(tz=ts.tz)
    if ts.date() == now.date():
        return ts.strftime("%H:%M:%S")
    return ts.strftime("%Y-%m-%d %H:%M:%S")


# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="Upcoming Machine Alerts", page_icon="⏱️", layout="wide")

st.title("⏱️ Upcoming Machine Alerts (Live)")
st.caption(
    "Predicts when an actionable alert will likely occur (P50) "
    "and a safe upper bound (P90). "
    "Non-actionable statuses (no work, run) are hidden."
)

with st.sidebar:
    st.subheader("Settings")
    st.text(f"Kafka: {BOOTSTRAP}")
    st.text(f"Topic: {TOPIC}")

    start_mode = st.radio(
        "Start from",
        ["latest (live)", "earliest (replay)"],
        index=1,
        help="For shadow mode, choose 'earliest' to read old predictions.",
    )
    offset_mode = "latest" if start_mode.startswith("latest") else "earliest"

    horizon_min = st.radio(
        "Horizon (minutes)",
        [30, 60, 120],
        index={30: 0, 60: 1, 120: 2}.get(DEFAULT_HORIZON_MIN, 1),
    )
    conf_thresh = st.slider(
        "Show type if confidence ≥", 0.0, 1.0, DEFAULT_CONF_THRESH, 0.05
    )
    tz_choice = st.radio("Time zone", ["Local", "UTC"], index=0)
    refresh_sec = st.radio(
        "Auto-refresh (sec)",
        [5, 10, 15],
        index={5: 0, 10: 1, 15: 2}.get(DEFAULT_REFRESH_SEC, 1),
    )

    reset_reader = st.button("Reset and read from start (earliest)")
    show_hist = st.checkbox(
        "Show historical predictions (ignore horizon)",
        value=(offset_mode == "earliest"),
    )

# ── Session state init ──
if "buf" not in st.session_state:
    st.session_state.buf = deque(maxlen=100000)
if "seen" not in st.session_state:
    st.session_state.seen = set()
if "gid_suffix" not in st.session_state:
    st.session_state.gid_suffix = ""

if reset_reader and offset_mode == "earliest":
    st.session_state.gid_suffix = f"-{int(time.time())}"
    st.session_state.buf.clear()
    st.session_state.seen.clear()
    if "consumer" in st.session_state:
        del st.session_state["consumer"]

gid = GROUP_ID + (st.session_state.gid_suffix if offset_mode == "earliest" else "")

if (
    "consumer" not in st.session_state
    or st.session_state.get("offset_mode") != offset_mode
    or st.session_state.get("gid") != gid
):
    st.session_state.consumer = get_consumer(BOOTSTRAP, TOPIC, gid, offset_mode)
    st.session_state.offset_mode = offset_mode
    st.session_state.gid = gid
    st.session_state.buf.clear()
    st.session_state.seen.clear()

# ── Ingest ──
_ = ingest_from_consumer(
    st.session_state.consumer,
    st.session_state.buf,
    st.session_state.seen,
    max_age_hours=BUFFER_HOURS,
)

# ── Build DataFrame ──
if st.session_state.buf:
    df = pd.DataFrame(list(st.session_state.buf))

    df["p50_utc"] = pd.to_datetime(df["eta_p50_ts"], utc=True, errors="coerce")
    df["p90_utc"] = pd.to_datetime(df["eta_p90_ts"], utc=True, errors="coerce")

    use_local = tz_choice == "Local"
    df["p50_disp"] = to_display_tz(df["p50_utc"], use_local)
    df["p90_disp"] = to_display_tz(df["p90_utc"], use_local)

    # Time window
    try:
        tzinfo = df["p50_disp"].dt.tz
    except Exception:
        tzinfo = timezone.utc
    now_disp = pd.Timestamp.now(tz=tzinfo)
    horizon = now_disp + pd.Timedelta(minutes=int(horizon_min))

    if show_hist:
        df_live = df.copy()
    else:
        mask = (df["p50_disp"] >= now_disp) & (df["p50_disp"] <= horizon)
        df_live = df.loc[mask].copy()

    # Machine filter
    machines = sorted(df_live["mc_no"].dropna().unique().tolist())
    selected = st.multiselect(
        "Filter machines", options=machines, default=machines,
        help="Select machines to display",
    )
    if selected:
        df_live = df_live[df_live["mc_no"].isin(selected)]

    # ── Display type resolution ──
    def display_type(row):
        t = row.get("next_type")
        c = row.get("type_conf")
        # next_type is None → service hid it (conf < threshold)
        if t is None or (pd.notna(c) and c < conf_thresh):
            return ("UNCERTAIN", TYPE_COLORS["uncertain"], None)
        return (t, TYPE_COLORS.get(t, TYPE_COLORS["uncertain"]), c)

    # ── Soonest alerts ──
    st.markdown("### ⚡ Soonest Actionable Alerts")

    if df_live.empty:
        st.info(
            "No upcoming actionable alerts within the selected horizon. "
            "Try increasing the horizon or wait for new predictions."
        )
    else:
        res = df_live.apply(display_type, axis=1)
        disp_df = pd.DataFrame(
            res.tolist(), index=df_live.index,
            columns=["type_disp", "type_color", "conf_val"],
        )
        df_live = pd.concat([df_live, disp_df], axis=1)

        # Deduplicate near-identical
        df_live["p50_bucket"] = df_live["p50_disp"].dt.floor("1min")
        df_live_dedup = (
            df_live.sort_values("p50_disp")
            .drop_duplicates(subset=["mc_no", "p50_bucket", "type_disp"], keep="first")
        )

        soonest = df_live_dedup.sort_values("p50_disp").head(10).copy()
        if soonest.empty:
            st.info("No upcoming actionable alerts in the selected horizon.")
        else:
            for _, row in soonest.iterrows():
                col1, col2, col3, col4, col5 = st.columns([1.2, 1.2, 2, 2, 1.2])
                with col1:
                    st.markdown(f"**{row['mc_no']}**")
                with col2:
                    badge_html = color_badge(row["type_disp"], row["type_color"])
                    st.markdown(badge_html, unsafe_allow_html=True)
                with col3:
                    st.markdown(f"around **{format_time(row['p50_disp'])}**")
                with col4:
                    if pd.notna(row["p90_disp"]):
                        st.caption(f"safe by {format_time(row['p90_disp'])} (P90)")
                    else:
                        st.caption("")
                with col5:
                    st.caption(
                        f"conf {row['conf_val']:.2f}"
                        if pd.notna(row["conf_val"]) else "conf —"
                    )

        # ── Earliest per machine ──
        st.markdown("### 📋 Earliest per Machine (within horizon)")
        per_mc_earliest = (
            df_live.sort_values("p50_disp")
            .groupby("mc_no", as_index=False)
            .first()
            .sort_values("p50_disp")
        )
        show_df = per_mc_earliest[
            ["mc_no", "type_disp", "p50_disp", "p90_disp", "conf_val"]
        ].copy()
        show_df.rename(
            columns={
                "mc_no":     "Machine",
                "type_disp": "Likely type",
                "p50_disp":  "Around (P50)",
                "p90_disp":  "Up to (P90)",
                "conf_val":  "Conf",
            },
            inplace=True,
        )
        show_df["Around (P50)"] = show_df["Around (P50)"].apply(format_time)
        show_df["Up to (P90)"]  = show_df["Up to (P90)"].apply(format_time)
        show_df["Conf"] = show_df["Conf"].apply(
            lambda x: f"{x:.2f}" if pd.notna(x) else "—"
        )
        st.dataframe(show_df, use_container_width=True, hide_index=True)

                # ── Timeline chart ──
        with st.expander("📊 Timeline (next alerts by machine)"):
            plot_df = df_live_dedup[["mc_no", "p50_disp", "type_disp"]].copy()
            # Clean: drop NaN, convert to native Python types
            plot_df = plot_df.dropna(subset=["mc_no", "p50_disp", "type_disp"])
            plot_df = plot_df.sort_values("p50_disp").tail(PLOT_MAX_ROWS)
            # Force string type (prevents Altair schema recursion on NA/numpy types)
            plot_df["type_disp"] = plot_df["type_disp"].astype(str)
            plot_df["mc_no"] = plot_df["mc_no"].astype(str)

            if not plot_df.empty:
                try:
                    chart = (
                        alt.Chart(plot_df)
                        .mark_circle(size=80, opacity=0.85)
                        .encode(
                            x=alt.X("p50_disp:T", title="Around (P50)"),
                            y=alt.Y("mc_no:N", title="Machine", sort=None),
                            color=alt.Color("type_disp:N", title="Type"),
                            tooltip=[
                                alt.Tooltip("mc_no", title="Machine"),
                                alt.Tooltip("type_disp", title="Type"),
                                alt.Tooltip("p50_disp:T", title="Around (P50)"),
                            ],
                        )
                        .properties(height=400)
                    )
                    st.altair_chart(chart, use_container_width=True)
                except Exception as e:
                    st.warning(f"Chart rendering error: {e}")
                    st.dataframe(
                        plot_df[["mc_no", "p50_disp", "type_disp"]].tail(20),
                        use_container_width=True,
                        hide_index=True,
                    )
            else:
                st.caption("No points to plot.")
else:
    st.info("Waiting for predictions… (no messages ingested yet)")

# ── Legend ──
st.markdown("---")
st.markdown("#### What do P50 and P90 mean?")
st.write(
    "- **P50 (best estimate):** The most likely time the alert will happen. "
    "About half of alerts happen before this time, half after.\n"
    "- **P90 (upper bound):** A safe time by which the alert will almost certainly "
    "have happened (9 out of 10 alerts happen before this time).\n"
    "- A wider P90 band means higher uncertainty "
    "(e.g., off-hours or irregular behavior).\n"
    "- We show the alert type only when confidence is high; "
    "otherwise, we say 'UNCERTAIN'.\n"
    "- Non-actionable predictions (no work, run) are automatically hidden."
)

# ── Metrics sidebar ──
with st.sidebar:
    st.markdown("---")
    st.subheader("Buffer stats")
    st.text(f"Buffered: {len(st.session_state.buf)} predictions")
    st.text(f"Unique IDs: {len(st.session_state.seen)}")

# ── Auto-refresh ──
st.caption(f"Auto-refresh every {refresh_sec}s")
time.sleep(refresh_sec)
st.rerun()