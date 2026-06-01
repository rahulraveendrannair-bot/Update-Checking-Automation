"""
streamlit_app.py
================
RPS Hash Tracking Dashboard — Streamlit UI

Shows the current state of RPS_Updates.xlsx:
  • Summary metrics
  • Full data table with colour-coded hash status
  • Change history from Audit Log
  • Downloadable change report (changed rows only)

Run locally:
    pip install streamlit openpyxl pandas
    streamlit run streamlit_app.py

On GitHub Actions (auto-deploy to Streamlit Cloud):
    Connect your repo to https://streamlit.io/cloud
"""

import io
import os
from datetime import datetime

import pandas as pd
import streamlit as st
from openpyxl import load_workbook

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RPS Hash Tracker",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Syne', sans-serif;
    }
    .main { background-color: #0f1117; }
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }

    /* Metric cards */
    div[data-testid="metric-container"] {
        background: #1a1d27;
        border: 1px solid #2a2d3e;
        border-radius: 12px;
        padding: 1rem 1.2rem;
    }
    div[data-testid="metric-container"] label {
        color: #8b8fa8 !important;
        font-size: 0.75rem !important;
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2rem !important;
        font-weight: 700;
    }

    /* Header */
    .rps-header {
        background: linear-gradient(135deg, #1a1d27 0%, #0f1117 100%);
        border: 1px solid #2a2d3e;
        border-radius: 16px;
        padding: 2rem 2.5rem;
        margin-bottom: 2rem;
    }
    .rps-title {
        font-family: 'Syne', sans-serif;
        font-weight: 800;
        font-size: 2.2rem;
        color: #ffffff;
        letter-spacing: -0.02em;
        margin: 0;
    }
    .rps-subtitle {
        color: #8b8fa8;
        font-size: 0.9rem;
        margin-top: 0.3rem;
        font-family: 'JetBrains Mono', monospace;
    }

    /* Status badges */
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 600;
        font-family: 'JetBrains Mono', monospace;
        letter-spacing: 0.05em;
    }
    .badge-changed  { background: #fef3c7; color: #92400e; }
    .badge-same     { background: #d1fae5; color: #065f46; }
    .badge-new      { background: #dbeafe; color: #1e40af; }
    .badge-null     { background: #fee2e2; color: #991b1b; }

    /* Section headers */
    .section-title {
        font-family: 'Syne', sans-serif;
        font-weight: 600;
        font-size: 1.1rem;
        color: #e2e4f0;
        letter-spacing: 0.02em;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #2a2d3e;
    }

    /* Dataframe styling */
    div[data-testid="stDataFrame"] {
        border: 1px solid #2a2d3e;
        border-radius: 10px;
        overflow: hidden;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: #1a1d27;
        border-right: 1px solid #2a2d3e;
    }
    section[data-testid="stSidebar"] .stMarkdown p {
        color: #8b8fa8;
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
EXCEL_FILE   = "RPS_Updates.xlsx"
COL_RPS      = "RPS List"
COL_OLD      = "Old Hash Value"
COL_NEW      = "New hash Value"

# ── Helpers ───────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_data(path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load Sheet1 and Audit Log from the Excel file."""
    xl      = pd.ExcelFile(path)
    df_main = pd.read_excel(path, sheet_name="Sheet1")
    df_audit = pd.read_excel(path, sheet_name="Audit Log") if "Audit Log" in xl.sheet_names else pd.DataFrame()
    return df_main, df_audit


def classify_row(row) -> str:
    old = str(row.get(COL_OLD, "") or "").strip()
    new = str(row.get(COL_NEW, "") or "").strip()
    if not new or new == "nan":
        return "null"
    if not old or old == "nan":
        return "new"
    if old != new:
        return "changed"
    return "same"


def status_badge(status: str) -> str:
    labels = {"changed": "CHANGED", "same": "SAME", "new": "FIRST RUN", "null": "NULL"}
    return f'<span class="badge badge-{status}">{labels.get(status, status.upper())}</span>'


def make_report_excel(df_changed: pd.DataFrame) -> bytes:
    """Create an in-memory Excel report of changed rows."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_changed.to_excel(writer, index=False, sheet_name="Changed RPS Lists")
    return buf.getvalue()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🔍 RPS Hash Tracker")
    st.markdown("---")

    uploaded = st.file_uploader(
        "Upload RPS_Updates.xlsx",
        type=["xlsx"],
        help="Upload the latest Excel file from your GitHub Actions artifact"
    )

    st.markdown("---")
    st.markdown("**Filters**")
    filter_status = st.multiselect(
        "Status",
        options=["changed", "same", "new", "null"],
        default=["changed", "same", "new", "null"],
        format_func=lambda x: {"changed": "🟨 Changed", "same": "✅ Same",
                                "new": "🟦 First Run", "null": "🟥 Null"}[x]
    )
    search_term = st.text_input("Search RPS List", placeholder="e.g. FBI, BD, EU…")

    st.markdown("---")
    st.markdown("**About**")
    st.markdown("This dashboard tracks SHA-256 hash changes across 175 RPS sanction/watchlists. Yellow = content changed on the source website.")


# ── Load data ─────────────────────────────────────────────────────────────────
excel_path = None
if uploaded:
    with open("_uploaded_rps.xlsx", "wb") as f:
        f.write(uploaded.read())
    excel_path = "_uploaded_rps.xlsx"
elif os.path.isfile(EXCEL_FILE):
    excel_path = EXCEL_FILE

if not excel_path:
    st.markdown("""
    <div class="rps-header">
        <p class="rps-title">🔍 RPS Hash Tracker</p>
        <p class="rps-subtitle">Upload RPS_Updates.xlsx from the sidebar to get started</p>
    </div>
    """, unsafe_allow_html=True)
    st.info("👈 Upload your `RPS_Updates.xlsx` file using the sidebar to view the dashboard.")
    st.stop()

df_main, df_audit = load_data(excel_path)

# Add status column
df_main["Status"] = df_main.apply(classify_row, axis=1)

# Apply filters
df_filtered = df_main[df_main["Status"].isin(filter_status)]
if search_term:
    df_filtered = df_filtered[
        df_filtered[COL_RPS].str.contains(search_term, case=False, na=False)
    ]

# Counts
total     = len(df_main)
changed   = (df_main["Status"] == "changed").sum()
same      = (df_main["Status"] == "same").sum()
new_      = (df_main["Status"] == "new").sum()
null_     = (df_main["Status"] == "null").sum()
last_run  = df_audit.iloc[-1]["Run Timestamp"] if len(df_audit) > 0 else "—"

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="rps-header">
    <p class="rps-title">🔍 RPS Hash Tracker</p>
    <p class="rps-subtitle">Last run: {last_run} &nbsp;·&nbsp; {total} RPS lists monitored</p>
</div>
""", unsafe_allow_html=True)

# ── Metrics ───────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Lists",   total)
c2.metric("🟨 Changed",    changed,  delta=f"{changed} need review" if changed else None,
          delta_color="inverse" if changed else "off")
c3.metric("✅ Unchanged",   same)
c4.metric("🟦 First Run",   new_)
c5.metric("🟥 Null / Failed", null_,
          delta="scrapers failed" if null_ else None,
          delta_color="inverse" if null_ else "off")

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📋 All RPS Lists", "🟨 Changed Only", "📊 Audit Log"])

# ── Tab 1: All RPS Lists ──────────────────────────────────────────────────────
with tab1:
    st.markdown(f'<p class="section-title">RPS Lists — {len(df_filtered)} shown</p>', unsafe_allow_html=True)

    # Download button
    if changed > 0:
        df_changed_only = df_main[df_main["Status"] == "changed"][[COL_RPS, COL_OLD, COL_NEW]]
        report_bytes = make_report_excel(df_changed_only)
        st.download_button(
            label=f"⬇️ Download Change Report ({changed} changed rows)",
            data=report_bytes,
            file_name=f"change_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # Colour map for Status column
    def colour_status(val):
        colours = {
            "changed": "background-color: #fef3c7; color: #92400e; font-weight: 600;",
            "same":    "background-color: #d1fae5; color: #065f46;",
            "new":     "background-color: #dbeafe; color: #1e40af;",
            "null":    "background-color: #fee2e2; color: #991b1b; font-weight: 600;",
        }
        return colours.get(val, "")

    display_df = df_filtered[[COL_RPS, COL_OLD, COL_NEW, "Status"]].copy()
    display_df[COL_OLD] = display_df[COL_OLD].fillna("—")
    display_df[COL_NEW] = display_df[COL_NEW].fillna("—")

    styled = display_df.style.applymap(colour_status, subset=["Status"])

    st.dataframe(
        styled,
        use_container_width=True,
        height=600,
        column_config={
            COL_RPS:  st.column_config.TextColumn("RPS List",       width=220),
            COL_OLD:  st.column_config.TextColumn("Old Hash Value", width=340),
            COL_NEW:  st.column_config.TextColumn("New Hash Value", width=340),
            "Status": st.column_config.TextColumn("Status",         width=100),
        }
    )

# ── Tab 2: Changed Only ───────────────────────────────────────────────────────
with tab2:
    df_changed_tab = df_main[df_main["Status"] == "changed"]

    if len(df_changed_tab) == 0:
        st.success("✅ No changes detected — all hashes match their previous values.")
    else:
        st.markdown(f'<p class="section-title">🟨 {len(df_changed_tab)} RPS lists with changed content</p>',
                    unsafe_allow_html=True)

        # Download button
        report_bytes = make_report_excel(df_changed_tab[[COL_RPS, COL_OLD, COL_NEW]])
        st.download_button(
            label=f"⬇️ Download Change Report ({len(df_changed_tab)} rows)",
            data=report_bytes,
            file_name=f"change_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_changed_tab"
        )

        st.markdown("<br>", unsafe_allow_html=True)

        # Show as cards
        for _, row in df_changed_tab.iterrows():
            with st.expander(f"🟨  {row[COL_RPS]}", expanded=False):
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown("**Old Hash Value**")
                    st.code(str(row[COL_OLD] or "—"), language=None)
                with col_b:
                    st.markdown("**New Hash Value**")
                    st.code(str(row[COL_NEW] or "—"), language=None)

# ── Tab 3: Audit Log ──────────────────────────────────────────────────────────
with tab3:
    if len(df_audit) == 0:
        st.info("No audit log found.")
    else:
        st.markdown('<p class="section-title">Run History</p>', unsafe_allow_html=True)

        # Reverse so latest is first
        df_audit_display = df_audit.iloc[::-1].reset_index(drop=True)

        # Bar chart of changed counts over time
        if "Changed (Yellow)" in df_audit_display.columns or "Updated" in df_audit_display.columns:
            count_col = "Changed (Yellow)" if "Changed (Yellow)" in df_audit_display.columns else "Updated"
        else:
            count_col = None

        if count_col and count_col in df_audit_display.columns:
            st.markdown("**Changes detected per run**")
            chart_df = df_audit[["Run Timestamp", count_col]].copy()
            chart_df["Run Timestamp"] = chart_df["Run Timestamp"].astype(str)
            chart_df = chart_df.rename(columns={count_col: "Changed"})
            st.bar_chart(chart_df.set_index("Run Timestamp")["Changed"])

        st.markdown("<br>", unsafe_allow_html=True)
        st.dataframe(
            df_audit_display,
            use_container_width=True,
            height=400,
        )

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    '<p style="color:#8b8fa8; font-size:0.75rem; font-family:\'JetBrains Mono\',monospace; text-align:center;">'
    'RPS Hash Tracker · Automated daily via GitHub Actions · '
    f'Dashboard loaded at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
    '</p>',
    unsafe_allow_html=True
)
