"""Cloudera Manager monitor env helpers."""

from __future__ import annotations

import json
import os
from typing import Any

DEFAULT_CM_API_BASE = "https://localhost:7183"


def _truthy(name: str, default: str = "") -> bool:
    return (os.environ.get(name) or default).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _int_env(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def cm_api_base() -> str:
    raw = (
        os.environ.get("CM_API_BASE")
        or os.environ.get("CLOUDERA_MANAGER_API_BASE")
        or DEFAULT_CM_API_BASE
    ).strip().rstrip("/")
    return raw or DEFAULT_CM_API_BASE


def cm_api_version() -> str:
    """API version string (e.g. v49) or ``auto`` for discovery."""
    raw = (os.environ.get("CM_API_VERSION") or "auto").strip()
    return raw or "auto"


def cm_user() -> str:
    return (os.environ.get("CM_USER") or os.environ.get("CM_USERNAME") or "").strip()


def cm_password() -> str:
    return (os.environ.get("CM_PASSWORD") or "").strip()


def cm_cluster() -> str:
    return (os.environ.get("CM_CLUSTER") or "").strip()


def cm_verify_ssl() -> bool:
    raw = (os.environ.get("CM_VERIFY_SSL") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def cm_request_timeout_sec() -> float:
    return max(1.0, _float_env("CM_REQUEST_TIMEOUT_SEC", 30.0))


def cm_probe_slow_ms() -> float:
    return max(1.0, _float_env("CM_PROBE_SLOW_MS", 5000.0))


def cm_event_lookback_sec() -> int:
    return max(0, _int_env("CM_EVENT_LOOKBACK_SEC", 300))


def cm_metric_thresholds() -> dict[str, float]:
    raw = (os.environ.get("CM_METRIC_THRESHOLDS") or "").strip()
    if not raw:
        return {"hdfs_capacity_pct": 85.0, "kafka_under_replicated_min": 1.0}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"hdfs_capacity_pct": 85.0, "kafka_under_replicated_min": 1.0}
    if not isinstance(parsed, dict):
        return {"hdfs_capacity_pct": 85.0, "kafka_under_replicated_min": 1.0}
    out: dict[str, float] = {}
    for key, value in parsed.items():
        try:
            out[str(key)] = float(value)
        except (TypeError, ValueError):
            continue
    return out or {"hdfs_capacity_pct": 85.0, "kafka_under_replicated_min": 1.0}


def knox_token() -> str:
    return (os.environ.get("KNOX_TOKEN") or os.environ.get("CM_KNOX_TOKEN") or "").strip()


def cm_auth_mode() -> str:
    """``knox`` when a Knox JWT is configured; otherwise ``basic``."""
    if knox_token():
        return "knox"
    raw = (os.environ.get("CM_AUTH_MODE") or "basic").strip().lower()
    return raw if raw in ("knox", "basic") else "basic"


def cm_knox_proxied(base_url: str | None = None) -> bool:
    """True when CM API base is a Knox ``cm-api`` proxy path."""
    url = (base_url or cm_api_base()).lower()
    return "/cm-api" in url or cm_auth_mode() == "knox"


def cm_console_base() -> str:
    """CM web UI base (for recommendation deep links)."""
    raw = (os.environ.get("CM_CONSOLE_BASE") or "").strip().rstrip("/")
    if raw:
        return raw
    api = cm_api_base()
    if "/cdp-proxy-token/cm-api" in api:
        return api.replace("/cdp-proxy-token/cm-api", "/cdp-proxy/cmf")
    return api


def cm_event_suppress_patterns_raw() -> str:
    """Comma-separated regexes; ``none`` disables built-in suppressions."""
    return (os.environ.get("CM_EVENT_SUPPRESS_PATTERNS") or "").strip()
