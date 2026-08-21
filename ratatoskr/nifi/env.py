"""NiFi env helpers — API base, heal gates, monitor thresholds."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Pattern

HEAL_PHASES = frozenset({"monitor", "safe", "lab"})

_PHASE_RANK = {"monitor": 0, "safe": 1, "lab": 2}


def default_nifi_api_base() -> str:
    """
    Host: https://localhost:8443/nifi-api
    Inside Flink image (/opt/flink): https://nifi:8443/nifi-api (compose service DNS)
    Override anytime with NIFI_API_BASE.
    """
    explicit = (os.environ.get("NIFI_API_BASE") or "").strip()
    if explicit:
        return explicit
    if Path("/opt/flink").is_dir():
        return "https://nifi:8443/nifi-api"
    return "https://localhost:8443/nifi-api"


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


def heal_phase() -> str:
    raw = (os.environ.get("NIFI_HEAL_PHASE") or "monitor").strip().lower()
    return raw if raw in HEAL_PHASES else "monitor"


def phase_at_least(active: str, minimum: str) -> bool:
    return _PHASE_RANK.get(active, 0) >= _PHASE_RANK.get(minimum, 0)


def allow_empty_queue() -> bool:
    return _truthy("NIFI_HEAL_ALLOW_EMPTY_QUEUE")


def heal_dry_run() -> bool:
    return _truthy("NIFI_HEAL_DRY_RUN")


def heal_verify() -> bool:
    """Re-poll after heal (default on). Set NIFI_HEAL_VERIFY=0 to skip."""
    raw = (os.environ.get("NIFI_HEAL_VERIFY") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def heal_cooldown_sec() -> float:
    return max(0.0, _float_env("NIFI_HEAL_COOLDOWN_SEC", 0.0))


def heal_max_mutations() -> int:
    """0 = unlimited."""
    return max(0, _int_env("NIFI_HEAL_MAX_MUTATIONS", 0))


def backpressure_warn_threshold() -> int:
    """flowFilesQueued >= warn → BACKPRESSURE_WARN (default 1)."""
    return max(1, _int_env("NIFI_BP_WARN", 1))


def backpressure_crit_threshold() -> int:
    """flowFilesQueued >= crit → BACKPRESSURE_CRIT (default 100)."""
    warn = backpressure_warn_threshold()
    return max(warn, _int_env("NIFI_BP_CRIT", 100))


def empty_queue_min_flowfiles() -> int:
    """Minimum queued flowfiles before empty_connection_queue is allowed (default 1)."""
    return max(1, _int_env("NIFI_EMPTY_QUEUE_MIN_FLOWFILES", 1))


def probe_slow_ms() -> float:
    """Poll slower than this → NIFI_SLOW (default 5000ms)."""
    return max(1.0, _float_env("NIFI_PROBE_SLOW_MS", 5000.0))


def watch_name_regex() -> Pattern[str] | None:
    raw = (os.environ.get("NIFI_WATCH_NAME_REGEX") or "").strip()
    if not raw:
        return None
    try:
        return re.compile(raw)
    except re.error:
        return None


def watch_id_regex() -> Pattern[str] | None:
    raw = (os.environ.get("NIFI_WATCH_ID_REGEX") or "").strip()
    if not raw:
        return None
    try:
        return re.compile(raw)
    except re.error:
        return None


def heal_allow_ids() -> frozenset[str] | None:
    """Comma-separated ids; None means allow all."""
    raw = (os.environ.get("NIFI_HEAL_ALLOW_IDS") or "").strip()
    if not raw:
        return None
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def heal_allow_name_regex() -> Pattern[str] | None:
    raw = (os.environ.get("NIFI_HEAL_ALLOW_NAME_REGEX") or "").strip()
    if not raw:
        return None
    try:
        return re.compile(raw)
    except re.error:
        return None
