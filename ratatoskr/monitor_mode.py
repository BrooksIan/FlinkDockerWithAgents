"""Continuous monitor mode — env helpers for NiFi / Kafka poll agents."""

from __future__ import annotations

import os

MONITOR_MODES = frozenset({"oneshot", "continuous"})
DEFAULT_MONITOR_INTERVAL_SEC = 10.0


def _truthy(name: str, default: str = "") -> bool:
    return (os.environ.get(name) or default).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
        "continuous",
    )


def monitor_mode() -> str:
    """
    ``oneshot`` (default) = finite / single poll.
    ``continuous`` = unbounded host loop or Kafka-tick Flink job.
    """
    raw = (os.environ.get("MONITOR_MODE") or "oneshot").strip().lower()
    if raw in MONITOR_MODES:
        return raw
    if _truthy("MONITOR_CONTINUOUS"):
        return "continuous"
    return "oneshot"


def is_continuous() -> bool:
    return monitor_mode() == "continuous"


def monitor_interval_sec(default: float = DEFAULT_MONITOR_INTERVAL_SEC) -> float:
    raw = (os.environ.get("MONITOR_INTERVAL_SEC") or "").strip()
    if not raw:
        return float(default)
    try:
        val = float(raw)
    except ValueError:
        return float(default)
    return val if val > 0 else float(default)


def nifi_monitor_polls() -> int | None:
    """
    Burst size for cluster NiFi jobs.
    ``None`` means continuous (Kafka tick source).
    ``0`` or MONITOR_MODE=continuous also means continuous.
    """
    if is_continuous():
        return None
    raw = (os.environ.get("NIFI_MONITOR_POLLS") or "5").strip()
    if raw in ("0", "continuous", "forever", "-1"):
        return None
    try:
        n = int(raw)
    except ValueError:
        return 5
    return None if n <= 0 else n


def kafka_monitor_polls() -> int | None:
    if is_continuous():
        return None
    raw = (os.environ.get("KAFKA_MONITOR_POLLS") or "5").strip()
    if raw in ("0", "continuous", "forever", "-1"):
        return None
    try:
        n = int(raw)
    except ValueError:
        return 5
    return None if n <= 0 else n


def nifi_poll_topic() -> str:
    return (os.environ.get("NIFI_MONITOR_POLL_TOPIC") or "nifi.monitor.poll").strip()


def kafka_poll_topic() -> str:
    return (os.environ.get("KAFKA_MONITOR_POLL_TOPIC") or "kafka.monitor.poll").strip()
