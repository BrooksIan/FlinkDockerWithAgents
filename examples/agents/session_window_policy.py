"""Deterministic session gap and classification policy for dynamic windowing demos."""

from __future__ import annotations

from typing import Any

# Milliseconds — short for lab demos so processing-time sessions close quickly.
GAP_MS_FAILED_LOGIN = 500
GAP_MS_COMMAND = 1_000
GAP_MS_DEFAULT = 800

SEVERITY_LOW = "low"
SEVERITY_MEDIUM = "medium"
SEVERITY_HIGH = "high"
SEVERITY_CRITICAL = "critical"

_BLOCK_SEVERITIES = {SEVERITY_HIGH, SEVERITY_CRITICAL}


def event_field(event: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in event and event[name] not in (None, ""):
            return event[name]
    value = event.get("value")
    if isinstance(value, dict):
        for name in names:
            if name in value and value[name] not in (None, ""):
                return value[name]
    return None


def src_ip_from_event(event: dict[str, Any]) -> str:
    ip = event_field(event, "src_ip", "source_ip")
    return str(ip or "unknown")


def session_gap_ms(event: dict[str, Any]) -> int:
    """Per-event inactivity gap for DynamicProcessingTimeSessionWindows."""
    eventid = str(event_field(event, "eventid", "event_type") or "").lower()
    if "login.failed" in eventid:
        return GAP_MS_FAILED_LOGIN
    if "command" in eventid:
        return GAP_MS_COMMAND
    return GAP_MS_DEFAULT


def summarize_session(key: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the session batch record consumed by SessionDetectAgent."""
    timestamps: list[int] = []
    for event in events:
        ts = event_field(event, "timestamp", "time")
        if ts is not None:
            try:
                timestamps.append(int(float(ts)))
            except (TypeError, ValueError):
                pass

    first_ts = min(timestamps) if timestamps else 0
    last_ts = max(timestamps) if timestamps else 0
    return {
        "key": key,
        "src_ip": key,
        "events": events,
        "event_count": len(events),
        "first_ts": first_ts,
        "last_ts": last_ts,
    }


def classify_session(summary: dict[str, Any]) -> str:
    """Rule-based session severity from aggregated Cowrie-like events."""
    events = summary.get("events") or []
    failed_logins = 0
    commands = 0
    downloads = 0

    for event in events:
        eventid = str(event_field(event, "eventid", "event_type") or "").lower()
        if "login.failed" in eventid:
            failed_logins += 1
        elif "command" in eventid:
            commands += 1
        elif "file_download" in eventid or "download" in eventid:
            downloads += 1

    if failed_logins >= 5:
        return SEVERITY_CRITICAL
    if failed_logins >= 3 or downloads >= 1:
        return SEVERITY_HIGH
    if failed_logins >= 1 or commands >= 2:
        return SEVERITY_MEDIUM
    return SEVERITY_LOW


def should_block(severity: str) -> bool:
    return severity in _BLOCK_SEVERITIES
