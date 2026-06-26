"""Session detect logic (no flink_agents import — runnable on host)."""

from __future__ import annotations

from typing import Any

from examples.agents.session_window_policy import classify_session, should_block


def block_ip(ip: str) -> dict[str, Any]:
    return {"ip": ip, "blocked": True, "mode": "demo"}


def process_session_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Classify a closed session and attach demo block actions when warranted."""
    src_ip = str(summary.get("src_ip") or summary.get("key") or "unknown")
    severity = classify_session(summary)
    response_actions: list[dict[str, Any]] = []
    if should_block(severity):
        response_actions.append(block_ip(src_ip))
    return {
        "src_ip": src_ip,
        "severity": severity,
        "event_count": summary.get("event_count", 0),
        "first_ts": summary.get("first_ts", 0),
        "last_ts": summary.get("last_ts", 0),
        "response_actions": response_actions,
        "agent": "session_detect",
    }
