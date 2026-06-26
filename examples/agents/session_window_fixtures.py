"""Synthetic Cowrie-like events for session window demos."""

from __future__ import annotations

import json
import time
from typing import Any

DEMO_TOPIC = "session.window.input"
DEMO_OUTPUT_TOPIC = "session.window.output"


def _failed_login(src_ip: str, *, ts: int, session: str) -> dict[str, Any]:
    return {
        "eventid": "cowrie.login.failed",
        "src_ip": src_ip,
        "session": session,
        "timestamp": ts,
        "username": "root",
    }


def _command(src_ip: str, *, ts: int, session: str, command: str = "uname -a") -> dict[str, Any]:
    return {
        "eventid": "cowrie.command.input",
        "src_ip": src_ip,
        "session": session,
        "timestamp": ts,
        "input": command,
    }


def demo_session_events() -> list[dict[str, Any]]:
    """Two attackers: brute-force burst (critical) and light probing (low)."""
    base = int(time.time())
    ip_brute = "10.0.0.42"
    ip_probe = "10.0.0.99"
    events: list[dict[str, Any]] = []
    for i in range(5):
        events.append(_failed_login(ip_brute, ts=base + i, session="sess-brute"))
    events.append(_command(ip_probe, ts=base + 10, session="sess-probe"))
    return events


def demo_session_summaries() -> list[dict[str, Any]]:
    """Pre-windowed batches for local agent runs."""
    from examples.agents.session_window_policy import summarize_session

    events = demo_session_events()
    by_ip: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        ip = str(event["src_ip"])
        by_ip.setdefault(ip, []).append(event)
    return [summarize_session(ip, rows) for ip, rows in sorted(by_ip.items())]


def publish_demo_events(
    topic: str = DEMO_TOPIC,
    *,
    bootstrap: str | None = None,
) -> int:
    from ratatoskr.kafka_sources import publish_topic_records

    records = [{"key": e["src_ip"], "value": e} for e in demo_session_events()]
    publish_topic_records(topic, records, bootstrap=bootstrap)
    return len(records)


def parse_kafka_line(raw: str) -> dict[str, Any] | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    if "src_ip" not in payload:
        return None
    return payload
