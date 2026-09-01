"""CM runbook logic (no flink_agents — testable; explain-only, no mutations)."""

from __future__ import annotations

from typing import Any

from ratatoskr.cm.runbook.fallback import fallback_runbook


def _normalize_monitor_event(payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("value"), dict):
        payload = payload["value"]
    if payload.get("agent") == "workflow_cm_monitor" or "classification" in payload:
        return payload
    for key in ("monitor", "cm", "cm_event", "event"):
        inner = payload.get(key)
        if isinstance(inner, dict):
            return _normalize_monitor_event(inner)
    return payload


def build_runbook(payload: dict[str, Any]) -> dict[str, Any]:
    """Build a runbook OutputEvent from a CM monitor payload (fallback-only for now)."""
    event = _normalize_monitor_event(payload)
    out = fallback_runbook(event)
    out["agent"] = "react_cm_runbook"
    out["mutations"] = []
    return out
