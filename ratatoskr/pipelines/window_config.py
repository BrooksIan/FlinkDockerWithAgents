"""Window node configuration helpers for Studio pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ratatoskr.pipelines.window_policies import (
    DEFAULT_GAP_MS,
    GAP_POLICY_DEFAULT,
    GAP_POLICY_SESSION_DETECT,
    KNOWN_GAP_POLICIES,
)

WINDOW_TYPE_DYNAMIC_SESSION = "dynamic_session"
EXECUTION_LOGIC = "logic"
EXECUTION_AGENT_BRIDGE = "agent_bridge"

DEFAULT_BRIDGE_TOPIC = "session.window.output"


@dataclass(frozen=True)
class WindowNodeConfig:
    window_type: str
    key_field: str
    gap_policy: str
    gap_ms: int
    time_mode: str
    execution_mode: str
    bridge_topic: str | None


def default_window_config() -> dict[str, Any]:
    return {
        "window_type": WINDOW_TYPE_DYNAMIC_SESSION,
        "key_field": "key",
        "gap_policy": GAP_POLICY_DEFAULT,
        "gap_ms": DEFAULT_GAP_MS,
        "time_mode": "processing",
        "execution_mode": EXECUTION_LOGIC,
    }


def parse_window_config(config: dict[str, Any] | None) -> WindowNodeConfig:
    raw = config or {}
    execution_mode = str(raw.get("execution_mode") or EXECUTION_LOGIC).strip().lower()
    if execution_mode not in (EXECUTION_LOGIC, EXECUTION_AGENT_BRIDGE):
        execution_mode = EXECUTION_LOGIC
    bridge = str(raw.get("bridge_topic") or "").strip() or None
    gap_policy = str(raw.get("gap_policy") or GAP_POLICY_DEFAULT).strip().lower()
    if gap_policy not in KNOWN_GAP_POLICIES:
        gap_policy = GAP_POLICY_DEFAULT
    try:
        gap_ms = max(1, int(raw.get("gap_ms", DEFAULT_GAP_MS)))
    except (TypeError, ValueError):
        gap_ms = DEFAULT_GAP_MS
    return WindowNodeConfig(
        window_type=str(raw.get("window_type") or WINDOW_TYPE_DYNAMIC_SESSION).strip().lower(),
        key_field=str(raw.get("key_field") or "key").strip() or "key",
        gap_policy=gap_policy,
        gap_ms=gap_ms,
        time_mode=str(raw.get("time_mode") or "processing").strip().lower(),
        execution_mode=execution_mode,
        bridge_topic=bridge,
    )


def pipeline_window_node(pipeline) -> Any | None:
    from ratatoskr.pipelines.models import Pipeline

    if not isinstance(pipeline, Pipeline):
        return None
    windows = [n for n in pipeline.nodes if n.kind == "window"]
    return windows[0] if windows else None


def default_bridge_topic(pipeline_id: str, config: dict[str, Any] | None = None) -> str:
    parsed = parse_window_config(config)
    if parsed.bridge_topic:
        return parsed.bridge_topic
    return f"pipeline.{pipeline_id}.sessions"


def agent_suggested_for_window(agent: str, window_config: WindowNodeConfig) -> str | None:
    """Optional hint when an agent is a natural fit for a domain-specific policy."""
    if window_config.gap_policy == GAP_POLICY_SESSION_DETECT and agent != "session_detect":
        return (
            f"Gap policy {GAP_POLICY_SESSION_DETECT!r} is tuned for the session_detect example; "
            f"consider gap_policy {GAP_POLICY_DEFAULT!r} for agent {agent!r}"
        )
    return None
