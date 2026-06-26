"""Module-level actions for Flink Agents YAML (session_detect)."""

from __future__ import annotations

from typing import Any

from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext

from examples.agents.session_detect_logic import block_ip, process_session_summary
from examples.agents.session_window_policy import classify_session


def classify_session_tool(summary: dict[str, Any]) -> str:
    return classify_session(summary)


def _session_from_event(event: Event) -> dict[str, Any]:
    payload = InputEvent.from_event(event).input
    if isinstance(payload, dict):
        return payload
    return {}


def process_session(event: Event, ctx: RunnerContext) -> None:
    summary = _session_from_event(event)
    ctx.send_event(OutputEvent(output=process_session_summary(summary)))
