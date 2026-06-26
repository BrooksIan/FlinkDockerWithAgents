"""Workflow agent — classify and optionally block on closed session windows."""

from __future__ import annotations

from typing import Any

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext

from examples.agents.session_detect_logic import block_ip as block_ip_action
from examples.agents.session_detect_logic import process_session_summary
from examples.agents.session_window_policy import classify_session

_INPUT_EVENT = InputEvent.EVENT_TYPE


def _session_from_event(event: Event) -> dict[str, Any]:
    payload = InputEvent.from_event(event).input
    if isinstance(payload, dict):
        return payload
    return {}


class SessionDetectAgent(Agent):
    """Classify a closed src_ip session and block on high/critical severity."""

    @tool
    @staticmethod
    def classify_session(summary: dict[str, Any]) -> str:
        """Return low / medium / high / critical from aggregated session events."""
        return classify_session(summary)

    @tool
    @staticmethod
    def block_ip(ip: str) -> dict[str, Any]:
        """Demo block action — records intent without calling external systems."""
        return block_ip_action(ip)

    @action(_INPUT_EVENT)
    @staticmethod
    def process_session(event: Event, ctx: RunnerContext) -> None:
        summary = _session_from_event(event)
        ctx.send_event(OutputEvent(output=process_session_summary(summary)))
