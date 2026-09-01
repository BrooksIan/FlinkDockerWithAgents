"""ReAct agent — structured CM debugging runbook (no mutations)."""

from __future__ import annotations

from typing import Any

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext

_INPUT_EVENT = InputEvent.EVENT_TYPE


def _payload_from_input(event: Event) -> dict[str, Any]:
    payload = InputEvent.from_event(event).input
    if isinstance(payload, dict):
        if isinstance(payload.get("value"), dict):
            return payload["value"]
        return payload
    return {}


class CMRunbookAgent(Agent):
    """
    Consumes ``workflow_cm_monitor`` OutputEvents (or equivalent JSON).

    Deterministic fallback runbook today; never mutates Cloudera Manager.
    """

    @tool
    @staticmethod
    def build_runbook(monitor: dict[str, Any]) -> dict[str, Any]:
        """Build a structured CM debugging runbook from a monitor payload."""
        from examples.agents.react_cm_runbook_logic import build_runbook

        return build_runbook(monitor)

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from examples.agents.react_cm_runbook_logic import build_runbook

        payload = _payload_from_input(event)
        result = build_runbook(payload)
        ctx.send_event(OutputEvent(output=result))
