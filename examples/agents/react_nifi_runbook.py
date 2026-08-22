"""ReAct agent — structured NiFi debugging runbook (no mutations)."""

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


class NiFiRunbookAgent(Agent):
    """
    Consumes ``workflow_nifi_monitor`` OutputEvents (or equivalent JSON).

    Uses Cloudera / Designer LLM when configured; otherwise deterministic fallback.
    Never mutates NiFi (``mutations`` always empty).
    """

    @tool
    @staticmethod
    def build_runbook(monitor: dict[str, Any]) -> dict[str, Any]:
        """Build a structured NiFi debugging runbook from a monitor payload."""
        from examples.agents.react_nifi_runbook_logic import build_runbook

        return build_runbook(monitor)

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from examples.agents.react_nifi_runbook_logic import build_runbook

        payload = _payload_from_input(event)
        result = build_runbook(payload)
        ctx.send_event(OutputEvent(output=result))
