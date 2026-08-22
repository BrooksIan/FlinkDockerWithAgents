"""ReAct agent — structured NiFi↔Kafka cross-signal runbook (no mutations)."""

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


class CrossRunbookAgent(Agent):
    """
    Consumes ``workflow_signal_correlate`` OutputEvents.

    Uses Designer / Cloudera LLM when configured; otherwise deterministic fallback.
    Never mutates NiFi or Kafka (``mutations`` always empty).
    """

    @tool
    @staticmethod
    def build_cross_runbook(correlation: dict[str, Any]) -> dict[str, Any]:
        """Build a cross-stack debugging runbook from a correlation payload."""
        from examples.agents.react_cross_runbook_logic import build_cross_runbook

        return build_cross_runbook(correlation)

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from examples.agents.react_cross_runbook_logic import build_cross_runbook

        result = build_cross_runbook(_payload_from_input(event))
        ctx.send_event(OutputEvent(output=result))
