"""ReAct agent — explain correlated incidents (no mutations)."""

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


class IncidentScribeAgent(Agent):
    """
    Demo-friendly incident scribe.

    Consumes ``workflow_signal_correlate`` OutputEvents (or equivalent JSON).
    Uses Designer LLM when configured; otherwise deterministic fallback.
    Never mutates NiFi or Kafka (``mutations`` always empty).
    """

    @tool
    @staticmethod
    def scribe(correlation: dict[str, Any]) -> dict[str, Any]:
        """Write an operator brief from a correlation payload."""
        from examples.agents.react_incident_scribe_logic import scribe_incident

        return scribe_incident(correlation)

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from examples.agents.react_incident_scribe_logic import scribe_incident

        payload = _payload_from_input(event)
        # Allow {correlation: {...}} or the correlation event itself
        result = scribe_incident(payload)
        ctx.send_event(OutputEvent(output=result))
