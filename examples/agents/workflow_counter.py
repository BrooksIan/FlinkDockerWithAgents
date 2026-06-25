"""Deterministic workflow agent — doubles numeric inputs via a @tool."""

from __future__ import annotations

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext

# Flink Agents 0.3+: actions listen on event type strings, not event classes.
_INPUT_EVENT = InputEvent.EVENT_TYPE


def _int_from_input(event: Event) -> int:
    payload = InputEvent.from_event(event).input
    if isinstance(payload, dict):
        raw = payload.get("value", 0)
    else:
        raw = getattr(payload, "value", payload)
    return int(raw)


class CounterAgent(Agent):
    """Workflow agent that doubles integer values from input events."""

    @tool
    @staticmethod
    def double(value: int) -> int:
        """Return twice the input value."""
        return value * 2

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        n = _int_from_input(event)
        result = CounterAgent.double(n)
        ctx.send_event(
            OutputEvent(
                output={"input": n, "doubled": result, "agent": "workflow_counter"}
            )
        )
