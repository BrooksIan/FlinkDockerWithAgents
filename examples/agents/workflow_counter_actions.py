"""Module-level actions for Flink Agents YAML (workflow_counter)."""

from __future__ import annotations

from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext


def double(value: int) -> int:
    """Return twice the input value."""
    return value * 2


def _int_from_input(event: Event) -> int:
    payload = InputEvent.from_event(event).input
    if isinstance(payload, dict):
        raw = payload.get("value", 0)
    else:
        raw = getattr(payload, "value", payload)
    return int(raw)


def process(event: Event, ctx: RunnerContext) -> None:
    n = _int_from_input(event)
    result = double(n)
    ctx.send_event(
        OutputEvent(
            output={"input": n, "doubled": result, "agent": "workflow_counter"}
        )
    )
