"""Module-level actions for Flink Agents YAML (workflow_counter)."""

from __future__ import annotations

from flink_agents.api.events.event import InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext


def double(value: int) -> int:
    """Return twice the input value."""
    return value * 2


def process(event: InputEvent, ctx: RunnerContext) -> None:
    payload = event.input
    if isinstance(payload, dict):
        raw = payload.get("value", 0)
    else:
        raw = payload
    n = int(raw)
    result = double(n)
    ctx.send_event(
        OutputEvent(
            output={"input": n, "doubled": result, "agent": "workflow_counter"}
        )
    )
