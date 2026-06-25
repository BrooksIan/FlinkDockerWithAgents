"""Deterministic workflow agent — doubles numeric inputs via a @tool."""

from __future__ import annotations

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext


class CounterAgent(Agent):
    """Workflow agent that doubles integer values from input events."""

    @tool
    @staticmethod
    def double(value: int) -> int:
        """Return twice the input value."""
        return value * 2

    @action(InputEvent)
    @staticmethod
    def process(event: InputEvent, ctx: RunnerContext) -> None:
        payload = event.input
        if isinstance(payload, dict):
            raw = payload.get("value", 0)
        else:
            raw = payload
        n = int(raw)
        result = CounterAgent.double(n)
        ctx.send_event(
            OutputEvent(
                output={"input": n, "doubled": result, "agent": "workflow_counter"}
            )
        )
