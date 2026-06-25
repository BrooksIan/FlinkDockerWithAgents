"""ReAct agent — direct LLM call via Designer settings (works local + cluster)."""

from __future__ import annotations

from typing import Any

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext

from examples.agents.react_double_value_logic import (
    double_value_from_message,
    hint_value,
    message_from_payload,
    payload_from_input,
)

_INPUT_EVENT = InputEvent.EVENT_TYPE


class ReactDoubleValueAgent(Agent):
    """Doubles numeric input using an LLM prompt (OpenAI-compatible API)."""

    @tool
    @staticmethod
    def double(value: int) -> int:
        """Return twice the input value."""
        return value * 2

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        payload = payload_from_input(InputEvent.from_event(event).input)
        message = message_from_payload(payload)
        value_hint = hint_value(payload)
        result = double_value_from_message(message, value_hint=value_hint)
        input_val = int(result["input"])
        ctx.send_event(
            OutputEvent(
                output={
                    "message": message,
                    "input": input_val,
                    "doubled": int(result["doubled"]),
                    "tool_doubled": ReactDoubleValueAgent.double(input_val),
                    "reasoning": result.get("reasoning", ""),
                    "mode": result.get("mode", "unknown"),
                    "agent": "react_double_value",
                }
            )
        )
