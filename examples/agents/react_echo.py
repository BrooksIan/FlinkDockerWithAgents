"""Tool-chaining workflow agent (ReAct-style pattern without an LLM)."""

from __future__ import annotations

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext

_INPUT_EVENT = InputEvent.EVENT_TYPE


def _text_from_input(event: Event) -> str:
    payload = InputEvent.from_event(event).input
    if isinstance(payload, dict):
        return str(payload.get("message", payload))
    message = getattr(payload, "message", None)
    return str(message if message is not None else payload)


class ReactEchoAgent(Agent):
    """
    Demonstrates observe → act → observe using deterministic tools only.

    Use this as a local lab stand-in before wiring a real chat model for ReAct.
    """

    @tool
    @staticmethod
    def classify(text: str) -> str:
        lowered = text.lower()
        if any(word in lowered for word in ("error", "fail", "critical")):
            return "HIGH"
        if any(word in lowered for word in ("warn", "retry")):
            return "MEDIUM"
        return "LOW"

    @tool
    @staticmethod
    def summarize(text: str, severity: str) -> str:
        return f"[{severity}] {text[:80]}"

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        text = _text_from_input(event)
        severity = ReactEchoAgent.classify(text)
        summary = ReactEchoAgent.summarize(text, severity)
        ctx.send_event(
            OutputEvent(
                output={
                    "message": text,
                    "severity": severity,
                    "summary": summary,
                    "agent": "react_echo",
                    "pattern": "react_without_llm",
                }
            )
        )
