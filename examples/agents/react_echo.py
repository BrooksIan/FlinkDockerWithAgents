"""Tool-chaining workflow agent (ReAct-style pattern without an LLM)."""

from __future__ import annotations

from flink_agents.api.agents.agent import Agent
from flink_agents.api.decorators import action, tool
from flink_agents.api.events.event import InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext


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

    @action(InputEvent)
    @staticmethod
    def process(event: InputEvent, ctx: RunnerContext) -> None:
        payload = event.input
        text = str(payload.get("message", payload) if isinstance(payload, dict) else payload)
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
