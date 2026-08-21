"""Deterministic workflow agent — correlate + coordinated NiFi↔Kafka heals."""

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


class CrossStackHealAgent(Agent):
    """
    Correlate NiFi + Kafka monitor events, then run cross-stack heal playbooks
    when ``CROSS_HEAL_PHASE=lab`` (or input ``phase: lab``).

    Input may include ``nifi`` / ``kafka`` events, ``poll_live``, ``phase``,
    ``dry_run``, and ``nifi_pg_id``.
    """

    @tool
    @staticmethod
    def cross_heal(
        nifi: dict[str, Any],
        kafka: dict[str, Any],
        phase: str = "monitor",
    ) -> dict[str, Any]:
        """Correlate two monitor events and optionally heal across stacks."""
        from ratatoskr.correlation import run_cross_stack_cycle

        return run_cross_stack_cycle(
            nifi_event=nifi,
            kafka_event=kafka,
            phase=phase,
        )

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from ratatoskr.correlation import run_cross_stack_cycle

        payload = _payload_from_input(event)
        nifi = payload.get("nifi") if isinstance(payload.get("nifi"), dict) else None
        kafka = payload.get("kafka") if isinstance(payload.get("kafka"), dict) else None
        poll_live = bool(payload.get("poll_live"))
        phase = payload.get("phase")
        dry_run = payload.get("dry_run")
        pg = str(payload.get("nifi_pg_id") or "root")
        result = run_cross_stack_cycle(
            nifi_event=nifi,
            kafka_event=kafka,
            poll_live=poll_live or (nifi is None and kafka is None),
            phase=str(phase) if phase else None,
            dry_run=bool(dry_run) if dry_run is not None else None,
            nifi_pg_id=pg,
        )
        ctx.send_event(OutputEvent(output=result))
