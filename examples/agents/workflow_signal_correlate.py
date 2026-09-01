"""Deterministic workflow agent — correlate NiFi ↔ Kafka monitor signals."""

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


class SignalCorrelateAgent(Agent):
    """
    Observe-only correlation of NiFi, Kafka, CM, and optional data-plane OutputEvents.

    Input may include ``nifi`` / ``kafka`` / ``cm`` / ``schema`` / ``route`` event dicts, or
    ``poll_live: true`` (also polls CM when configured, schema gate + route enrich).
    """

    @tool
    @staticmethod
    def correlate(
        nifi: dict[str, Any],
        kafka: dict[str, Any],
        cm: dict[str, Any] | None = None,
        schema: dict[str, Any] | None = None,
        route: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Correlate monitor OutputEvents into incidents."""
        from ratatoskr.correlation import correlate_signals

        return correlate_signals(
            nifi,
            kafka,
            cm_event=cm,
            schema_event=schema,
            route_event=route,
        )

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from ratatoskr.correlation import run_correlate_cycle

        payload = _payload_from_input(event)
        nifi = payload.get("nifi") if isinstance(payload.get("nifi"), dict) else None
        kafka = payload.get("kafka") if isinstance(payload.get("kafka"), dict) else None
        cm = payload.get("cm") if isinstance(payload.get("cm"), dict) else None
        schema = payload.get("schema") if isinstance(payload.get("schema"), dict) else None
        route = payload.get("route") if isinstance(payload.get("route"), dict) else None
        poll_live = bool(payload.get("poll_live"))
        poll_cm = payload.get("poll_cm")
        result = run_correlate_cycle(
            nifi_event=nifi,
            kafka_event=kafka,
            cm_event=cm,
            schema_event=schema,
            route_event=route,
            poll_live=poll_live or (nifi is None and kafka is None and cm is None),
            poll_cm=poll_cm if isinstance(poll_cm, bool) else None,
        )
        ctx.send_event(OutputEvent(output=result))
