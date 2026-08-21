"""Deterministic workflow agent — monitor Apache Kafka and optionally heal."""

from __future__ import annotations

import os
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


class KafkaMonitorAgent(Agent):
    """
    Workflow agent for Kafka cluster monitoring / healing.

    Heal behavior is gated by ``KAFKA_HEAL_PHASE``:
      - monitor: alerts only
      - safe: create missing catalog topics
      - lab: safe + allowlisted offset reset / delete empty groups
    """

    @tool
    @staticmethod
    def get_cluster_health_status() -> dict[str, Any]:
        """Poll Kafka broker health."""
        from ratatoskr.kafka import KafkaClient

        client = KafkaClient()
        try:
            return client.get_cluster_health_status()
        finally:
            client.close()

    @tool
    @staticmethod
    def classify_and_heal(
        health: dict[str, Any],
        phase: str | None = None,
    ) -> dict[str, Any]:
        """Classify severities and apply heal policy for the active phase."""
        from ratatoskr.kafka import KafkaClient, apply_heal_policy, classify_health, heal_phase

        client = KafkaClient()
        try:
            classification = classify_health(health)
            actions = apply_heal_policy(client, health, phase=phase)
            return {
                "classification": classification,
                "heal_actions": actions,
                "phase": phase or heal_phase(),
                "mutations": list(client.mutations),
            }
        finally:
            client.close()

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from ratatoskr.kafka import KafkaClient, heal_phase, run_monitor_cycle

        payload = _payload_from_input(event)
        phase = payload.get("phase") or os.environ.get("KAFKA_HEAL_PHASE") or heal_phase()
        kwargs: dict[str, Any] = {}
        if payload.get("bootstrap"):
            kwargs["bootstrap"] = str(payload["bootstrap"])
        client = KafkaClient(**kwargs)
        try:
            result = run_monitor_cycle(client, phase=str(phase))
        finally:
            client.close()
        ctx.send_event(OutputEvent(output=result))
