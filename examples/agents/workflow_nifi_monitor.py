"""Deterministic workflow agent — monitor Apache NiFi flows and optionally heal."""

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


class NiFiMonitorAgent(Agent):
    """
    Workflow agent for NiFi flow monitoring / healing.

    Heal behavior is gated by ``NIFI_HEAL_PHASE``:
      - monitor (1A): alerts only, no mutations
      - safe (1B): start stopped processors; enable disabled services
      - lab (1C): safe + templated config fix; restart on repeated bulletins;
        terminate invalid (no template); optional empty queues
    """

    @tool
    @staticmethod
    def get_flow_health_status(process_group_id: str = "root") -> dict[str, Any]:
        """Poll NiFi health (MCP-aligned tool name)."""
        from ratatoskr.nifi.client import NiFiClient

        return NiFiClient().get_flow_health_status(process_group_id)

    @tool
    @staticmethod
    def classify_and_heal(
        health: dict[str, Any],
        phase: str | None = None,
    ) -> dict[str, Any]:
        """Classify severities and apply heal policy for the active phase."""
        from ratatoskr.nifi.client import NiFiClient, heal_phase
        from ratatoskr.nifi.policy import apply_heal_policy, classify_health

        client = NiFiClient()
        classification = classify_health(health)
        actions = apply_heal_policy(client, health, phase=phase)
        return {
            "classification": classification,
            "heal_actions": actions,
            "phase": phase or heal_phase(),
            "mutations": list(client.mutations),
        }

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from ratatoskr.nifi.client import heal_phase
        from ratatoskr.nifi.policy import run_monitor_cycle
        from ratatoskr.nifi.client import NiFiClient

        payload = _payload_from_input(event)
        pg = str(payload.get("process_group_id") or payload.get("pg") or "root")
        phase = payload.get("phase") or os.environ.get("NIFI_HEAL_PHASE") or heal_phase()

        client = NiFiClient()
        result = run_monitor_cycle(client, pg, phase=str(phase))
        ctx.send_event(OutputEvent(output=result))
