"""Deterministic workflow agent — schema / contract gate (no heal run-state)."""

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


class SchemaGateAgent(Agent):
    """
    Monitor ``schema.violations`` / validate throughput; optionally ensure topics
    or swap JsonTreeReader schema text.

    ``SCHEMA_GATE_PHASE`` / ``DATAPLANE_PHASE``:
      - monitor: report only
      - safe: ensure topics + dataplane flow
      - lab: allowlisted schema text update only (never start/stop/heal)
    """

    @tool
    @staticmethod
    def poll_schema_status() -> dict[str, Any]:
        """Poll raw/valid/violations topic counts and flow presence."""
        from ratatoskr.schema import poll_schema_snapshot

        return poll_schema_snapshot()

    @tool
    @staticmethod
    def classify_and_apply(
        snapshot: dict[str, Any],
        phase: str | None = None,
        desired_schema: str | None = None,
    ) -> dict[str, Any]:
        """Classify severities and apply schema-gate policy for the active phase."""
        from ratatoskr.nifi.client import NiFiClient
        from ratatoskr.schema import (
            apply_schema_plan,
            build_schema_plan,
            classify_schema_health,
            schema_phase,
        )

        client = NiFiClient()
        classification = classify_schema_health(snapshot)
        plan = build_schema_plan(
            snapshot, phase=phase, desired_schema=desired_schema
        )
        actions = apply_schema_plan(client, plan, phase=phase)
        return {
            "classification": classification,
            "actions": actions,
            "phase": phase or schema_phase(),
            "mutations": list(client.mutations),
        }

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from ratatoskr.schema import run_schema_gate_cycle, schema_phase

        payload = _payload_from_input(event)
        phase = (
            payload.get("phase")
            or os.environ.get("SCHEMA_GATE_PHASE")
            or os.environ.get("DATAPLANE_PHASE")
            or schema_phase()
        )
        desired = payload.get("desired_schema") or payload.get("schema_text")
        dry_run = payload.get("dry_run")
        result = run_schema_gate_cycle(
            phase=str(phase),
            dry_run=bool(dry_run) if dry_run is not None else None,
            desired_schema=str(desired) if desired else None,
        )
        ctx.send_event(OutputEvent(output=result))
