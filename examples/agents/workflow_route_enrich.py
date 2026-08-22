"""Deterministic workflow agent — propose routing/enrichment rules; NiFi applies."""

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


class RouteEnrichAgent(Agent):
    """
    Diff declarative routing rules against EnrichUpdate / RouteType properties,
    then apply allowlisted patches (``config_apply``, not heal).

    ``ROUTE_PHASE`` / ``DATAPLANE_PHASE``: monitor | safe | lab
    """

    @tool
    @staticmethod
    def poll_route_status(rule: dict[str, Any] | None = None) -> dict[str, Any]:
        """Compare proposed rule to live NiFi processor properties."""
        from ratatoskr.routing import poll_route_snapshot

        return poll_route_snapshot(rule=rule)

    @tool
    @staticmethod
    def classify_and_apply(
        snapshot: dict[str, Any],
        phase: str | None = None,
    ) -> dict[str, Any]:
        """Classify drift and apply route/enrich property patches."""
        from ratatoskr.nifi.client import NiFiClient
        from ratatoskr.routing import (
            apply_route_plan,
            build_route_plan,
            classify_route_health,
            route_phase,
        )

        client = NiFiClient()
        classification = classify_route_health(snapshot)
        plan = build_route_plan(snapshot, phase=phase)
        actions = apply_route_plan(client, plan, phase=phase)
        return {
            "classification": classification,
            "actions": actions,
            "phase": phase or route_phase(),
            "mutations": list(client.mutations),
        }

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from ratatoskr.routing import run_route_enrich_cycle, route_phase

        payload = _payload_from_input(event)
        phase = (
            payload.get("phase")
            or os.environ.get("ROUTE_PHASE")
            or os.environ.get("DATAPLANE_PHASE")
            or route_phase()
        )
        rule = payload.get("rule") if isinstance(payload.get("rule"), dict) else None
        dry_run = payload.get("dry_run")
        result = run_route_enrich_cycle(
            phase=str(phase),
            dry_run=bool(dry_run) if dry_run is not None else None,
            rule=rule,
        )
        ctx.send_event(OutputEvent(output=result))
