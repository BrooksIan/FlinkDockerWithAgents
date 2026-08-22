"""Deterministic workflow agent — lab-gated Kafka/NiFi replay job (not heal)."""

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


class ReplayAgent(Agent):
    """
    Plan or execute a windowed replay: reset dedicated replay consumer offsets
    by timestamp, run Replay* NiFi path into dest topic, then stop.

    ``REPLAY_PHASE``: monitor (plan only) | lab (execute).
    """

    @tool
    @staticmethod
    def plan_replay(
        source: str | None = None,
        dest: str | None = None,
        hours: float = 1.0,
        group: str | None = None,
    ) -> dict[str, Any]:
        """Build replay job steps without mutating."""
        from ratatoskr.replay import build_replay_plan

        return build_replay_plan(
            source=source, dest=dest, hours=hours, group=group
        )

    @tool
    @staticmethod
    def run_replay(
        source: str | None = None,
        dest: str | None = None,
        hours: float = 1.0,
        group: str | None = None,
        phase: str = "monitor",
    ) -> dict[str, Any]:
        """Plan (monitor) or execute (lab) a replay job."""
        from ratatoskr.replay import run_replay_cycle

        return run_replay_cycle(
            phase=phase,
            source=source,
            dest=dest,
            hours=hours,
            group=group,
        )

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from ratatoskr.replay import replay_phase, run_replay_cycle

        payload = _payload_from_input(event)
        phase = (
            payload.get("phase")
            or os.environ.get("REPLAY_PHASE")
            or os.environ.get("DATAPLANE_PHASE")
            or replay_phase()
        )
        hours = payload.get("hours")
        result = run_replay_cycle(
            phase=str(phase),
            dry_run=bool(payload["dry_run"]) if payload.get("dry_run") is not None else None,
            source=str(payload["source"]) if payload.get("source") else None,
            dest=str(payload["dest"]) if payload.get("dest") else None,
            hours=float(hours) if hours is not None else None,
            group=str(payload["group"]) if payload.get("group") else None,
        )
        ctx.send_event(OutputEvent(output=result))
