"""Deterministic workflow agent — dataplane propose → ack → apply bus."""

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


class DataplaneApprovalAgent(Agent):
    """
    Kafka desired-state bus for schema / route / replay.

    Input ``action``: propose | ack | apply | propose_ack_apply
    Input ``target``: schema | route | replay
    """

    @tool
    @staticmethod
    def propose(target: str = "schema", phase_on_apply: str = "lab") -> dict[str, Any]:
        """Publish a live plan to dataplane.propose."""
        from ratatoskr.dataplane.bus import propose_from_live

        return propose_from_live(target, phase_on_apply=phase_on_apply)

    @tool
    @staticmethod
    def ack(proposal_id: str, approved: bool = True) -> dict[str, Any]:
        """Publish an approval to dataplane.ack."""
        from ratatoskr.dataplane.bus import publish_ack

        return publish_ack(proposal_id, approved=approved)

    @action(_INPUT_EVENT)
    @staticmethod
    def process(event: Event, ctx: RunnerContext) -> None:
        from ratatoskr.dataplane.bus import run_approval_cycle

        payload = _payload_from_input(event)
        result = run_approval_cycle(
            action=str(payload.get("action") or "propose"),
            target=str(payload.get("target") or "schema"),
            proposal_id=str(payload["proposal_id"])
            if payload.get("proposal_id")
            else None,
            approved=bool(payload.get("approved", True)),
            dry_run=bool(payload.get("dry_run", False)),
            phase_on_apply=str(payload.get("phase_on_apply") or "lab"),
            rule=payload.get("rule") if isinstance(payload.get("rule"), dict) else None,
            hours=float(payload["hours"]) if payload.get("hours") is not None else None,
        )
        ctx.send_event(OutputEvent(output=result))
