"""Desired-state approval bus: propose → ack → apply over Kafka."""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from ratatoskr.dataplane.topics import TOPIC_ACK, TOPIC_PROPOSE

PROPOSE_TOPIC = TOPIC_PROPOSE
ACK_TOPIC = TOPIC_ACK

# Targets the apply step may execute.
APPLY_TARGETS = frozenset({"schema", "route", "replay"})


def _producer():
    from kafka import KafkaProducer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    return KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: (k or "").encode("utf-8"),
    )


def publish_proposal(
    *,
    target: str,
    plan: list[dict[str, Any]] | dict[str, Any],
    phase_on_apply: str = "lab",
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Publish a propose message to ``dataplane.propose`` (no mutations)."""
    if target not in APPLY_TARGETS:
        raise ValueError(f"target must be one of {sorted(APPLY_TARGETS)}")
    proposal_id = str(uuid.uuid4())
    body = {
        "kind": "propose",
        "proposal_id": proposal_id,
        "target": target,
        "plan": plan,
        "phase_on_apply": phase_on_apply,
        "meta": meta or {},
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    producer = _producer()
    try:
        producer.send(PROPOSE_TOPIC, key=proposal_id, value=body)
        producer.flush()
    finally:
        producer.close()
    return body


def publish_ack(
    proposal_id: str,
    *,
    approved: bool = True,
    actor: str = "operator",
    note: str = "",
) -> dict[str, Any]:
    """Publish an ack/nack to ``dataplane.ack``."""
    body = {
        "kind": "ack",
        "proposal_id": str(proposal_id),
        "approved": bool(approved),
        "actor": actor,
        "note": note,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    producer = _producer()
    try:
        producer.send(ACK_TOPIC, key=str(proposal_id), value=body)
        producer.flush()
    finally:
        producer.close()
    return body


def fetch_proposals(
    *,
    max_messages: int = 50,
    timeout_ms: int = 3000,
    group_id: str | None = None,
) -> list[dict[str, Any]]:
    """Read recent proposals (earliest in group; dedicated sample group by default)."""
    from kafka import KafkaConsumer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    group = group_id or f"ratatoskr-propose-read-{uuid.uuid4().hex[:8]}"
    consumer = KafkaConsumer(
        PROPOSE_TOPIC,
        bootstrap_servers=kafka_bootstrap_servers(),
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda b: json.loads(b.decode("utf-8", errors="replace")),
    )
    out: list[dict[str, Any]] = []
    try:
        for msg in consumer:
            if isinstance(msg.value, dict):
                out.append(msg.value)
            if len(out) >= max_messages:
                break
    finally:
        consumer.close()
    return out


def fetch_acks(
    *,
    max_messages: int = 50,
    timeout_ms: int = 3000,
    group_id: str | None = None,
) -> list[dict[str, Any]]:
    from kafka import KafkaConsumer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    group = group_id or f"ratatoskr-ack-read-{uuid.uuid4().hex[:8]}"
    consumer = KafkaConsumer(
        ACK_TOPIC,
        bootstrap_servers=kafka_bootstrap_servers(),
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda b: json.loads(b.decode("utf-8", errors="replace")),
    )
    out: list[dict[str, Any]] = []
    try:
        for msg in consumer:
            if isinstance(msg.value, dict):
                out.append(msg.value)
            if len(out) >= max_messages:
                break
    finally:
        consumer.close()
    return out


def apply_proposal(
    proposal: dict[str, Any],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute a proposal's plan for schema | route | replay."""
    target = str(proposal.get("target") or "")
    phase = str(proposal.get("phase_on_apply") or "lab")
    plan = proposal.get("plan")
    proposal_id = proposal.get("proposal_id")

    if target == "schema":
        from ratatoskr.nifi.client import NiFiClient
        from ratatoskr.schema import apply_schema_plan, run_schema_gate_cycle

        if isinstance(plan, list) and plan:
            actions = apply_schema_plan(
                NiFiClient(), plan, phase=phase, dry_run=dry_run
            )
            return {
                "ok": all(a.get("ok") or a.get("skipped") for a in actions),
                "target": target,
                "proposal_id": proposal_id,
                "actions": actions,
                "dry_run": dry_run,
            }
        cycle = run_schema_gate_cycle(phase=phase, dry_run=dry_run)
        return {
            "ok": True,
            "target": target,
            "proposal_id": proposal_id,
            "cycle": cycle,
            "dry_run": dry_run,
        }

    if target == "route":
        from ratatoskr.nifi.client import NiFiClient
        from ratatoskr.routing import apply_route_plan, run_route_enrich_cycle

        if isinstance(plan, list) and plan:
            actions = apply_route_plan(
                NiFiClient(), plan, phase=phase, dry_run=dry_run
            )
            return {
                "ok": all(a.get("ok") or a.get("skipped") for a in actions),
                "target": target,
                "proposal_id": proposal_id,
                "actions": actions,
                "dry_run": dry_run,
            }
        rule = None
        if isinstance(plan, dict):
            rule = plan.get("rule") if isinstance(plan.get("rule"), dict) else plan
        cycle = run_route_enrich_cycle(phase=phase, dry_run=dry_run, rule=rule)
        return {
            "ok": True,
            "target": target,
            "proposal_id": proposal_id,
            "cycle": cycle,
            "dry_run": dry_run,
        }

    if target == "replay":
        from ratatoskr.replay import apply_replay_plan, run_replay_cycle

        if isinstance(plan, dict) and plan.get("job_steps"):
            actions = apply_replay_plan(plan, phase=phase, dry_run=dry_run)
            return {
                "ok": all(a.get("ok") or a.get("skipped") for a in actions),
                "target": target,
                "proposal_id": proposal_id,
                "actions": actions,
                "dry_run": dry_run,
            }
        meta = proposal.get("meta") if isinstance(proposal.get("meta"), dict) else {}
        cycle = run_replay_cycle(
            phase=phase,
            dry_run=dry_run,
            source=meta.get("source"),
            dest=meta.get("dest"),
            hours=meta.get("hours"),
            group=meta.get("group"),
        )
        return {
            "ok": not any(
                a.get("ok") is False and not a.get("skipped")
                for a in (cycle.get("actions") or [])
            ),
            "target": target,
            "proposal_id": proposal_id,
            "cycle": cycle,
            "dry_run": dry_run,
        }

    return {
        "ok": False,
        "error": f"unknown target {target!r}",
        "proposal_id": proposal_id,
    }


def propose_from_live(
    target: str,
    *,
    phase_on_apply: str = "lab",
    rule: dict[str, Any] | None = None,
    hours: float | None = None,
) -> dict[str, Any]:
    """Poll live monitor/plan for target and publish a proposal."""
    if target == "schema":
        from ratatoskr.schema import build_schema_plan, poll_schema_snapshot

        snap = poll_schema_snapshot()
        plan = build_schema_plan(snap, phase=phase_on_apply)
        return publish_proposal(
            target="schema",
            plan=plan,
            phase_on_apply=phase_on_apply,
            meta={"classification": snap.get("flow_missing")},
        )
    if target == "route":
        from ratatoskr.routing import build_route_plan, poll_route_snapshot

        snap = poll_route_snapshot(rule=rule)
        plan = build_route_plan(snap, phase=phase_on_apply)
        return publish_proposal(
            target="route",
            plan=plan if plan else {"rule": rule or snap.get("rule")},
            phase_on_apply=phase_on_apply,
            meta={"diff": snap.get("diff")},
        )
    if target == "replay":
        from ratatoskr.replay import build_replay_plan

        plan = build_replay_plan(hours=hours)
        return publish_proposal(
            target="replay",
            plan=plan,
            phase_on_apply=phase_on_apply,
            meta={
                "source": plan.get("source"),
                "dest": plan.get("dest"),
                "hours": plan.get("hours"),
                "group": plan.get("group_id"),
            },
        )
    raise ValueError(f"unknown target {target!r}")


def run_approval_cycle(
    *,
    action: str = "propose",
    target: str = "schema",
    proposal_id: str | None = None,
    approved: bool = True,
    dry_run: bool = False,
    phase_on_apply: str = "lab",
    rule: dict[str, Any] | None = None,
    hours: float | None = None,
    wait_ack_sec: float = 0.0,
) -> dict[str, Any]:
    """
    One-shot bus cycle.

    action:
      - propose: publish live plan
      - ack: publish ack for proposal_id
      - apply: find proposal by id (or latest), require matching ack, apply
      - propose_ack_apply: propose → ack → apply (demo helper)
    """
    action = (action or "propose").strip().lower()
    out: dict[str, Any] = {
        "agent": "workflow_dataplane_approval",
        "poll_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "target": target,
        "mutations": [],
    }

    if action == "propose":
        proposal = propose_from_live(
            target, phase_on_apply=phase_on_apply, rule=rule, hours=hours
        )
        out["proposal"] = proposal
        return out

    if action == "ack":
        if not proposal_id:
            raise ValueError("proposal_id required for ack")
        out["ack"] = publish_ack(proposal_id, approved=approved)
        return out

    if action == "propose_ack_apply":
        proposal = propose_from_live(
            target, phase_on_apply=phase_on_apply, rule=rule, hours=hours
        )
        out["proposal"] = proposal
        ack = publish_ack(proposal["proposal_id"], approved=True, actor="demo")
        out["ack"] = ack
        if wait_ack_sec > 0:
            time.sleep(wait_ack_sec)
        result = apply_proposal(proposal, dry_run=dry_run)
        out["apply"] = result
        if result.get("actions"):
            out["mutations"] = [
                a
                for a in result["actions"]
                if a.get("ok") and not a.get("dry_run") and not a.get("skipped")
            ]
        return out

    if action == "apply":
        proposals = fetch_proposals(max_messages=100)
        acks = {a.get("proposal_id"): a for a in fetch_acks(max_messages=100)}
        chosen: Optional[dict[str, Any]] = None
        if proposal_id:
            for p in reversed(proposals):
                if p.get("proposal_id") == proposal_id:
                    chosen = p
                    break
        else:
            # Latest proposal for target that has an approved ack
            for p in reversed(proposals):
                if p.get("target") != target:
                    continue
                ack = acks.get(p.get("proposal_id"))
                if ack and ack.get("approved"):
                    chosen = p
                    break
        if not chosen:
            out["ok"] = False
            out["error"] = "no approved proposal found"
            return out
        ack = acks.get(chosen.get("proposal_id"))
        if not ack or not ack.get("approved"):
            out["ok"] = False
            out["error"] = "proposal not approved"
            out["proposal"] = chosen
            return out
        out["proposal"] = chosen
        out["ack"] = ack
        result = apply_proposal(chosen, dry_run=dry_run)
        out["apply"] = result
        out["ok"] = bool(result.get("ok"))
        if result.get("actions"):
            out["mutations"] = [
                a
                for a in result["actions"]
                if a.get("ok") and not a.get("dry_run") and not a.get("skipped")
            ]
        return out

    out["ok"] = False
    out["error"] = f"unknown action {action!r}"
    return out
