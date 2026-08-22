"""HITL for cross-signal runbooks: propose → ack → workflow_cross_stack_heal.

ReAct never mutates. Approval gates ``CROSS_HEAL_PHASE=lab`` playbooks.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, TextIO

PROPOSE_TOPIC = "signals.cross_runbook.propose"
ACK_TOPIC = "signals.cross_runbook.ack"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_cross_heal_proposal(
    runbook_event: dict[str, Any],
    *,
    dry_run: bool = False,
    scenario: str | None = None,
    allow_ops: list[str] | None = None,
) -> dict[str, Any]:
    """Build a cross-heal proposal from runbook remediation (no mutations)."""
    rb = runbook_event.get("runbook") or {}
    rem = rb.get("remediation") or {}
    source = runbook_event.get("source") or {}
    safe = list(rem.get("safe_options") or [])
    lab = list(rem.get("lab_options") or [])
    ops = list(safe) + list(lab)
    if allow_ops is not None:
        allow = set(allow_ops)
        ops = [o for o in ops if o in allow]
        safe = [o for o in safe if o in allow]
        lab = [o for o in lab if o in allow]

    return {
        "kind": "cross_runbook_heal_propose",
        "proposal_id": str(uuid.uuid4()),
        "ts": _utc_now(),
        "status": "pending",
        # Cross-stack only executes playbooks when phase=lab
        "heal_phase": "lab",
        "dry_run": bool(dry_run),
        "scenario": scenario,
        "headline": rb.get("headline"),
        "mode": rb.get("mode"),
        "proposed_ops": ops,
        "safe_options": safe,
        "lab_options": lab,
        "matched_rules": source.get("matched_rules") or [],
        "incident_count": source.get("incident_count"),
        "agent": "react_cross_runbook",
        "mutations": [],
    }


def attach_cross_hitl(
    runbook_event: dict[str, Any],
    proposal: dict[str, Any],
    *,
    status: str | None = None,
    approved: bool | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    out = dict(runbook_event)
    out["mutations"] = []
    out["hitl"] = {
        "proposal_id": proposal.get("proposal_id"),
        "status": status or proposal.get("status") or "pending",
        "heal_phase": proposal.get("heal_phase"),
        "dry_run": bool(proposal.get("dry_run")),
        "proposed_ops": list(proposal.get("proposed_ops") or []),
        "approved": approved,
        "note": note,
        "ts": _utc_now(),
    }
    return out


def prompt_cross_approve(
    proposal: dict[str, Any],
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
) -> dict[str, Any]:
    import sys

    inp = stdin or sys.stdin
    out = stdout or sys.stdout
    dry = bool(proposal.get("dry_run"))
    ops = proposal.get("proposed_ops") or []
    out.write("\n--- HITL: Approve cross-stack heal? ---\n")
    out.write(f"proposal_id: {proposal.get('proposal_id')}\n")
    out.write("heal_phase:  lab (CROSS_HEAL_PHASE)\n")
    out.write(f"dry_run:     {dry}\n")
    out.write(f"rules:       {proposal.get('matched_rules') or []}\n")
    out.write(f"ops ({len(ops)}):\n")
    for op in ops:
        out.write(f"  • {op}\n")
    out.write("Approve and run workflow_cross_stack_heal? [y/N] ")
    out.flush()
    raw = (inp.readline() or "").strip().lower()
    return {
        "kind": "cross_runbook_heal_ack",
        "proposal_id": proposal.get("proposal_id"),
        "approved": raw in ("y", "yes"),
        "dry_run": dry,
        "heal_phase": "lab",
        "raw": raw,
        "ts": _utc_now(),
        "mutations": [],
    }


def decide_cross_approval(
    proposal: dict[str, Any],
    *,
    auto_approve: bool | None = None,
    interactive: bool = False,
    stdin: TextIO | None = None,
) -> dict[str, Any]:
    if auto_approve is True:
        return {
            "kind": "cross_runbook_heal_ack",
            "proposal_id": proposal.get("proposal_id"),
            "approved": True,
            "dry_run": bool(proposal.get("dry_run")),
            "heal_phase": "lab",
            "raw": "auto-approve",
            "ts": _utc_now(),
            "mutations": [],
        }
    if auto_approve is False:
        return {
            "kind": "cross_runbook_heal_ack",
            "proposal_id": proposal.get("proposal_id"),
            "approved": False,
            "dry_run": bool(proposal.get("dry_run")),
            "heal_phase": "lab",
            "raw": "auto-reject",
            "ts": _utc_now(),
            "mutations": [],
        }
    if interactive:
        return prompt_cross_approve(proposal, stdin=stdin)
    return {
        "kind": "cross_runbook_heal_ack",
        "proposal_id": proposal.get("proposal_id"),
        "approved": False,
        "dry_run": bool(proposal.get("dry_run")),
        "heal_phase": "lab",
        "raw": "no-decision",
        "ts": _utc_now(),
        "mutations": [],
        "note": "Heal skipped — pass --approve or use interactive HITL",
    }


def format_cross_apply_status(applied: dict[str, Any]) -> str:
    actions = list(applied.get("heal_actions") or [])
    ok_t = sum(1 for a in actions if a.get("ok") is True)
    ok_f = sum(1 for a in actions if a.get("ok") is False)
    ok_n = sum(1 for a in actions if a.get("ok") is None)
    parts = [
        f"dry_run={bool(applied.get('dry_run') or applied.get('cross_heal_dry_run'))}",
        f"phase={applied.get('phase') or applied.get('cross_heal_phase')}",
        f"plan_steps={len(applied.get('cross_heal_plan') or [])}",
        f"actions={len(actions)}",
        f"executed_ok={applied.get('executed_ok', ok_t)}",
        f"failed={ok_f}",
        f"planned_only={ok_n}",
    ]
    if applied.get("skipped"):
        parts.append(f"gate={applied['skipped']}")
    return "cross heal status: " + " ".join(parts)


def apply_approved_cross_heal(
    ack: dict[str, Any],
    correlation: dict[str, Any],
    *,
    nifi_pg_id: str | None = None,
) -> dict[str, Any]:
    """
    Run ``apply_cross_heal_policy`` only if approved.

    Always uses CROSS phase ``lab`` (playbooks execute only then).
    """
    if not ack.get("approved"):
        return {
            "ok": False,
            "skipped": "not_approved",
            "proposal_id": ack.get("proposal_id"),
            "heal_actions": [],
            "cross_heal_plan": [],
            "mutations": [],
        }

    from ratatoskr.correlation.heal import apply_cross_heal_policy

    dry = bool(ack.get("dry_run"))
    prev_phase = os.environ.get("CROSS_HEAL_PHASE")
    prev_dry = os.environ.get("CROSS_HEAL_DRY_RUN")
    os.environ["CROSS_HEAL_PHASE"] = "lab"
    if dry:
        os.environ["CROSS_HEAL_DRY_RUN"] = "1"
    else:
        os.environ.pop("CROSS_HEAL_DRY_RUN", None)

    pg = nifi_pg_id or os.environ.get("NIFI_PROCESS_GROUP_ID", "root")
    try:
        heal = apply_cross_heal_policy(
            correlation,
            nifi_pg_id=pg,
            dry_run=dry,
            phase="lab",
        )
    finally:
        if prev_phase is None:
            os.environ.pop("CROSS_HEAL_PHASE", None)
        else:
            os.environ["CROSS_HEAL_PHASE"] = prev_phase
        if prev_dry is None:
            os.environ.pop("CROSS_HEAL_DRY_RUN", None)
        else:
            os.environ["CROSS_HEAL_DRY_RUN"] = prev_dry

    actions = list(heal.get("heal_actions") or [])
    executed_ok = sum(1 for a in actions if a.get("ok") is True)
    return {
        "ok": True,
        "proposal_id": ack.get("proposal_id"),
        "dry_run": dry,
        "phase": "lab",
        "cross_heal_phase": heal.get("cross_heal_phase"),
        "cross_heal_dry_run": heal.get("cross_heal_dry_run"),
        "cross_heal_plan": heal.get("cross_heal_plan") or [],
        "step_results": heal.get("step_results") or [],
        "heal_actions": actions,
        "executed_ok": executed_ok,
        "mutations": actions if not dry else [],
    }


def publish_cross_proposal(
    proposal: dict[str, Any], *, topic: str = PROPOSE_TOPIC
) -> dict[str, Any]:
    from kafka import KafkaProducer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: (k or "").encode("utf-8"),
    )
    key = str(proposal.get("proposal_id") or "")
    meta = producer.send(topic, key=key, value=proposal).get(timeout=15)
    producer.flush()
    producer.close()
    return {
        "ok": True,
        "topic": topic,
        "proposal_id": proposal.get("proposal_id"),
        "partition": getattr(meta, "partition", None),
        "offset": getattr(meta, "offset", None),
    }


def publish_cross_ack(ack: dict[str, Any], *, topic: str = ACK_TOPIC) -> dict[str, Any]:
    from kafka import KafkaProducer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: (k or "").encode("utf-8"),
    )
    key = str(ack.get("proposal_id") or "")
    meta = producer.send(topic, key=key, value=ack).get(timeout=15)
    producer.flush()
    producer.close()
    return {
        "ok": True,
        "topic": topic,
        "proposal_id": ack.get("proposal_id"),
        "approved": ack.get("approved"),
        "partition": getattr(meta, "partition", None),
        "offset": getattr(meta, "offset", None),
    }
