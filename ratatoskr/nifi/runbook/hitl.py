"""Phase 4 HITL: propose NiFi heal from a runbook → human ack → workflow applies.

ReAct never mutates. Approval gates ``workflow_nifi_monitor`` heal phases.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, TextIO

PROPOSE_TOPIC = "nifi.runbook.propose"
ACK_TOPIC = "nifi.runbook.ack"

HITL_STATUSES = frozenset({"pending", "approved", "rejected", "applied", "skipped"})


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_heal_proposal(
    runbook_event: dict[str, Any],
    *,
    heal_phase: str = "safe",
    dry_run: bool = False,
    scenario: str | None = None,
) -> dict[str, Any]:
    """Build a heal proposal from runbook remediation (no mutations)."""
    rb = runbook_event.get("runbook") or {}
    rem = rb.get("remediation") or {}
    source = runbook_event.get("source") or {}
    safe = list(rem.get("safe_options") or [])
    lab = list(rem.get("lab_options") or [])
    phase = (heal_phase or "safe").lower()
    if phase == "safe":
        ops = list(safe)
    else:
        ops = list(safe) + list(lab)

    return {
        "kind": "nifi_runbook_heal_propose",
        "proposal_id": str(uuid.uuid4()),
        "ts": _utc_now(),
        "status": "pending",
        "heal_phase": phase,
        "dry_run": bool(dry_run),
        "scenario": scenario,
        "headline": rb.get("headline"),
        "mode": rb.get("mode"),
        "proposed_ops": ops,
        "safe_options": safe,
        "lab_options": lab,
        "poll_id": source.get("poll_id"),
        "severities": source.get("severities") or [],
        "agent": "react_nifi_runbook",
        "mutations": [],  # proposal itself never mutates
    }


def attach_hitl(
    runbook_event: dict[str, Any],
    proposal: dict[str, Any],
    *,
    status: str | None = None,
    approved: bool | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """Return a copy of the runbook event with ``hitl`` envelope fields."""
    out = dict(runbook_event)
    out["mutations"] = []
    hitl = {
        "proposal_id": proposal.get("proposal_id"),
        "status": status or proposal.get("status") or "pending",
        "heal_phase": proposal.get("heal_phase"),
        "dry_run": bool(proposal.get("dry_run")),
        "proposed_ops": list(proposal.get("proposed_ops") or []),
        "approved": approved,
        "note": note,
        "ts": _utc_now(),
    }
    out["hitl"] = hitl
    return out


def prompt_approve(
    proposal: dict[str, Any],
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    default_no: bool = True,
) -> dict[str, Any]:
    """
    Interactive Approve heal? prompt.

    Returns an ack dict: ``{proposal_id, approved, dry_run, heal_phase, raw}``.
    """
    import sys

    inp = stdin or sys.stdin
    out = stdout or sys.stdout
    phase = proposal.get("heal_phase") or "safe"
    dry = bool(proposal.get("dry_run"))
    ops = proposal.get("proposed_ops") or []
    out.write("\n--- HITL: Approve heal? ---\n")
    out.write(f"proposal_id: {proposal.get('proposal_id')}\n")
    out.write(f"heal_phase:  {phase}\n")
    out.write(f"dry_run:     {dry}\n")
    out.write(f"ops ({len(ops)}):\n")
    for op in ops:
        out.write(f"  • {op}\n")
    out.write(
        "Approve and run workflow_nifi_monitor heal? "
        f"[y/N]{' (default N)' if default_no else ''} "
    )
    out.flush()
    raw = (inp.readline() or "").strip().lower()
    approved = raw in ("y", "yes")
    return {
        "kind": "nifi_runbook_heal_ack",
        "proposal_id": proposal.get("proposal_id"),
        "approved": approved,
        "dry_run": dry,
        "heal_phase": phase,
        "raw": raw,
        "ts": _utc_now(),
        "mutations": [],
    }


def decide_approval(
    proposal: dict[str, Any],
    *,
    auto_approve: bool | None = None,
    interactive: bool = False,
    stdin: TextIO | None = None,
) -> dict[str, Any]:
    """
    Resolve HITL decision.

    - ``auto_approve=True`` → approved without prompt
    - ``auto_approve=False`` → rejected without prompt
    - ``interactive=True`` → prompt
    - otherwise → rejected (safe default)
    """
    if auto_approve is True:
        return {
            "kind": "nifi_runbook_heal_ack",
            "proposal_id": proposal.get("proposal_id"),
            "approved": True,
            "dry_run": bool(proposal.get("dry_run")),
            "heal_phase": proposal.get("heal_phase") or "safe",
            "raw": "auto-approve",
            "ts": _utc_now(),
            "mutations": [],
        }
    if auto_approve is False:
        return {
            "kind": "nifi_runbook_heal_ack",
            "proposal_id": proposal.get("proposal_id"),
            "approved": False,
            "dry_run": bool(proposal.get("dry_run")),
            "heal_phase": proposal.get("heal_phase") or "safe",
            "raw": "auto-reject",
            "ts": _utc_now(),
            "mutations": [],
        }
    if interactive:
        return prompt_approve(proposal, stdin=stdin)
    return {
        "kind": "nifi_runbook_heal_ack",
        "proposal_id": proposal.get("proposal_id"),
        "approved": False,
        "dry_run": bool(proposal.get("dry_run")),
        "heal_phase": proposal.get("heal_phase") or "safe",
        "raw": "no-decision",
        "ts": _utc_now(),
        "mutations": [],
        "note": "Heal skipped — pass --approve or use interactive HITL",
    }


def apply_approved_heal(
    ack: dict[str, Any],
    *,
    process_group_id: str | None = None,
) -> dict[str, Any]:
    """
    Execute heal via ``workflow_nifi_monitor`` only if ``ack.approved``.

    Sets env phase/dry_run for the cycle, then restores monitor phase afterward.
    """
    if not ack.get("approved"):
        return {
            "ok": False,
            "skipped": "not_approved",
            "proposal_id": ack.get("proposal_id"),
            "heal_actions": [],
            "mutations": [],
        }

    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import run_monitor_cycle

    phase = str(ack.get("heal_phase") or "safe").lower()
    dry = bool(ack.get("dry_run"))
    prev_phase = os.environ.get("NIFI_HEAL_PHASE")
    prev_dry = os.environ.get("NIFI_HEAL_DRY_RUN")
    os.environ["NIFI_HEAL_PHASE"] = phase
    if dry:
        os.environ["NIFI_HEAL_DRY_RUN"] = "1"
    else:
        os.environ.pop("NIFI_HEAL_DRY_RUN", None)

    pg = process_group_id or os.environ.get("NIFI_PROCESS_GROUP_ID", "root")
    try:
        result = run_monitor_cycle(NiFiClient(), pg, phase=phase, dry_run=dry)
    finally:
        if prev_phase is None:
            os.environ.pop("NIFI_HEAL_PHASE", None)
        else:
            os.environ["NIFI_HEAL_PHASE"] = prev_phase
        if prev_dry is None:
            os.environ.pop("NIFI_HEAL_DRY_RUN", None)
        else:
            os.environ["NIFI_HEAL_DRY_RUN"] = prev_dry

    actions = list(result.get("heal_actions") or [])
    executed_ok = sum(1 for a in actions if a.get("ok") is True)
    return {
        "ok": True,
        "proposal_id": ack.get("proposal_id"),
        "dry_run": dry,
        "phase": phase,
        "audit": result.get("audit"),
        "heal_actions": actions,
        "executed_ok": executed_ok,
        "monitor": result,
        "mutations": actions if not dry else [],
    }


def publish_proposal(proposal: dict[str, Any], *, topic: str = PROPOSE_TOPIC) -> dict[str, Any]:
    from kafka import KafkaProducer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    bootstrap = kafka_bootstrap_servers()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
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


def publish_ack(ack: dict[str, Any], *, topic: str = ACK_TOPIC) -> dict[str, Any]:
    from kafka import KafkaProducer

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    bootstrap = kafka_bootstrap_servers()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
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
