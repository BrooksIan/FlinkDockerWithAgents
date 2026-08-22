#!/usr/bin/env python3
"""POC demo: correlate → react_cross_runbook → HITL → workflow_cross_stack_heal.

Talking point: Inference builds one checklist; humans approve; cross-stack workflow mutates.

Prereqs (live / inject):
  ratatoskr kafka up && ratatoskr up --profile nifi
  ./scripts/nifi_load_kafka_flow.sh
  Designer / Cloudera LLM settings (optional — falls back without them)

Usage:
  python3 scripts/demo_cross_runbook.py
  python3 scripts/demo_cross_runbook.py --scenario topic-missing
  python3 scripts/demo_cross_runbook.py --scenario topic-missing --heal --approve
  python3 scripts/demo_cross_runbook.py --live
  # Live inject (cross-topic) + HITL approve + heal:
  python3 scripts/demo_cross_runbook.py --live --inject --heal --approve
  python3 scripts/demo_cross_runbook.py --live --inject --heal --approve --dry-run-heal
  python3 scripts/demo_cross_runbook.py --live --inject --heal --reject
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def _bootstrap() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))


def _banner(title: str) -> None:
    print()
    print("=" * 64)
    print(title)
    print("=" * 64)


def _pause(enabled: bool, hint: str) -> None:
    if not enabled:
        return
    input(f"\n[pause] {hint} — press Enter to continue… ")


def _pp(label: str, obj: Any) -> None:
    print(f"\n--- {label} ---")
    print(json.dumps(obj, indent=2, default=str), flush=True)


def _demo_topic_missing() -> tuple[dict, dict]:
    nifi = {
        "agent": "workflow_nifi_monitor",
        "poll_id": "demo-nifi-stopped",
        "classification": {
            "healthy": False,
            "level": "MEDIUM",
            "score": 75,
            "severities": ["STOPPED"],
            "summary": "STOPPED",
        },
        "health": {
            "severities": ["STOPPED"],
            "stopped_processors": [{"id": "c1", "name": "ConsumeKafka", "state": "STOPPED"}],
        },
    }
    kafka = {
        "agent": "workflow_kafka_monitor",
        "poll_id": "demo-kafka-missing",
        "classification": {
            "healthy": False,
            "level": "HIGH",
            "score": 50,
            "severities": ["TOPIC_MISSING"],
            "summary": "TOPIC_MISSING",
        },
        "health": {
            "severities": ["TOPIC_MISSING"],
            "missing_topics": [{"name": "nifi.kafka.demo"}],
        },
    }
    return nifi, kafka


def _summarize_correlation(correlation: dict[str, Any]) -> dict[str, Any]:
    from ratatoskr.correlation import plan_cross_heals

    return {
        "classification": correlation.get("classification"),
        "matched_rules": correlation.get("matched_rules"),
        "incidents": [
            {"rule": i.get("rule"), "title": i.get("title"), "level": i.get("level")}
            for i in (correlation.get("incidents") or [])
        ],
        "cross_heal_plan": [
            {"id": s.get("id"), "side": s.get("side"), "phase": s.get("phase")}
            for s in plan_cross_heals(correlation)
        ],
    }


def _summarize_runbook(runbook: dict[str, Any]) -> dict[str, Any]:
    rb = runbook.get("runbook") or {}
    rem = rb.get("remediation") or {}
    hitl = runbook.get("hitl")
    out: dict[str, Any] = {
        "mode": rb.get("mode"),
        "headline": rb.get("headline"),
        "situation": rb.get("situation"),
        "safe_options": rem.get("safe_options"),
        "lab_options": rem.get("lab_options"),
        "verify": rb.get("verify"),
        "mutations": runbook.get("mutations"),
        "source": runbook.get("source"),
    }
    if hitl:
        out["hitl"] = hitl
    return out


def _configure_dry_run(*, dry_run_heal: bool) -> None:
    """Ensure dry-run is explicit — clear stale shell env unless requested."""
    if dry_run_heal:
        os.environ["CROSS_HEAL_DRY_RUN"] = "1"
    else:
        os.environ.pop("CROSS_HEAL_DRY_RUN", None)


def _load_heal_demo():
    path = ROOT / "scripts" / "demo_nifi_kafka_heal.py"
    spec = importlib.util.spec_from_file_location("demo_nifi_kafka_heal", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _inject_cross_topic() -> dict[str, Any]:
    """Stop ConsumeKafka + delete nifi.kafka.demo (same as demo cross-topic)."""
    heal = _load_heal_demo()
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.nifi.client import NiFiClient

    fault = heal._load("nifi_fault_inject", "scripts/nifi_fault_inject.py")
    flow = heal._load("nifi_load_kafka_flow", "scripts/nifi_load_kafka_flow.py")
    kfault = heal._load("kafka_fault_inject", "scripts/kafka_fault_inject.py")
    nifi = NiFiClient()
    kc = KafkaClient()
    pg_id = fault._find_pg(nifi, fault.KAFKA_PG)
    # Best-effort clean: ensure kafka flow exists before inject
    try:
        flow.repair_kafka_flow(nifi, pg_id, bootstrap=flow.default_bootstrap())
    except Exception as exc:  # noqa: BLE001
        print(f"pre-inject repair (continuing): {exc}")
    return heal._sc_cross_topic(fault, flow, kfault, nifi, kc, pg_id)


def _poll_live() -> tuple[dict, dict]:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import run_monitor_cycle as kafka_cycle
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import run_monitor_cycle as nifi_cycle

    os.environ.setdefault("NIFI_HEAL_PHASE", "monitor")
    os.environ.setdefault("KAFKA_HEAL_PHASE", "monitor")
    os.environ.setdefault("CROSS_HEAL_PHASE", "monitor")
    nifi = nifi_cycle(NiFiClient(), "root", phase="monitor")
    kafka = kafka_cycle(KafkaClient(), phase="monitor")
    return nifi, kafka


def _hitl_step(
    *,
    runbook: dict[str, Any],
    correlation: dict[str, Any],
    args: argparse.Namespace,
    live_apply: bool,
) -> dict[str, Any]:
    from ratatoskr.correlation.runbook.hitl import (
        apply_approved_cross_heal,
        attach_cross_hitl,
        build_cross_heal_proposal,
        decide_cross_approval,
        format_cross_apply_status,
        publish_cross_ack,
        publish_cross_proposal,
    )

    _configure_dry_run(dry_run_heal=args.dry_run_heal)
    proposal = build_cross_heal_proposal(
        runbook,
        dry_run=args.dry_run_heal,
        scenario=args.scenario if not args.live else "live",
    )
    _banner("3. HITL — Approve cross-stack heal?")
    _pp(
        "proposal",
        {
            "proposal_id": proposal["proposal_id"],
            "heal_phase": proposal["heal_phase"],
            "dry_run": proposal["dry_run"],
            "matched_rules": proposal.get("matched_rules"),
            "proposed_ops": proposal["proposed_ops"],
        },
    )
    if args.dry_run_heal:
        print("NOTE: dry_run=true — heal will plan only (ok=null / no mutations).")

    auto = True if args.approve else (False if args.reject else None)
    interactive = auto is None
    ack = decide_cross_approval(
        proposal, auto_approve=auto, interactive=interactive
    )
    runbook = attach_cross_hitl(
        runbook,
        proposal,
        status="approved" if ack.get("approved") else "rejected",
        approved=bool(ack.get("approved")),
    )
    _pp("ack", ack)

    if args.publish_hitl:
        _pp("propose publish", publish_cross_proposal(proposal))
        _pp("ack publish", publish_cross_ack(ack))

    if not ack.get("approved"):
        print("\nHeal skipped (not approved). Runbook mutations remain [].")
        return runbook

    if not live_apply:
        print(
            "\nOffline mode: approval recorded only. "
            "Re-run with --live [--inject] --heal --approve to apply "
            "via workflow_cross_stack_heal."
        )
        _pp("runbook+hitl", _summarize_runbook(runbook))
        return runbook

    _banner("4. Apply (CROSS_HEAL_PHASE=lab)")
    applied = apply_approved_cross_heal(ack, correlation)
    print(format_cross_apply_status(applied))
    _pp(
        "apply",
        {
            "ok": applied.get("ok"),
            "dry_run": applied.get("dry_run"),
            "phase": applied.get("phase"),
            "executed_ok": applied.get("executed_ok"),
            "cross_heal_plan": applied.get("cross_heal_plan"),
            "heal_actions": applied.get("heal_actions"),
            "step_results": applied.get("step_results"),
        },
    )
    runbook = attach_cross_hitl(
        runbook,
        proposal,
        status="applied" if applied.get("ok") else "approved",
        approved=True,
        note=format_cross_apply_status(applied),
    )
    return runbook


def main() -> int:
    _bootstrap()
    from examples.agents.react_cross_runbook_logic import build_cross_runbook
    from ratatoskr.correlation import correlate_signals
    from ratatoskr.correlation.runbook.hitl import ACK_TOPIC, PROPOSE_TOPIC

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=("backpressure-lag", "topic-missing"),
        default="backpressure-lag",
        help="Offline correlation fixture (default: backpressure-lag)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Poll live NiFi + Kafka monitors",
    )
    parser.add_argument(
        "--inject",
        action="store_true",
        help="With --live, inject cross-topic fault before poll",
    )
    parser.add_argument(
        "--heal",
        action="store_true",
        help="After runbook, run HITL then heal if approved",
    )
    parser.add_argument(
        "--approve",
        action="store_true",
        help="With --heal, auto-approve (non-interactive)",
    )
    parser.add_argument(
        "--reject",
        action="store_true",
        help="With --heal, auto-reject (non-interactive)",
    )
    parser.add_argument(
        "--dry-run-heal",
        action="store_true",
        help="With --heal, propose/apply as CROSS_HEAL_DRY_RUN=1",
    )
    parser.add_argument(
        "--publish-hitl",
        action="store_true",
        help=f"Publish propose/ack to {PROPOSE_TOPIC} / {ACK_TOPIC}",
    )
    parser.add_argument("--pause", action="store_true", help="Pause between steps")
    args = parser.parse_args()

    if args.approve and args.reject:
        print("Choose only one of --approve or --reject", file=sys.stderr)
        return 2
    if args.inject and not args.live:
        print("--inject requires --live", file=sys.stderr)
        return 2

    label = "live" if args.live else args.scenario
    _banner(f"Cross-signal runbook demo: {label}")
    print(
        "Principle: react_cross_runbook explains; HITL approves; "
        "workflow_cross_stack_heal mutates under CROSS_HEAL_PHASE=lab."
    )

    _configure_dry_run(dry_run_heal=False)

    if args.live:
        if args.inject:
            _pause(args.pause, "about to inject cross-topic fault")
            _banner("0. Inject (cross-topic)")
            try:
                _pp("inject", _inject_cross_topic())
            except Exception as exc:  # noqa: BLE001
                print(f"inject failed: {exc}", file=sys.stderr)
                return 1
            time.sleep(1.5)
        _pause(args.pause, "about to poll live monitors")
        _banner("1. Correlate (live poll)")
        nifi, kafka = _poll_live()
    elif args.scenario == "topic-missing":
        _banner("1. Correlate (fixture)")
        nifi, kafka = _demo_topic_missing()
    else:
        from examples.agents.run_workflow_signal_correlate_local import _demo_events

        _banner("1. Correlate (fixture)")
        nifi, kafka = _demo_events()

    correlation = correlate_signals(nifi, kafka)
    _pp("correlation", _summarize_correlation(correlation))

    _pause(args.pause, "about to build cross runbook")
    _banner("2. react_cross_runbook")
    runbook = build_cross_runbook(correlation)
    _pp("runbook", _summarize_runbook(runbook))
    print("\nTalking points:")
    print("  • Inference/fallback built one checklist for both sides.")
    print("  • mutations=[] until HITL + workflow_cross_stack_heal (lab).")
    print("  • Side gates (NIFI_HEAL_* / KAFKA_HEAL_*) still apply inside playbooks.")

    if args.heal:
        runbook = _hitl_step(
            runbook=runbook,
            correlation=correlation,
            args=args,
            live_apply=bool(args.live),
        )

        if args.live and args.approve and not args.reject:
            _banner("5. Verify (re-correlate)")
            os.environ["CROSS_HEAL_PHASE"] = "monitor"
            os.environ.pop("CROSS_HEAL_DRY_RUN", None)
            nifi2, kafka2 = _poll_live()
            verify = correlate_signals(nifi2, kafka2)
            _pp("verify correlation", _summarize_correlation(verify))
            verify_rb = build_cross_runbook(verify)
            _pp("verify runbook", _summarize_runbook(verify_rb))

    print()
    print("Done. Inference never mutated; heal ran only after HITL approval (if any).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
