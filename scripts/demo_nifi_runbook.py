#!/usr/bin/env python3
"""POC demo: fault → NiFi monitor → react_nifi_runbook → HITL approve → heal.

Talking point: Inference builds the runbook; humans approve; workflow_nifi_monitor mutates.

Prereqs (live):
  ratatoskr up --profile nifi
  ./scripts/nifi_load_sample_flow.sh          # or nifi_load_kafka_flow.sh for stop-consume
  Designer / Cloudera LLM settings (optional — falls back without them)

Usage:
  python3 scripts/demo_nifi_runbook.py --list
  python3 scripts/demo_nifi_runbook.py --offline --scenario stop-generate
  python3 scripts/demo_nifi_runbook.py --scenario stop-generate
  # Phase 4 HITL (interactive prompt before heal):
  python3 scripts/demo_nifi_runbook.py --scenario stop-generate --heal
  # Non-interactive approve / reject:
  # Clean slate + scoped heal (default). Skip with --no-clean / --no-scope.
  python3 scripts/demo_nifi_runbook.py --scenario stop-generate --heal --approve --restore
  python3 scripts/demo_nifi_runbook.py --scenario stop-generate --heal --reject
  python3 scripts/demo_nifi_runbook.py --scenario stop-generate --heal --approve --dry-run-heal
  python3 scripts/demo_nifi_runbook.py --scenario stop-generate --publish-kafka --publish-hitl
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


def _load_fault_inject():
    path = ROOT / "scripts" / "nifi_fault_inject.py"
    spec = importlib.util.spec_from_file_location("nifi_fault_inject", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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


def _inject(scenario: dict[str, Any]) -> dict[str, Any]:
    fault = _load_fault_inject()
    from ratatoskr.nifi.client import NiFiClient

    client = NiFiClient()
    target = str(scenario["target"])
    pg_name = fault.KAFKA_PG if target == "kafka" else fault.SAMPLE_PG
    pg_id = fault._find_pg(client, pg_name)
    procs = fault._processors_by_name(client, pg_id)
    flag = str(scenario["fault"])

    if flag == "--stop-generate":
        gen = procs.get("GenerateFlowFile")
        if not gen:
            raise RuntimeError(
                "GenerateFlowFile not found — run ./scripts/nifi_load_sample_flow.sh"
            )
        return fault._stop(client, gen, "GenerateFlowFile")
    if flag == "--invalid-log":
        return fault.inject_invalid_log(client, pg_id)
    if flag == "--queue-backlog":
        return fault.inject_queue_backlog(client, pg_id, settle_sec=3.0)
    if flag == "--stop-consume":
        return fault.inject_stop_consume(client, pg_id)
    raise RuntimeError(f"unsupported fault flag: {flag}")


def _monitor(*, phase: str, dry_run: bool | None = None) -> dict[str, Any]:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import run_monitor_cycle

    pg = os.environ.get("NIFI_PROCESS_GROUP_ID", "root")
    return run_monitor_cycle(NiFiClient(), pg, phase=phase, dry_run=dry_run)


def _restore(scenario: dict[str, Any]) -> dict[str, Any]:
    fault = _load_fault_inject()
    from ratatoskr.nifi.client import NiFiClient

    client = NiFiClient()
    target = str(scenario["target"])
    if target == "kafka":
        flow = fault._load_sibling("nifi_load_kafka_flow")
        pg_id = fault._find_pg(client, fault.KAFKA_PG)
        return flow.repair_kafka_flow(client, pg_id, bootstrap=flow.default_bootstrap())
    flow = fault._load_sibling("nifi_load_sample_flow")
    pg_id = fault._find_pg(client, fault.SAMPLE_PG)
    return flow.repair_sample_flow(client, pg_id)


def _configure_dry_run(*, dry_run_heal: bool) -> None:
    """Ensure dry-run is explicit — clear stale shell env unless requested."""
    if dry_run_heal:
        os.environ["NIFI_HEAL_DRY_RUN"] = "1"
    else:
        os.environ.pop("NIFI_HEAL_DRY_RUN", None)


def main() -> int:
    _bootstrap()
    from ratatoskr.nifi.runbook.demo import (
        RUNBOOK_BRIEF_TOPIC,
        SCENARIOS,
        filter_ops_by_scenario,
        format_apply_status,
        list_scenarios,
        operator_talking_points,
        publish_runbook_brief,
        run_offline_scenario,
        scenario_heal_regex,
        scenario_watch_regex,
        scoped_nifi_env,
        summarize_monitor,
        summarize_runbook,
    )
    from ratatoskr.nifi.runbook.hitl import (
        ACK_TOPIC,
        PROPOSE_TOPIC,
        apply_approved_heal,
        attach_hitl,
        build_heal_proposal,
        decide_approval,
        publish_ack,
        publish_proposal,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list", action="store_true", help="List scenarios and exit")
    parser.add_argument(
        "--scenario",
        default="stop-generate",
        choices=sorted(SCENARIOS.keys()),
        help="Demo scenario (default: stop-generate)",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use Phase 0 fixtures only (no NiFi / fault inject)",
    )
    parser.add_argument(
        "--heal",
        action="store_true",
        help="After runbook, run Phase 4 HITL then heal if approved",
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
        help="With --heal, propose/apply as NIFI_HEAL_DRY_RUN=1 (ok=null)",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Skip pre-inject restore (default restores target flow first)",
    )
    parser.add_argument(
        "--no-scope",
        action="store_true",
        help="Do not scope watch/heal to scenario names (full canvas)",
    )
    parser.add_argument(
        "--publish-kafka",
        action="store_true",
        help=f"Publish runbook JSON to {RUNBOOK_BRIEF_TOPIC}",
    )
    parser.add_argument(
        "--publish-hitl",
        action="store_true",
        help=f"Publish propose/ack to {PROPOSE_TOPIC} / {ACK_TOPIC}",
    )
    parser.add_argument(
        "--kafka-topic",
        default=RUNBOOK_BRIEF_TOPIC,
        help=f"Override runbook brief topic (default: {RUNBOOK_BRIEF_TOPIC})",
    )
    parser.add_argument("--pause", action="store_true", help="Pause between steps")
    parser.add_argument(
        "--restore",
        action="store_true",
        help="Best-effort restore target flow after demo",
    )
    args = parser.parse_args()
    do_clean = not args.no_clean
    do_scope = not args.no_scope

    if args.approve and args.reject:
        print("Choose only one of --approve or --reject", file=sys.stderr)
        return 2

    if args.list:
        print(json.dumps(list_scenarios(), indent=2))
        return 0

    scenario = SCENARIOS[args.scenario]
    _banner(f"NiFi runbook demo: {args.scenario}")
    print(scenario["title"])
    print(
        "Principle: react_nifi_runbook explains; HITL approves; "
        "workflow_nifi_monitor heals under NIFI_HEAL_PHASE."
    )

    if args.offline:
        _pause(args.pause, "about to build offline fixture runbook")
        result = run_offline_scenario(args.scenario)
        _pp("monitor (fixture)", result["monitor_summary"])
        runbook = result["runbook"]
        _pp("runbook", result["runbook_summary"])
        print("\nTalking points:")
        for line in result["talking_points"]:
            print(f"  • {line}")

        if args.heal:
            _configure_dry_run(dry_run_heal=args.dry_run_heal)
            rem = (runbook.get("runbook") or {}).get("remediation") or {}
            allow = filter_ops_by_scenario(
                list(rem.get("safe_options") or []) + list(rem.get("lab_options") or []),
                scenario,
            ) if do_scope else None
            proposal = build_heal_proposal(
                runbook,
                heal_phase=str(scenario["heal_phase"]),
                dry_run=args.dry_run_heal,
                scenario=args.scenario,
                allow_ops=allow,
            )
            _banner("4. HITL propose (offline — no live heal apply)")
            _pp("proposal", proposal)
            auto = True if args.approve else (False if args.reject else None)
            interactive = auto is None
            ack = decide_approval(
                proposal, auto_approve=auto, interactive=interactive
            )
            runbook = attach_hitl(
                runbook,
                proposal,
                status="approved" if ack.get("approved") else "rejected",
                approved=bool(ack.get("approved")),
            )
            _pp("ack", ack)
            _pp("runbook+hitl", summarize_runbook(runbook))
            if ack.get("approved"):
                print(
                    "\nOffline mode: approval recorded only. "
                    "Re-run without --offline to apply via workflow_nifi_monitor."
                )
            if args.publish_hitl:
                _pp("propose publish", publish_proposal(proposal))
                _pp("ack publish", publish_ack(ack))

        if args.publish_kafka:
            pub = publish_runbook_brief(runbook, topic=args.kafka_topic)
            _pp("kafka publish", pub)
        return 0

    # --- live path ---
    _configure_dry_run(dry_run_heal=False)  # clear stale dry-run until heal step
    watch_re = scenario_watch_regex(scenario) if do_scope else None
    heal_re = scenario_heal_regex(scenario) if do_scope else None
    if do_scope:
        print(f"Scope: watch={watch_re!r} heal={heal_re!r}")

    if do_clean:
        _pause(args.pause, "about to restore target flow (clean slate)")
        _banner("0. Clean (restore target flow)")
        try:
            _pp("clean", _restore(scenario))
        except Exception as exc:  # noqa: BLE001
            print(f"clean failed (continuing): {exc}")

    _pause(args.pause, "about to inject fault")
    _banner("1. Fault inject")
    injected = _inject(scenario)
    _pp("inject", injected)
    time.sleep(1.0)

    _pause(args.pause, "about to poll monitor (phase=monitor)")
    _banner("2. workflow_nifi_monitor (phase=monitor)")
    os.environ["NIFI_HEAL_PHASE"] = "monitor"
    with scoped_nifi_env(watch_regex=watch_re):
        monitor = _monitor(phase="monitor", dry_run=False)
    _pp("monitor", summarize_monitor(monitor))

    _pause(args.pause, "about to build runbook via Cloudera Inference / fallback")
    _banner("3. react_nifi_runbook (explain-only)")
    from examples.agents.react_nifi_runbook_logic import build_runbook

    with scoped_nifi_env(watch_regex=watch_re):
        # Runbook uses the already-scoped monitor snapshot (not a new poll).
        runbook = build_runbook(monitor)
    if do_scope:
        rem = (runbook.get("runbook") or {}).get("remediation") or {}
        for key in ("safe_options", "lab_options"):
            rem[key] = filter_ops_by_scenario(list(rem.get(key) or []), scenario)
        runbook.setdefault("runbook", {})["remediation"] = rem
    _pp("runbook", summarize_runbook(runbook))
    print("\nTalking points:")
    for line in operator_talking_points(runbook, heal_phase=str(scenario["heal_phase"])):
        print(f"  • {line}")

    if args.publish_kafka:
        _banner("3b. Publish runbook brief")
        pub = publish_runbook_brief(runbook, topic=args.kafka_topic)
        _pp("kafka publish", pub)

    if args.heal:
        _configure_dry_run(dry_run_heal=args.dry_run_heal)
        rem = (runbook.get("runbook") or {}).get("remediation") or {}
        allow = filter_ops_by_scenario(
            list(rem.get("safe_options") or []) + list(rem.get("lab_options") or []),
            scenario,
        ) if do_scope else None
        proposal = build_heal_proposal(
            runbook,
            heal_phase=str(scenario["heal_phase"]),
            dry_run=args.dry_run_heal,
            scenario=args.scenario,
            allow_ops=allow,
        )
        _banner("4. HITL — Approve heal?")
        _pp(
            "proposal",
            {
                "proposal_id": proposal["proposal_id"],
                "heal_phase": proposal["heal_phase"],
                "dry_run": proposal["dry_run"],
                "proposed_ops": proposal["proposed_ops"],
                "scope": heal_re,
            },
        )
        if args.dry_run_heal:
            print("NOTE: dry_run=true — heal will plan only (heal_actions ok=null).")

        auto = True if args.approve else (False if args.reject else None)
        interactive = auto is None
        ack = decide_approval(proposal, auto_approve=auto, interactive=interactive)
        runbook = attach_hitl(
            runbook,
            proposal,
            status="approved" if ack.get("approved") else "rejected",
            approved=bool(ack.get("approved")),
        )
        _pp("ack", ack)

        if args.publish_hitl:
            _pp("propose publish", publish_proposal(proposal))
            _pp("ack publish", publish_ack(ack))

        if not ack.get("approved"):
            print("\nHeal skipped (not approved). Runbook mutations remain [].")
        else:
            _banner(
                f"5. Apply heal (phase={proposal['heal_phase']}, "
                f"dry_run={proposal['dry_run']})"
            )
            with scoped_nifi_env(watch_regex=watch_re, heal_regex=heal_re):
                applied = apply_approved_heal(ack, heal_name_regex=heal_re)
            print(format_apply_status(applied))
            audit = applied.get("audit") or {}
            _pp(
                "apply",
                {
                    "ok": applied.get("ok"),
                    "dry_run": applied.get("dry_run"),
                    "phase": applied.get("phase"),
                    "audit_dry_run": audit.get("dry_run"),
                    "executed_ok": applied.get("executed_ok"),
                    "heal_name_regex": applied.get("heal_name_regex"),
                    "heal_actions": applied.get("heal_actions"),
                },
            )
            runbook = attach_hitl(
                runbook,
                proposal,
                status="applied" if applied.get("ok") else "approved",
                approved=True,
                note=format_apply_status(applied),
            )

            _banner("6. Verify (re-poll monitor + runbook)")
            os.environ["NIFI_HEAL_PHASE"] = "monitor"
            os.environ.pop("NIFI_HEAL_DRY_RUN", None)
            with scoped_nifi_env(watch_regex=watch_re):
                verify = _monitor(phase="monitor", dry_run=False)
            _pp("verify monitor", summarize_monitor(verify))
            verify_rb = build_runbook(verify)
            if do_scope:
                vrem = (verify_rb.get("runbook") or {}).get("remediation") or {}
                for key in ("safe_options", "lab_options"):
                    vrem[key] = filter_ops_by_scenario(list(vrem.get(key) or []), scenario)
                verify_rb.setdefault("runbook", {})["remediation"] = vrem
            _pp("verify runbook", summarize_runbook(verify_rb))

    if args.restore:
        _banner("Restore")
        try:
            restored = _restore(scenario)
            _pp("restore", restored)
        except Exception as exc:  # noqa: BLE001
            print(f"restore failed: {exc}")

    print()
    print("Done. Inference never mutated; heal ran only after HITL approval (if any).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
