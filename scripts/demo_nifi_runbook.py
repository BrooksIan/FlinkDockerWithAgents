#!/usr/bin/env python3
"""POC demo: fault → NiFi monitor → react_nifi_runbook → (optional) heal / Kafka sink.

Talking point: Inference builds the runbook; workflow_nifi_monitor still owns mutations.

Prereqs (live):
  ratatoskr up --profile nifi
  ./scripts/nifi_load_sample_flow.sh          # or nifi_load_kafka_flow.sh for stop-consume
  Designer / Cloudera LLM settings (optional — falls back without them)

Usage:
  python3 scripts/demo_nifi_runbook.py --list
  python3 scripts/demo_nifi_runbook.py --offline --scenario stop-generate
  python3 scripts/demo_nifi_runbook.py --scenario stop-generate
  python3 scripts/demo_nifi_runbook.py --scenario invalid-log --heal
  python3 scripts/demo_nifi_runbook.py --scenario stop-generate --publish-kafka
  python3 scripts/demo_nifi_runbook.py --scenario stop-generate --pause
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
            raise RuntimeError("GenerateFlowFile not found — run ./scripts/nifi_load_sample_flow.sh")
        return fault._stop(client, gen, "GenerateFlowFile")
    if flag == "--invalid-log":
        return fault.inject_invalid_log(client, pg_id)
    if flag == "--queue-backlog":
        return fault.inject_queue_backlog(client, pg_id, settle_sec=3.0)
    if flag == "--stop-consume":
        return fault.inject_stop_consume(client, pg_id)
    raise RuntimeError(f"unsupported fault flag: {flag}")


def _monitor(*, phase: str) -> dict[str, Any]:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import run_monitor_cycle

    pg = os.environ.get("NIFI_PROCESS_GROUP_ID", "root")
    return run_monitor_cycle(NiFiClient(), pg, phase=phase)


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


def main() -> int:
    _bootstrap()
    from ratatoskr.nifi.runbook.demo import (
        RUNBOOK_BRIEF_TOPIC,
        SCENARIOS,
        list_scenarios,
        operator_talking_points,
        publish_runbook_brief,
        run_offline_scenario,
        summarize_monitor,
        summarize_runbook,
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
        help="After runbook, re-poll with scenario heal phase (mutates if not dry-run)",
    )
    parser.add_argument(
        "--dry-run-heal",
        action="store_true",
        help="With --heal, set NIFI_HEAL_DRY_RUN=1",
    )
    parser.add_argument(
        "--publish-kafka",
        action="store_true",
        help=f"Publish runbook JSON to {RUNBOOK_BRIEF_TOPIC}",
    )
    parser.add_argument(
        "--kafka-topic",
        default=RUNBOOK_BRIEF_TOPIC,
        help=f"Override Kafka sink topic (default: {RUNBOOK_BRIEF_TOPIC})",
    )
    parser.add_argument("--pause", action="store_true", help="Pause between steps")
    parser.add_argument(
        "--restore",
        action="store_true",
        help="Best-effort restore target flow after demo",
    )
    args = parser.parse_args()

    if args.list:
        print(json.dumps(list_scenarios(), indent=2))
        return 0

    scenario = SCENARIOS[args.scenario]
    _banner(f"NiFi runbook demo: {args.scenario}")
    print(scenario["title"])
    print(
        "Principle: react_nifi_runbook explains; workflow_nifi_monitor heals under NIFI_HEAL_PHASE."
    )

    if args.offline:
        _pause(args.pause, "about to build offline fixture runbook")
        result = run_offline_scenario(args.scenario)
        _pp("monitor (fixture)", result["monitor_summary"])
        _pp("runbook", result["runbook_summary"])
        print("\nTalking points:")
        for line in result["talking_points"]:
            print(f"  • {line}")
        if args.publish_kafka:
            pub = publish_runbook_brief(result["runbook"], topic=args.kafka_topic)
            _pp("kafka publish", pub)
        return 0

    # --- live path ---
    _pause(args.pause, "about to inject fault")
    _banner("1. Fault inject")
    injected = _inject(scenario)
    _pp("inject", injected)
    time.sleep(1.0)

    _pause(args.pause, "about to poll monitor (phase=monitor)")
    _banner("2. workflow_nifi_monitor (phase=monitor)")
    os.environ["NIFI_HEAL_PHASE"] = "monitor"
    monitor = _monitor(phase="monitor")
    _pp("monitor", summarize_monitor(monitor))

    _pause(args.pause, "about to build runbook via Cloudera Inference / fallback")
    _banner("3. react_nifi_runbook (explain-only)")
    from examples.agents.react_nifi_runbook_logic import build_runbook

    runbook = build_runbook(monitor)
    _pp("runbook", summarize_runbook(runbook))
    print("\nTalking points:")
    for line in operator_talking_points(runbook, heal_phase=str(scenario["heal_phase"])):
        print(f"  • {line}")

    if args.publish_kafka:
        _banner("3b. Publish runbook brief")
        pub = publish_runbook_brief(runbook, topic=args.kafka_topic)
        _pp("kafka publish", pub)

    if args.heal:
        _pause(args.pause, f"about to heal with phase={scenario['heal_phase']}")
        _banner(f"4. Heal (NIFI_HEAL_PHASE={scenario['heal_phase']})")
        if args.dry_run_heal:
            os.environ["NIFI_HEAL_DRY_RUN"] = "1"
        os.environ["NIFI_HEAL_PHASE"] = str(scenario["heal_phase"])
        healed = _monitor(phase=str(scenario["heal_phase"]))
        _pp("heal monitor", summarize_monitor(healed))
        _pp("heal_actions", healed.get("heal_actions") or [])

        _banner("5. Verify runbook (re-poll monitor)")
        os.environ["NIFI_HEAL_PHASE"] = "monitor"
        verify = _monitor(phase="monitor")
        _pp("verify monitor", summarize_monitor(verify))
        verify_rb = build_runbook(verify)
        _pp("verify runbook", summarize_runbook(verify_rb))

    if args.restore:
        _banner("Restore")
        try:
            restored = _restore(scenario)
            _pp("restore", restored)
        except Exception as exc:  # noqa: BLE001
            print(f"restore failed: {exc}")

    print()
    print("Done. Inference did not touch the canvas; gated heal (if used) did.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
