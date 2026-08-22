#!/usr/bin/env python3
"""POC demo: correlate NiFi+Kafka → react_cross_runbook (explain-only).

Talking point: one checklist for cross-signal incidents; heals stay on
workflow_cross_stack_heal / side monitors.

Usage:
  python3 scripts/demo_cross_runbook.py
  python3 scripts/demo_cross_runbook.py --scenario topic-missing
  python3 scripts/demo_cross_runbook.py --live
"""

from __future__ import annotations

import argparse
import json
import os
import sys
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


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=("backpressure-lag", "topic-missing"),
        default="backpressure-lag",
        help="Offline correlation fixture (default: backpressure-lag)",
    )
    parser.add_argument("--live", action="store_true", help="Poll live monitors")
    args = parser.parse_args()

    from examples.agents.react_cross_runbook_logic import build_cross_runbook
    from ratatoskr.correlation import correlate_signals, plan_cross_heals

    _banner(f"Cross-signal runbook demo: {args.scenario if not args.live else 'live'}")
    print("Principle: react_cross_runbook explains; workflow_cross_stack_heal mutates.")

    if args.live:
        from ratatoskr.kafka.client import KafkaClient
        from ratatoskr.kafka.policy import run_monitor_cycle as kafka_cycle
        from ratatoskr.nifi.client import NiFiClient
        from ratatoskr.nifi.policy import run_monitor_cycle as nifi_cycle

        os.environ.setdefault("NIFI_HEAL_PHASE", "monitor")
        os.environ.setdefault("KAFKA_HEAL_PHASE", "monitor")
        nifi = nifi_cycle(NiFiClient(), "root", phase="monitor")
        kafka = kafka_cycle(KafkaClient(), phase="monitor")
    elif args.scenario == "topic-missing":
        nifi, kafka = _demo_topic_missing()
    else:
        from examples.agents.run_workflow_signal_correlate_local import _demo_events

        nifi, kafka = _demo_events()

    _banner("1. Correlate")
    correlation = correlate_signals(nifi, kafka)
    _pp(
        "correlation",
        {
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
        },
    )

    _banner("2. react_cross_runbook")
    runbook = build_cross_runbook(correlation)
    rb = runbook.get("runbook") or {}
    rem = rb.get("remediation") or {}
    _pp(
        "runbook",
        {
            "mode": rb.get("mode"),
            "headline": rb.get("headline"),
            "situation": rb.get("situation"),
            "safe_options": rem.get("safe_options"),
            "lab_options": rem.get("lab_options"),
            "verify": rb.get("verify"),
            "mutations": runbook.get("mutations"),
            "source": runbook.get("source"),
        },
    )
    print("\nTalking points:")
    print("  • Inference/fallback built one checklist for both sides.")
    print("  • mutations=[] — apply with workflow_cross_stack_heal (CROSS_HEAL_PHASE=lab).")
    print("  • Or heal sides separately: NIFI_HEAL_PHASE / KAFKA_HEAL_PHASE.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
