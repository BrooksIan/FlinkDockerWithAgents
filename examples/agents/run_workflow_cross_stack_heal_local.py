#!/usr/bin/env python3
"""Local runner for ``workflow_cross_stack_heal``.

Examples:
  # Correlate only (plan playbooks, no mutations)
  python examples/agents/run_workflow_cross_stack_heal_local.py --demo

  # Live poll + lab heals (requires NiFi + Kafka + CROSS gates)
  CROSS_HEAL_PHASE=lab CROSS_HEAL_ALLOW_EMPTY_QUEUE=1 \\
    python examples/agents/run_workflow_cross_stack_heal_local.py --live --phase lab
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _bootstrap() -> None:
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def _demo_topic_missing() -> tuple[dict, dict]:
    nifi = {
        "agent": "workflow_nifi_monitor",
        "poll_id": "demo-nifi",
        "classification": {
            "healthy": False,
            "level": "MEDIUM",
            "score": 70,
            "severities": ["STOPPED"],
            "summary": "STOPPED",
        },
        "health": {
            "severities": ["STOPPED"],
            "stopped_processors": [{"id": "p1", "name": "ConsumeKafka"}],
            "queued_connections": [],
        },
    }
    kafka = {
        "agent": "workflow_kafka_monitor",
        "poll_id": "demo-kafka",
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
            "lag_crit_groups": [],
        },
    }
    return nifi, kafka


def _demo_backpressure_lag() -> tuple[dict, dict]:
    nifi = {
        "agent": "workflow_nifi_monitor",
        "poll_id": "demo-nifi",
        "classification": {
            "healthy": False,
            "level": "MEDIUM",
            "score": 70,
            "severities": ["BACKPRESSURE", "BACKPRESSURE_CRIT"],
            "summary": "BACKPRESSURE, BACKPRESSURE_CRIT",
        },
        "health": {
            "severities": ["BACKPRESSURE", "BACKPRESSURE_CRIT"],
            "queued_connections": [{"id": "c1", "flowFilesQueued": 500}],
            "stopped_processors": [],
        },
    }
    kafka = {
        "agent": "workflow_kafka_monitor",
        "poll_id": "demo-kafka",
        "classification": {
            "healthy": False,
            "level": "HIGH",
            "score": 60,
            "severities": ["LAG_CRIT"],
            "summary": "LAG_CRIT",
        },
        "health": {
            "severities": ["LAG_CRIT"],
            "lag_crit_groups": [{"group_id": "demo-group", "lag": 20000}],
            "missing_topics": [],
        },
    }
    return nifi, kafka


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--demo",
        choices=("topic-missing", "backpressure-lag"),
        nargs="?",
        const="topic-missing",
        help="Fixture pair (no live brokers). Default fixture: topic-missing.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Poll live NiFi + Kafka then correlate/heal.",
    )
    parser.add_argument(
        "--phase",
        choices=("monitor", "lab"),
        default=None,
        help="Override CROSS_HEAL_PHASE (monitor=plan only, lab=mutate).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Propose heal actions without applying.",
    )
    parser.add_argument(
        "--nifi-pg-id",
        default="root",
        help="NiFi process group id for live heal (default root).",
    )
    args = parser.parse_args()

    from ratatoskr.correlation import run_cross_stack_cycle

    if args.demo:
        nifi, kafka = (
            _demo_backpressure_lag()
            if args.demo == "backpressure-lag"
            else _demo_topic_missing()
        )
        result = run_cross_stack_cycle(
            nifi_event=nifi,
            kafka_event=kafka,
            phase=args.phase or "monitor",
            dry_run=True if args.dry_run else None,
            nifi_pg_id=args.nifi_pg_id,
        )
    else:
        result = run_cross_stack_cycle(
            poll_live=True,
            phase=args.phase,
            dry_run=True if args.dry_run else None,
            nifi_pg_id=args.nifi_pg_id,
        )

    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
