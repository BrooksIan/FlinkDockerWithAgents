#!/usr/bin/env python3
"""Local runner for ``workflow_signal_correlate``.

Examples:
  python examples/agents/run_workflow_signal_correlate_local.py --live
  python examples/agents/run_workflow_signal_correlate_local.py --demo
  python examples/agents/run_workflow_signal_correlate_local.py --demo-dataplane
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


def _demo_events() -> tuple[dict, dict]:
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


def _demo_dataplane_events() -> tuple[dict, dict, dict, dict]:
    nifi, kafka = _demo_events()
    # Drop NiFi/Kafka pairing for a clean schema+lag story
    nifi = {
        "agent": "workflow_nifi_monitor",
        "poll_id": "demo-nifi-ok",
        "classification": {
            "healthy": True,
            "level": "OK",
            "score": 100,
            "severities": [],
        },
    }
    schema = {
        "agent": "workflow_schema_gate",
        "poll_id": "demo-schema",
        "classification": {
            "healthy": False,
            "level": "MEDIUM",
            "score": 70,
            "severities": ["SCHEMA_VIOLATIONS"],
            "violation_count": 12,
            "summary": "SCHEMA_VIOLATIONS",
        },
    }
    route = {
        "agent": "workflow_route_enrich",
        "poll_id": "demo-route",
        "classification": {
            "healthy": False,
            "level": "MEDIUM",
            "score": 65,
            "severities": ["ROUTE_DRIFT:EnrichUpdate"],
            "summary": "ROUTE_DRIFT:EnrichUpdate",
        },
    }
    return nifi, kafka, schema, route


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Use baked-in BACKPRESSURE + LAG_CRIT fixtures (no live brokers).",
    )
    parser.add_argument(
        "--demo-dataplane",
        action="store_true",
        help="Fixtures for schema_violation_spike + route_config_drift (+ lag).",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Poll live NiFi + Kafka + schema + route (default when not --demo*).",
    )
    args = parser.parse_args()

    from ratatoskr.correlation import run_correlate_cycle

    if args.demo_dataplane:
        nifi, kafka, schema, route = _demo_dataplane_events()
        result = run_correlate_cycle(
            nifi_event=nifi,
            kafka_event=kafka,
            schema_event=schema,
            route_event=route,
        )
    elif args.demo:
        nifi, kafka = _demo_events()
        result = run_correlate_cycle(nifi_event=nifi, kafka_event=kafka)
    else:
        result = run_correlate_cycle(poll_live=True)

    print("Signal correlate results:")
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
