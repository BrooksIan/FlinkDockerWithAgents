#!/usr/bin/env python3
"""Local runner for ``react_cm_runbook`` (explain-only).

Default: built-in fixture with Impala SPNEGO grouped events.
``--live``: one ``workflow_cm_monitor`` poll then runbook.
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


def _fixture_impala_events() -> dict:
    from ratatoskr.cm.policy import build_recommendations, classify_health

    health = {
        "cluster": "worldwidebank",
        "healthy": False,
        "severities": ["EVENT_CRITICAL", "CM_SLOW"],
        "probe": {"ok": True, "probe_ms": 120.0, "poll_ms": 6200.0, "slow": True},
        "cluster_info": {"name": "worldwidebank", "health_summary": "GOOD"},
        "critical_events": [
            {
                "fingerprint": "impala_spnego",
                "event_kind": "impala_spnego",
                "content": "Must authenticate with SPNEGO to access Impala metrics",
                "count": 12,
                "first_seen": "2026-01-01T09:00:00Z",
                "last_seen": "2026-01-01T12:00:00Z",
                "service_hint": "impala",
            }
        ],
        "event_warnings": [],
        "suppressed_events": 13,
        "counts": {"critical_events": 1, "suppressed_events": 13},
    }
    classification = classify_health(health)
    return {
        "agent": "workflow_cm_monitor",
        "poll_id": "fixture-impala-spnego",
        "classification": classification,
        "health": health,
        "recommendations": build_recommendations(health, classification),
        "audit": {"mode": "recommend_only"},
    }


def _monitor_from_live() -> dict:
    try:
        from dotenv import load_dotenv

        load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    except ImportError:
        pass

    from ratatoskr.cm import CMClient, run_monitor_cycle

    return run_monitor_cycle(CMClient())


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description="react_cm_runbook local runner")
    parser.add_argument(
        "--fixture",
        default="impala-spnego",
        choices=["impala-spnego"],
        help="Fixture id when not --live",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Poll live CM via workflow_cm_monitor cycle",
    )
    args = parser.parse_args()

    if args.live:
        monitor = _monitor_from_live()
        print("CM monitor (live):")
    elif args.fixture == "impala-spnego":
        monitor = _fixture_impala_events()
        print("CM monitor fixture=impala-spnego:")
    else:
        print(f"Unknown fixture: {args.fixture}", file=sys.stderr)
        return 2

    print(json.dumps(monitor, indent=2, default=str))
    print("---")

    from examples.agents.react_cm_runbook_logic import build_runbook

    out = build_runbook(monitor)
    print("CM runbook:")
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
