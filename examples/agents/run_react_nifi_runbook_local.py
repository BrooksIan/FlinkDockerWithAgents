#!/usr/bin/env python3
"""Local runner for ``react_nifi_runbook`` (explain-only).

Default: fixture ``stop-generate`` → runbook (no NiFi / LLM required).
``--live``: one ``workflow_nifi_monitor`` poll then runbook.
``--fixture ID``: use a Phase 0 fixture pack.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _bootstrap() -> None:
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def _monitor_from_live() -> dict:
    from ratatoskr.nifi.client import NiFiClient, heal_phase
    from ratatoskr.nifi.policy import run_monitor_cycle

    phase = os.environ.get("NIFI_HEAL_PHASE") or heal_phase() or "monitor"
    pg = os.environ.get("NIFI_PROCESS_GROUP_ID", "root")
    return run_monitor_cycle(NiFiClient(), pg, phase=phase)


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description="react_nifi_runbook local runner")
    parser.add_argument(
        "--fixture",
        default="stop-generate",
        help="Fixture id when not --live (default: stop-generate)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Poll live NiFi via workflow_nifi_monitor cycle",
    )
    parser.add_argument(
        "--list-fixtures",
        action="store_true",
        help="List fixture ids and exit",
    )
    args = parser.parse_args()

    from ratatoskr.nifi.runbook import list_fixture_ids, load_fixture

    if args.list_fixtures:
        print("\n".join(list_fixture_ids()))
        return 0

    if args.live:
        monitor = _monitor_from_live()
        print("NiFi monitor (live):")
        print(json.dumps(monitor, indent=2, default=str))
        print("---")
    else:
        monitor = load_fixture(args.fixture)
        print(f"NiFi monitor fixture={args.fixture!r}:")
        print(json.dumps(monitor, indent=2, default=str))
        print("---")

    from examples.agents.react_nifi_runbook_logic import build_runbook

    out = build_runbook(monitor)
    print("NiFi runbook:")
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
