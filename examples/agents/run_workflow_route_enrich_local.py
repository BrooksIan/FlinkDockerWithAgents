#!/usr/bin/env python3
"""Local runner for ``workflow_route_enrich``."""

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


def main() -> int:
    _bootstrap()
    os.environ.setdefault("ROUTE_PHASE", "monitor")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", default=None, help="monitor|safe|lab")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--rule-json",
        default="",
        help='Declarative rule JSON, e.g. {"match":{"type":"order"},"set":{"env":"lab"},"route":"enriched"}',
    )
    args = parser.parse_args()

    if args.phase:
        os.environ["ROUTE_PHASE"] = args.phase

    rule = None
    if args.rule_json.strip():
        rule = json.loads(args.rule_json)

    from ratatoskr.routing import run_route_enrich_cycle

    result = run_route_enrich_cycle(
        phase=args.phase,
        dry_run=True if args.dry_run else None,
        rule=rule,
    )
    print("Route / enrich results:")
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
