#!/usr/bin/env python3
"""Demo: propose → ack → apply on the dataplane approval bus."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        default="route",
        choices=("schema", "route", "replay"),
        help="route is safest live apply (property patch); schema lab swaps schema text",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    from ratatoskr.dataplane.bus import run_approval_cycle
    from ratatoskr.dataplane.flow import ensure_dataplane_topics

    ensure_dataplane_topics()
    result = run_approval_cycle(
        action="propose_ack_apply",
        target=args.target,
        dry_run=args.dry_run,
        phase_on_apply="safe" if args.target == "route" else "lab",
        rule={
            "match": {"type": "order"},
            "set": {"env": "approved", "pipeline": "dataplane"},
            "route": "enriched",
        }
        if args.target == "route"
        else None,
        hours=1.0 if args.target == "replay" else None,
    )
    print(json.dumps(result, indent=2, default=str))
    if result.get("error"):
        return 1
    print("OK: propose → ack → apply completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
