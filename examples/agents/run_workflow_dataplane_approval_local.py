#!/usr/bin/env python3
"""Local runner for ``workflow_dataplane_approval``."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _bootstrap() -> None:
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--action",
        default="propose",
        choices=("propose", "ack", "apply", "propose_ack_apply"),
    )
    parser.add_argument(
        "--target",
        default="schema",
        choices=("schema", "route", "replay"),
    )
    parser.add_argument("--proposal-id", default=None)
    parser.add_argument("--nack", action="store_true", help="Ack with approved=false")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--phase-on-apply", default="lab")
    parser.add_argument("--hours", type=float, default=None)
    args = parser.parse_args()

    from ratatoskr.dataplane.bus import run_approval_cycle
    from ratatoskr.dataplane.flow import ensure_dataplane_topics

    ensure_dataplane_topics()
    result = run_approval_cycle(
        action=args.action,
        target=args.target,
        proposal_id=args.proposal_id,
        approved=not args.nack,
        dry_run=args.dry_run,
        phase_on_apply=args.phase_on_apply,
        hours=args.hours,
    )
    print("Dataplane approval results:")
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("error") is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
