#!/usr/bin/env python3
"""Local runner for ``workflow_replay``."""

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
    os.environ.setdefault("REPLAY_PHASE", "monitor")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", default=None, help="monitor|lab")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--source", default=None, help="Source topic (default events.valid)")
    parser.add_argument("--dest", default=None, help="Dest topic (default events.replay.out)")
    parser.add_argument("--hours", type=float, default=None, help="Replay window in hours")
    parser.add_argument("--group", default=None, help="Replay consumer group")
    args = parser.parse_args()

    if args.phase:
        os.environ["REPLAY_PHASE"] = args.phase

    from ratatoskr.replay import run_replay_cycle

    result = run_replay_cycle(
        phase=args.phase,
        dry_run=True if args.dry_run else None,
        source=args.source,
        dest=args.dest,
        hours=args.hours,
        group=args.group,
    )
    print("Replay results:")
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
