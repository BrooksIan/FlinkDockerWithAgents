#!/usr/bin/env python3
"""Local runner for ``workflow_schema_gate``."""

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
    os.environ.setdefault("SCHEMA_GATE_PHASE", "monitor")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        default=None,
        help="monitor|safe|lab (default SCHEMA_GATE_PHASE / DATAPLANE_PHASE)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--schema-text",
        default=None,
        help="Lab: desired JsonTreeReader schema text",
    )
    args = parser.parse_args()

    if args.phase:
        os.environ["SCHEMA_GATE_PHASE"] = args.phase

    from ratatoskr.schema import run_schema_gate_cycle

    result = run_schema_gate_cycle(
        phase=args.phase,
        dry_run=True if args.dry_run else None,
        desired_schema=args.schema_text,
    )
    print("Schema gate results:")
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
