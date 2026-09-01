#!/usr/bin/env python3
"""Local / continuous runner for ``workflow_cm_monitor``.

Modes:
  - one-shot (default)
  - ``--continuous`` / ``--interval SEC`` host polling (``--count 0`` = forever)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def _bootstrap() -> None:
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def _one_cycle(*, previous_health: dict | None = None) -> dict:
    from ratatoskr.cm import CMClient, run_monitor_cycle
    from ratatoskr.cm.env import cm_cluster

    cluster = os.environ.get("CM_CLUSTER") or cm_cluster() or None
    client = CMClient(cluster=cluster or "")
    return run_monitor_cycle(client, cluster=cluster, previous_health=previous_health)


def _print_result(result: dict, *, label: str) -> None:
    print(label)
    print(json.dumps(result, indent=2, default=str))
    print("---", flush=True)


def _health_snapshot(result: dict) -> dict:
    h = result.get("health") or {}
    return {
        "severities": h.get("severities"),
        "bad_services": h.get("bad_services"),
        "stopped_roles": h.get("stopped_roles"),
        "failed_health_checks": h.get("failed_health_checks"),
        "bad_hosts": h.get("bad_hosts"),
        "critical_events": h.get("critical_events"),
    }


def _run_direct_loop(*, interval: float, count: int) -> int:
    n = 0
    previous = None
    while True:
        n += 1
        result = _one_cycle(previous_health=previous)
        previous = _health_snapshot(result)
        _print_result(
            result,
            label=f"CM monitor poll #{n} (interval={interval}s)",
        )
        if count > 0 and n >= count:
            return 0
        time.sleep(interval)


def main() -> int:
    _bootstrap()

    from ratatoskr.monitor_mode import (
        DEFAULT_MONITOR_INTERVAL_SEC,
        is_continuous,
        monitor_interval_sec,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Forever host polls (sets MONITOR_MODE=continuous; uses --interval).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help=f"Seconds between polls (default {DEFAULT_MONITOR_INTERVAL_SEC} when continuous).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of polls when interval > 0 (0 = forever until Ctrl-C).",
    )
    parser.add_argument(
        "--cluster",
        default="",
        help="Override CM_CLUSTER for this run.",
    )
    args = parser.parse_args()

    if args.cluster:
        os.environ["CM_CLUSTER"] = args.cluster

    if args.continuous or is_continuous():
        os.environ["MONITOR_MODE"] = "continuous"
        interval = (
            float(args.interval)
            if args.interval is not None and args.interval > 0
            else monitor_interval_sec(DEFAULT_MONITOR_INTERVAL_SEC)
        )
        return _run_direct_loop(interval=interval, count=args.count)

    if args.interval is not None and args.interval > 0:
        return _run_direct_loop(interval=args.interval, count=args.count)

    result = _one_cycle()
    _print_result(result, label="CM monitor results:")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
