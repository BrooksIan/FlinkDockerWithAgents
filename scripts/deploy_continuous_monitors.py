#!/usr/bin/env python3
"""Deploy continuous NiFi + Kafka monitor agents as two Flink jobs.

Submits ``workflow_nifi_monitor`` and ``workflow_kafka_monitor`` separately
(MONITOR_MODE=continuous, in-job interval ticks). Healing is gated by
``--phase`` (same agents — not separate heal jobs).

Prereqs:
  ratatoskr kafka up
  ratatoskr up --profile nifi   # Flink + NiFi

Examples:
  python3 scripts/deploy_continuous_monitors.py
  python3 scripts/deploy_continuous_monitors.py --phase safe --interval 10
  python3 scripts/deploy_continuous_monitors.py --status
  python3 scripts/deploy_continuous_monitors.py --stop
"""

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
    from ratatoskr.constants import NIFI_PROFILE
    from ratatoskr.monitor_mode import DEFAULT_MONITOR_INTERVAL_SEC
    from ratatoskr.monitor_runtime import (
        start_cluster_monitors,
        status_dict,
        stop_monitors,
    )

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--phase",
        default="monitor",
        choices=("monitor", "safe", "lab"),
        help="Heal phase for both jobs (default: monitor = observe only)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_MONITOR_INTERVAL_SEC,
        help=f"Seconds between polls (default {DEFAULT_MONITOR_INTERVAL_SEC})",
    )
    parser.add_argument(
        "--profile",
        default=NIFI_PROFILE,
        help="Compose profile for JobManager (default: nifi)",
    )
    parser.add_argument(
        "--nifi-only",
        action="store_true",
        help="Deploy only workflow_nifi_monitor",
    )
    parser.add_argument(
        "--kafka-only",
        action="store_true",
        help="Deploy only workflow_kafka_monitor",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show tracked continuous monitor jobs and exit",
    )
    parser.add_argument(
        "--stop",
        action="store_true",
        help="Cancel tracked Flink monitor jobs and exit",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Machine-readable status / deploy result",
    )
    args = parser.parse_args()

    if args.nifi_only and args.kafka_only:
        print("Use only one of --nifi-only / --kafka-only", file=sys.stderr)
        return 2

    if args.status:
        info = status_dict()
        if args.json:
            print(json.dumps(info, indent=2))
        elif not info.get("running"):
            print("Continuous monitors: off")
        else:
            print(
                f"Continuous monitors: on  mode={info.get('mode')}  "
                f"phase={info.get('phase')}  interval={info.get('interval')}s"
            )
            for proc in info.get("processes") or []:
                print(
                    f"  {proc.get('agent'):28} job={proc.get('flink_job_id')}  "
                    f"state={proc.get('flink_state')}"
                )
        return 0

    if args.stop:
        result = stop_monitors()
        if args.json:
            print(json.dumps(result, indent=2))
        elif result.get("mode") is None:
            print("No continuous monitors were running.")
        else:
            jobs = result.get("canceled_jobs") or []
            pids = result.get("stopped_pids") or []
            if jobs:
                print(f"Canceled {len(jobs)} Flink job(s): {', '.join(jobs)}")
            if pids:
                print(f"Stopped {len(pids)} host process(es): {pids}")
            if not jobs and not pids:
                print("Cleared monitor state.")
        return 0

    nifi = not args.kafka_only
    kafka = not args.nifi_only
    try:
        state = start_cluster_monitors(
            nifi=nifi,
            kafka=kafka,
            interval=args.interval,
            phase=args.phase,
            profile=args.profile,
        )
    except (RuntimeError, ValueError, FileNotFoundError) as exc:
        print(f"FAIL  {exc}", file=sys.stderr)
        return 1

    payload = {
        "mode": "cluster",
        "phase": state.phase,
        "interval": state.interval,
        "profile": state.profile,
        "jobs": [
            {
                "agent": p.agent,
                "flink_job_id": p.flink_job_id,
                "key": p.key,
            }
            for p in state.processes
        ],
    }
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print("Deployed continuous Flink monitor jobs:")
        print(f"  phase={state.phase}  interval={state.interval}s  profile={state.profile}")
        for p in state.processes:
            print(f"  {p.agent:28} job={p.flink_job_id or '(pending)'}")
        print()
        print("Flink UI: http://localhost:8082")
        print("Status:   python3 scripts/deploy_continuous_monitors.py --status")
        print("Stop:     python3 scripts/deploy_continuous_monitors.py --stop")
        print("  (or)    ratatoskr monitor stop")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
