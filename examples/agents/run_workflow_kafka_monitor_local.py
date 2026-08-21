#!/usr/bin/env python3
"""Local / continuous runner for ``workflow_kafka_monitor``.

Modes:
  - one-shot (default)
  - ``--interval SEC`` continuous host polling (``--count 0`` = forever)
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


def _one_cycle() -> dict:
    from ratatoskr.kafka import KafkaClient, heal_phase, run_monitor_cycle

    phase = os.environ.get("KAFKA_HEAL_PHASE") or heal_phase()
    client = KafkaClient()
    try:
        return run_monitor_cycle(client, phase=phase)
    finally:
        client.close()


def _print_result(result: dict, *, label: str) -> None:
    print(label)
    print(json.dumps(result, indent=2, default=str))
    print("---", flush=True)


def main() -> int:
    _bootstrap()
    os.environ.setdefault("KAFKA_HEAL_PHASE", "monitor")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="Seconds between polls (0 = one-shot).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of polls when --interval > 0 (0 = forever until Ctrl-C).",
    )
    args = parser.parse_args()

    if args.interval > 0:
        n = 0
        previous = None
        while True:
            n += 1
            from ratatoskr.kafka import KafkaClient, heal_phase, run_monitor_cycle

            phase = os.environ.get("KAFKA_HEAL_PHASE") or heal_phase()
            client = KafkaClient()
            try:
                result = run_monitor_cycle(
                    client, phase=phase, previous_health=previous
                )
            finally:
                client.close()
            previous = {
                "severities": (result.get("health") or {}).get("severities"),
                "missing_topics": (result.get("health") or {}).get("missing_topics"),
                "lag_warn_groups": (result.get("health") or {}).get("lag_warn_groups"),
                "lag_crit_groups": (result.get("health") or {}).get("lag_crit_groups"),
                "stalled_groups": (result.get("health") or {}).get("stalled_groups"),
                "under_replicated_topics": (result.get("health") or {}).get(
                    "under_replicated_topics"
                ),
            }
            _print_result(
                result,
                label=f"Kafka monitor poll #{n} (interval={args.interval}s)",
            )
            if args.count > 0 and n >= args.count:
                return 0
            time.sleep(args.interval)

    result = _one_cycle()
    _print_result(result, label="Kafka monitor results:")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
