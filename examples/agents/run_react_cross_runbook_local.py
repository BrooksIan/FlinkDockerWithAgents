#!/usr/bin/env python3
"""Local runner for ``react_cross_runbook`` (explain-only).

Default: demo correlation (BACKPRESSURE + LAG) → cross runbook.
``--live``: poll NiFi + Kafka monitors then correlate.
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


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description="react_cross_runbook local runner")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Poll live NiFi + Kafka monitors then correlate",
    )
    args = parser.parse_args()

    from examples.agents.react_cross_runbook_logic import build_cross_runbook
    from ratatoskr.correlation import correlate_signals

    if args.live:
        from ratatoskr.kafka.client import KafkaClient
        from ratatoskr.kafka.policy import run_monitor_cycle as kafka_cycle
        from ratatoskr.nifi.client import NiFiClient
        from ratatoskr.nifi.policy import run_monitor_cycle as nifi_cycle

        os.environ.setdefault("NIFI_HEAL_PHASE", "monitor")
        os.environ.setdefault("KAFKA_HEAL_PHASE", "monitor")
        nifi = nifi_cycle(NiFiClient(), os.environ.get("NIFI_PROCESS_GROUP_ID", "root"), phase="monitor")
        kafka = kafka_cycle(KafkaClient(), phase="monitor")
        correlation = correlate_signals(nifi, kafka)
        print("Correlation (live):")
    else:
        from examples.agents.run_workflow_signal_correlate_local import _demo_events

        nifi, kafka = _demo_events()
        correlation = correlate_signals(nifi, kafka)
        print("Correlation (demo):")

    print(json.dumps(correlation, indent=2, default=str))
    print("---")
    out = build_cross_runbook(correlation)
    print("Cross-signal runbook:")
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
