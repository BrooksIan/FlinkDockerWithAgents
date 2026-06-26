#!/usr/bin/env python3
"""Seed Kafka topics for testing workflow_counter in Agentic Studio."""

from __future__ import annotations

import sys
from pathlib import Path

INPUT_TOPIC = "workflow.test.input"
OUTPUT_TOPIC = "workflow.test.output"

DEFAULT_RECORDS = [
    {"key": "1", "value": 3},
    {"key": "2", "value": 10},
    {"key": "3", "value": 21},
    {"key": "4", "value": 7},
    {"key": "5", "value": 42},
]


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    from ratatoskr.kafka_sources import kafka_bootstrap_servers, publish_topic_records

    bootstrap = kafka_bootstrap_servers()
    count = publish_topic_records(INPUT_TOPIC, DEFAULT_RECORDS)
    print(f"Published {count} messages to {INPUT_TOPIC} (bootstrap={bootstrap})")
    print(f"Use sink topic {OUTPUT_TOPIC!r} in Studio (auto-created on first write)")
    print()
    print("Studio pipeline:")
    print(f"  Source: Kafka topic → {INPUT_TOPIC}")
    print("  Agent:  workflow_counter")
    print(f"  Sink:   Kafka topic → {OUTPUT_TOPIC}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
