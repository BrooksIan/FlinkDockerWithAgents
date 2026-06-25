#!/usr/bin/env python3
"""
Ensure Cowrie pipeline Kafka topics exist at compose startup.

Flink Kafka sources resolve topic metadata when the job starts; if a topic does
not exist yet, the job fails even when ``KAFKA_AUTO_CREATE_TOPICS_ENABLE=true``
(broker auto-create only runs on produce). This script pre-creates all pipeline
topics and optionally re-checks on an interval.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Optional, Sequence

from cowrie_pipeline import ensure_kafka_topics, ensure_pipeline_kafka_topics, pipeline_kafka_topics


def ensure_topics_once(*, bootstrap: Optional[str] = None) -> list[str]:
    servers = (bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")).strip()
    topics = ensure_pipeline_kafka_topics(bootstrap=servers)
    print(f"Kafka topics ready on {servers}: {', '.join(topics)}", flush=True)
    return topics


def watch_topics(
    *,
    bootstrap: Optional[str] = None,
    interval_sec: float = 60.0,
) -> None:
    """Re-ensure topics periodically (no-op when they already exist)."""
    servers = (bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")).strip()
    wanted = pipeline_kafka_topics()
    print(
        f"Watching Kafka topics every {interval_sec}s on {servers}",
        flush=True,
    )
    while True:
        time.sleep(interval_sec)
        try:
            ensure_kafka_topics(wanted, bootstrap=servers)
        except Exception as exc:
            print(f"Kafka topic watchdog error: {exc}", flush=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Ensure topics once and exit (no watchdog)",
    )
    parser.add_argument(
        "--watch-interval",
        type=float,
        default=float(os.environ.get("COWRIE_KAFKA_TOPIC_WATCH_INTERVAL", "60")),
        help="Seconds between topic health checks (0 = ensure once and sleep forever)",
    )
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Kafka bootstrap servers",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    ensure_topics_once(bootstrap=args.bootstrap)

    if args.once:
        return 0

    interval = args.watch_interval
    if interval <= 0:
        print("Topic init complete — sleeping (watchdog disabled)", flush=True)
        while True:
            time.sleep(3600)
    watch_topics(bootstrap=args.bootstrap, interval_sec=interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
