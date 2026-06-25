#!/usr/bin/env python3
"""
Unified compose supervisor for the Cowrie Kafka + Flink pipeline.

Replaces separate ``kafka-topic-init``, ``kafka-normalizer``, ``kafka-actor-classifier``,
and ``kafka-workflow-processor`` sidecars. On startup it:

1. Ensures all pipeline Kafka topics exist
2. Submits Phase 1 / 1.5 / 2 Flink jobs (visible on JobManager :8081)
3. Watchdog-loops to re-create missing topics or Flink jobs after restarts
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Optional, Sequence

from cowrie_flink_jobs_startup import (
    PHASE_JOB_NAMES,
    PHASE_ORDER,
    _needs_resubmit,
    ensure_all_phases,
    ensure_phase,
    watch_phases,
)
from cowrie_kafka_topics_startup import ensure_topics_once
from cowrie_pipeline import ensure_kafka_topics, pipeline_kafka_topics


def _env_float(name: str, default: str) -> float:
    return float(os.environ.get(name, default))


def bootstrap_pipeline(*, max_attempts: int = 5) -> None:
    """Topics first, then Flink jobs 1 → 1.5 → 2."""
    ensure_topics_once()
    ensure_all_phases(max_attempts=max_attempts)
    print("Flink pipeline supervisor: topics + Phase 1/1.5/2 OK", flush=True)


def watch_pipeline(
    *,
    topic_interval_sec: float,
    flink_interval_sec: float,
    max_attempts: int = 5,
) -> None:
    """Periodic topic + Flink health checks."""
    if topic_interval_sec <= 0 and flink_interval_sec <= 0:
        print("Pipeline supervisor idle (watchdog disabled)", flush=True)
        while True:
            time.sleep(3600)

    topics = pipeline_kafka_topics()
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").strip()
    next_topic = 0.0
    next_flink = 0.0
    print(
        f"Watching pipeline: topics every {topic_interval_sec}s, "
        f"Flink every {flink_interval_sec}s",
        flush=True,
    )

    while True:
        now = time.time()
        if topic_interval_sec > 0 and now >= next_topic:
            try:
                ensure_kafka_topics(topics, bootstrap=bootstrap)
            except Exception as exc:
                print(f"Topic watchdog error: {exc}", flush=True)
            next_topic = now + topic_interval_sec

        if flink_interval_sec > 0 and now >= next_flink:
            for phase in PHASE_ORDER:
                job_name = PHASE_JOB_NAMES[phase]
                if _needs_resubmit(job_name):
                    try:
                        ensure_phase(phase, max_attempts=max_attempts)
                    except Exception as exc:
                        print(f"Flink watchdog failed for phase {phase}: {exc}", flush=True)
            next_flink = now + flink_interval_sec

        time.sleep(1.0)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Bootstrap topics + Flink jobs once and exit",
    )
    parser.add_argument(
        "--topic-watch-interval",
        type=float,
        default=_env_float("COWRIE_KAFKA_TOPIC_WATCH_INTERVAL", "60"),
        help="Topic watchdog interval (0 disables)",
    )
    parser.add_argument(
        "--flink-watch-interval",
        type=float,
        default=_env_float("COWRIE_FLINK_WATCH_INTERVAL", "30"),
        help="Flink job watchdog interval (0 disables)",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=int(os.environ.get("COWRIE_FLINK_SUBMIT_MAX_ATTEMPTS", "5")),
        help="Submit retries per ensure call",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    bootstrap_pipeline(max_attempts=args.max_attempts)

    if args.once:
        return 0

    if args.flink_watch_interval > 0 and args.topic_watch_interval <= 0:
        watch_phases(PHASE_ORDER, interval_sec=args.flink_watch_interval, max_attempts=args.max_attempts)
        return 0

    watch_pipeline(
        topic_interval_sec=args.topic_watch_interval,
        flink_interval_sec=args.flink_watch_interval,
        max_attempts=args.max_attempts,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
