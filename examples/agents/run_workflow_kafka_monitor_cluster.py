#!/usr/bin/env python3
"""Cluster runner for ``workflow_kafka_monitor`` — PyFlink + Flink Agents operator.

Modes:
  - **oneshot / burst** (default): ``KAFKA_MONITOR_POLLS`` finite collection (default 5)
  - **continuous**: unbounded in-job interval ticks (default). Optional Kafka ticks via
    ``MONITOR_CONTINUOUS_SOURCE=kafka``.

Inside Flink containers, bootstrap defaults to host.docker.internal:9094
(Studio Kafka) unless KAFKA_BOOTSTRAP_SERVERS is set — used by the *agent*
to probe Kafka, not necessarily by the tick source.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _bootstrap() -> None:
    root = Path("/opt/flink")
    if root.is_dir():
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        return
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def main() -> None:
    _bootstrap()
    os.environ.setdefault("KAFKA_HEAL_PHASE", "monitor")
    os.environ.setdefault("KAFKA_CATALOG", "studio")

    from ratatoskr.runtime.flink_agents_bootstrap import patch_flink_agents_version

    patch_flink_agents_version()
    from pyflink.datastream import StreamExecutionEnvironment

    from flink_agents.api.execution_environment import AgentsExecutionEnvironment

    from examples.agents.monitor_cluster_source import (
        interval_tick_stream,
        kafka_tick_stream,
        resolve_cluster_bootstrap,
        use_kafka_ticks,
    )
    from examples.agents.workflow_kafka_monitor import KafkaMonitorAgent
    from ratatoskr.monitor_mode import (
        DEFAULT_MONITOR_INTERVAL_SEC,
        kafka_monitor_polls,
        kafka_poll_topic,
        monitor_interval_sec,
    )

    phase = os.environ.get("KAFKA_HEAL_PHASE", "monitor")
    bootstrap = resolve_cluster_bootstrap()
    # Prefer EXTERNAL for host.docker.internal when INTERNAL kafka DNS missing
    if bootstrap.startswith("kafka:") and Path("/opt/flink").is_dir():
        try:
            import socket

            socket.getaddrinfo("kafka", 9092)
        except OSError:
            from ratatoskr.kafka_sources import cluster_kafka_bootstrap_servers

            bootstrap = cluster_kafka_bootstrap_servers()
    polls = kafka_monitor_polls()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    agents_env = AgentsExecutionEnvironment.get_execution_environment(env)

    defaults = {
        "phase": phase,
        **({"bootstrap": bootstrap} if bootstrap else {}),
    }
    if polls is None:
        if use_kafka_ticks():
            topic = kafka_poll_topic()
            stream = kafka_tick_stream(
                env,
                topic=topic,
                bootstrap=bootstrap,
                group_id=os.environ.get(
                    "KAFKA_MONITOR_KAFKA_GROUP", "ratatoskr-kafka-monitor-cluster"
                ),
                defaults=defaults,
            )
            job_name = f"Ratatoskr Kafka Monitor (continuous kafka @ {topic})"
        else:
            interval = monitor_interval_sec(DEFAULT_MONITOR_INTERVAL_SEC)
            stream = interval_tick_stream(
                env, interval_sec=interval, defaults=defaults
            )
            job_name = f"Ratatoskr Kafka Monitor (continuous every {interval:g}s)"
    else:
        records = [
            {
                "key": f"poll-{i}",
                "value": {**defaults, "tick": i},
            }
            for i in range(1, polls + 1)
        ]
        stream = env.from_collection(records)
        job_name = f"Ratatoskr Kafka Monitor ({polls} polls)"

    keyed = agents_env.from_datastream(
        input=stream,
        key_selector=lambda row: row["key"],
    )
    out = keyed.apply(KafkaMonitorAgent()).to_datastream()
    out.print()
    agents_env.execute(job_name)


if __name__ == "__main__":
    main()
