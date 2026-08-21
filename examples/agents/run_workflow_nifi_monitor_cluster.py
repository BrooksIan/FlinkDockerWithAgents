#!/usr/bin/env python3
"""Cluster runner for ``workflow_nifi_monitor`` — PyFlink + Flink Agents operator.

Modes:
  - **oneshot / burst** (default): ``NIFI_MONITOR_POLLS`` finite collection (default 5)
  - **continuous**: ``MONITOR_MODE=continuous`` or ``NIFI_MONITOR_POLLS=0`` —
    unbounded in-job interval ticks (default). Optional Kafka ticks via
    ``MONITOR_CONTINUOUS_SOURCE=kafka`` + ``scripts/publish_monitor_poll_ticks.py``.

Inside the Flink container, NiFi is reached at https://nifi:8443/nifi-api unless
NIFI_API_BASE is set.
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
    os.environ.setdefault("NIFI_HEAL_PHASE", "monitor")
    if Path("/opt/flink").is_dir() and not os.environ.get("NIFI_API_BASE"):
        os.environ["NIFI_API_BASE"] = "https://nifi:8443/nifi-api"
    os.environ.setdefault("NIFI_VERIFY_SSL", "false")

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
    from examples.agents.workflow_nifi_monitor import NiFiMonitorAgent
    from ratatoskr.monitor_mode import (
        DEFAULT_MONITOR_INTERVAL_SEC,
        monitor_interval_sec,
        nifi_monitor_polls,
        nifi_poll_topic,
    )

    phase = os.environ.get("NIFI_HEAL_PHASE", "monitor")
    pg = os.environ.get("NIFI_PROCESS_GROUP_ID", "root")
    polls = nifi_monitor_polls()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    agents_env = AgentsExecutionEnvironment.get_execution_environment(env)

    defaults = {"process_group_id": pg, "phase": phase}
    if polls is None:
        if use_kafka_ticks():
            bootstrap = resolve_cluster_bootstrap()
            topic = nifi_poll_topic()
            stream = kafka_tick_stream(
                env,
                topic=topic,
                bootstrap=bootstrap,
                group_id=os.environ.get(
                    "NIFI_MONITOR_KAFKA_GROUP", "ratatoskr-nifi-monitor-cluster"
                ),
                defaults=defaults,
            )
            job_name = f"Ratatoskr NiFi Monitor (continuous kafka @ {topic})"
        else:
            interval = monitor_interval_sec(DEFAULT_MONITOR_INTERVAL_SEC)
            stream = interval_tick_stream(
                env, interval_sec=interval, defaults=defaults
            )
            job_name = f"Ratatoskr NiFi Monitor (continuous every {interval:g}s)"
    else:
        records = [
            {
                "key": f"poll-{i}",
                "value": {**defaults, "tick": i},
            }
            for i in range(1, polls + 1)
        ]
        stream = env.from_collection(records)
        job_name = f"Ratatoskr NiFi Monitor ({polls} polls)"

    keyed = agents_env.from_datastream(
        input=stream,
        key_selector=lambda row: row["key"],
    )
    out = keyed.apply(NiFiMonitorAgent()).to_datastream()
    out.print()
    agents_env.execute(job_name)


if __name__ == "__main__":
    main()
