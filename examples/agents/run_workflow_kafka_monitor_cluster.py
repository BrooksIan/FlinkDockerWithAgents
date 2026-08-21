#!/usr/bin/env python3
"""Cluster runner for ``workflow_kafka_monitor`` — PyFlink + Flink Agents operator.

Emits a burst of poll ticks (KAFKA_MONITOR_POLLS, default 5). For continuous
host polling use the local runner with ``--interval``.

Inside Flink containers, bootstrap defaults to host.docker.internal:9094
(Studio Kafka) unless KAFKA_BOOTSTRAP_SERVERS is set. Bootstrap is also
passed on each poll payload so TaskManagers do not rely on JM process env.
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

    from examples.agents.workflow_kafka_monitor import KafkaMonitorAgent
    from ratatoskr.kafka_sources import cluster_kafka_bootstrap_servers

    polls = max(1, int(os.environ.get("KAFKA_MONITOR_POLLS", "5")))
    phase = os.environ.get("KAFKA_HEAL_PHASE", "monitor")
    bootstrap = (os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or "").strip()
    if not bootstrap and Path("/opt/flink").is_dir():
        bootstrap = cluster_kafka_bootstrap_servers()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    agents_env = AgentsExecutionEnvironment.get_execution_environment(env)

    records = [
        {
            "key": f"poll-{i}",
            "value": {
                "phase": phase,
                "tick": i,
                **({"bootstrap": bootstrap} if bootstrap else {}),
            },
        }
        for i in range(1, polls + 1)
    ]
    stream = env.from_collection(records)
    keyed = agents_env.from_datastream(
        input=stream,
        key_selector=lambda row: row["key"],
    )
    out = keyed.apply(KafkaMonitorAgent()).to_datastream()
    out.print()
    agents_env.execute(f"Ratatoskr Kafka Monitor ({polls} polls)")


if __name__ == "__main__":
    main()
