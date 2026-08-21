#!/usr/bin/env python3
"""Cluster runner for ``workflow_nifi_monitor`` — PyFlink + Flink Agents operator.

Emits a burst of poll ticks (NIFI_MONITOR_POLLS, default 5) so the job finishes
in a demo-friendly way. For continuous host polling use the local runner with
``--interval``. For Kafka-triggered polls use ``--kafka-topic`` on the host runner
or publish ticks with ``scripts/nifi_publish_poll_ticks.py``.

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
    # Prefer compose DNS when running inside Flink containers.
    if Path("/opt/flink").is_dir() and not os.environ.get("NIFI_API_BASE"):
        os.environ["NIFI_API_BASE"] = "https://nifi:8443/nifi-api"
    os.environ.setdefault("NIFI_VERIFY_SSL", "false")

    from ratatoskr.runtime.flink_agents_bootstrap import patch_flink_agents_version

    patch_flink_agents_version()
    from pyflink.datastream import StreamExecutionEnvironment

    from flink_agents.api.execution_environment import AgentsExecutionEnvironment

    from examples.agents.workflow_nifi_monitor import NiFiMonitorAgent

    polls = max(1, int(os.environ.get("NIFI_MONITOR_POLLS", "5")))
    phase = os.environ.get("NIFI_HEAL_PHASE", "monitor")
    pg = os.environ.get("NIFI_PROCESS_GROUP_ID", "root")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    agents_env = AgentsExecutionEnvironment.get_execution_environment(env)

    records = [
        {
            "key": f"poll-{i}",
            "value": {"process_group_id": pg, "phase": phase, "tick": i},
        }
        for i in range(1, polls + 1)
    ]
    stream = env.from_collection(records)
    keyed = agents_env.from_datastream(
        input=stream,
        key_selector=lambda row: row["key"],
    )
    out = keyed.apply(NiFiMonitorAgent()).to_datastream()
    out.print()
    agents_env.execute(f"Ratatoskr NiFi Monitor ({polls} polls)")


if __name__ == "__main__":
    main()
