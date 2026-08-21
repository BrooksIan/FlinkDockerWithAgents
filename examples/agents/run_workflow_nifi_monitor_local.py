#!/usr/bin/env python3
"""Local runner for ``workflow_nifi_monitor``.

Prefers Flink Agents local runner when ``flink_agents`` is installed (inside the
Docker image / cluster). On the host venv — where ``flink_agents`` is usually
absent — falls back to a direct NiFi poll via ``ratatoskr.nifi.policy``.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _bootstrap() -> None:
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def _run_direct() -> int:
    from ratatoskr.nifi.client import NiFiClient, heal_phase
    from ratatoskr.nifi.policy import run_monitor_cycle

    phase = os.environ.get("NIFI_HEAL_PHASE") or heal_phase()
    pg = os.environ.get("NIFI_PROCESS_GROUP_ID", "root")
    client = NiFiClient()
    result = run_monitor_cycle(client, pg, phase=phase)
    print("NiFi monitor results (direct host runner — flink_agents not on PATH):")
    print(json.dumps(result, indent=2, default=str))
    return 0


def _run_flink_agents() -> int:
    from flink_agents.api.execution_environment import AgentsExecutionEnvironment

    from examples.agents.workflow_nifi_monitor import NiFiMonitorAgent

    env = AgentsExecutionEnvironment.get_execution_environment()
    input_data = [
        {
            "key": "poll-1",
            "value": {
                "process_group_id": os.environ.get("NIFI_PROCESS_GROUP_ID", "root"),
                "phase": os.environ["NIFI_HEAL_PHASE"],
            },
        },
    ]
    agent = NiFiMonitorAgent()
    output_data = env.from_list(input_data).apply(agent).to_list()
    env.execute()

    print("NiFi monitor results (Flink Agents local runner):")
    for record in output_data:
        print(record)
    return 0


def main() -> int:
    _bootstrap()
    os.environ.setdefault("NIFI_HEAL_PHASE", "monitor")

    try:
        import flink_agents  # noqa: F401
    except ImportError:
        return _run_direct()
    return _run_flink_agents()


if __name__ == "__main__":
    raise SystemExit(main())
