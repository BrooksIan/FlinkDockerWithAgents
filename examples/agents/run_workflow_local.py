#!/usr/bin/env python3
"""Local runner for ``workflow_counter`` using AgentsExecutionEnvironment."""

from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap() -> None:
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def main() -> int:
    _bootstrap()
    from flink_agents.api.execution_environment import AgentsExecutionEnvironment

    from examples.agents.workflow_counter import CounterAgent

    env = AgentsExecutionEnvironment.get_execution_environment()
    input_data = [
        {"key": "1", "value": 3},
        {"key": "2", "value": 10},
        {"key": "3", "value": 21},
    ]
    agent = CounterAgent()
    output_data = env.from_list(input_data).apply(agent).to_list()
    env.execute()

    print("Workflow counter results:")
    for record in output_data:
        print(record)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
