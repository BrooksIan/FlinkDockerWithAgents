#!/usr/bin/env python3
"""Local runner for ``react_double_value``."""

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

    from examples.agents.react_double_value import ReactDoubleValueAgent

    env = AgentsExecutionEnvironment.get_execution_environment()
    input_data = [
        {"key": "1", "message": "Please double the input value 7"},
        {"key": "2", "message": "process value 21", "value": 21},
        {"key": "3", "value": 10},
    ]
    agent = ReactDoubleValueAgent()
    output_data = env.from_list(input_data).apply(agent).to_list()
    env.execute()

    print("React double value results:")
    for record in output_data:
        print(record)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
