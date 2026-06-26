#!/usr/bin/env python3
"""Local runner for ``react_skills_demo`` (native Flink chat model + skills)."""

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

    from examples.agents.react_skills_demo import ReactSkillsDemoAgent

    env = AgentsExecutionEnvironment.get_execution_environment()
    input_data = [
        {"key": "1", "message": "What is (2 + 3) * 4?"},
        {"key": "2", "message": "Compute 2 ^ 10."},
        {"key": "3", "message": "What is 144 divided by 12?"},
    ]
    agent = ReactSkillsDemoAgent()
    output_data = env.from_list(input_data).apply(agent).to_list()
    env.execute()

    print("React skills demo results:")
    for record in output_data:
        print(record)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
