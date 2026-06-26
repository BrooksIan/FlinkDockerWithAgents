#!/usr/bin/env python3
"""Cluster runner for ``react_skills_demo``."""

from __future__ import annotations

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
    from pyflink.datastream import StreamExecutionEnvironment

    from flink_agents.api.core_options import AgentExecutionOptions
    from flink_agents.api.execution_environment import AgentsExecutionEnvironment

    from examples.agents.react_skills_demo import ReactSkillsDemoAgent

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    agents_env = AgentsExecutionEnvironment.get_execution_environment(env)
    agents_env.get_config().set(AgentExecutionOptions.NUM_ASYNC_THREADS, 2)

    records = [
        {"key": "1", "message": "What is (2 + 3) * 4?"},
        {"key": "2", "message": "Compute 2 ^ 10."},
    ]
    stream = env.from_collection(records)
    keyed = agents_env.from_datastream(
        input=stream,
        key_selector=lambda row: row["key"],
    )
    out = keyed.apply(ReactSkillsDemoAgent()).to_datastream()
    out.print()
    agents_env.execute("Ratatoskr React Skills Demo")


if __name__ == "__main__":
    main()
