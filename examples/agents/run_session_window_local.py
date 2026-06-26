#!/usr/bin/env python3
"""Local runner for ``session_detect`` on pre-windowed session batches."""

from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap() -> None:
    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))


def _run_with_flink_agents() -> list[dict]:
    from flink_agents.api.execution_environment import AgentsExecutionEnvironment

    from examples.agents.session_detect import SessionDetectAgent
    from examples.agents.session_window_fixtures import demo_session_summaries

    env = AgentsExecutionEnvironment.get_execution_environment()
    output_data = env.from_list(demo_session_summaries()).apply(SessionDetectAgent()).to_list()
    env.execute()
    return output_data


def _run_host_fallback() -> list[dict]:
    from examples.agents.session_detect_logic import process_session_summary
    from examples.agents.session_window_fixtures import demo_session_summaries

    print(
        "Note: flink_agents is not installed on the host. "
        "Running session_detect_logic directly (same rules as the agent).\n"
        "For the full AgentsExecutionEnvironment path: ratatoskr build && "
        "ratatoskr agent submit session_detect\n"
    )
    return [process_session_summary(summary) for summary in demo_session_summaries()]


def main() -> int:
    _bootstrap()
    try:
        output_data = _run_with_flink_agents()
    except ModuleNotFoundError as exc:
        if exc.name != "flink_agents":
            raise
        output_data = _run_host_fallback()

    print("Session detect results:")
    for record in output_data:
        print(record)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
