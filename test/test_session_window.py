#!/usr/bin/env python3
"""Session window policy and agent manifest tests."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def test_session_gap_ms_varies_by_eventid() -> None:
    from examples.agents.session_window_policy import (
        GAP_MS_COMMAND,
        GAP_MS_DEFAULT,
        GAP_MS_FAILED_LOGIN,
        session_gap_ms,
    )

    assert session_gap_ms({"eventid": "cowrie.login.failed"}) == GAP_MS_FAILED_LOGIN
    assert session_gap_ms({"eventid": "cowrie.command.input"}) == GAP_MS_COMMAND
    assert session_gap_ms({"eventid": "cowrie.version"}) == GAP_MS_DEFAULT


def test_classify_session_brute_force_critical() -> None:
    from examples.agents.session_window_fixtures import demo_session_summaries
    from examples.agents.session_window_policy import SEVERITY_CRITICAL, classify_session

    summaries = demo_session_summaries()
    brute = next(s for s in summaries if s["src_ip"] == "10.0.0.42")
    assert classify_session(brute) == SEVERITY_CRITICAL


def test_classify_session_probe_low() -> None:
    from examples.agents.session_window_fixtures import demo_session_summaries
    from examples.agents.session_window_policy import SEVERITY_LOW, classify_session

    summaries = demo_session_summaries()
    probe = next(s for s in summaries if s["src_ip"] == "10.0.0.99")
    assert classify_session(probe) == SEVERITY_LOW


def test_summarize_session_shape() -> None:
    from examples.agents.session_window_fixtures import demo_session_events
    from examples.agents.session_window_policy import summarize_session

    events = [e for e in demo_session_events() if e["src_ip"] == "10.0.0.42"]
    summary = summarize_session("10.0.0.42", events)
    assert summary["key"] == "10.0.0.42"
    assert summary["event_count"] == 5
    assert len(summary["events"]) == 5
    assert summary["first_ts"] <= summary["last_ts"]


def test_session_detect_in_manifest() -> None:
    from apemosyne.agents.registry import load_agent_registry

    registry = load_agent_registry()
    spec = registry.agents["session_detect"]
    assert spec.type == "workflow"
    assert spec.class_name == "SessionDetectAgent"
    assert spec.cluster_script.endswith("run_session_window_cluster.py")


def test_session_detect_agent_local() -> None:
    pytest = __import__("pytest")
    try:
        from flink_agents.api.execution_environment import AgentsExecutionEnvironment
    except ImportError:
        pytest.skip("flink_agents not installed")

    from examples.agents.session_detect import SessionDetectAgent
    from examples.agents.session_window_fixtures import demo_session_summaries
    from examples.agents.session_window_policy import SEVERITY_CRITICAL, SEVERITY_LOW

    env = AgentsExecutionEnvironment.get_execution_environment()
    output = env.from_list(demo_session_summaries()).apply(SessionDetectAgent()).to_list()
    env.execute()

    by_ip = {row.get("src_ip"): row for row in output}
    assert by_ip["10.0.0.42"]["severity"] == SEVERITY_CRITICAL
    assert by_ip["10.0.0.42"]["response_actions"]
    assert by_ip["10.0.0.99"]["severity"] == SEVERITY_LOW
    assert not by_ip["10.0.0.99"]["response_actions"]
