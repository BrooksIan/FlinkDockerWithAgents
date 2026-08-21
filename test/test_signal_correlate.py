#!/usr/bin/env python3
"""Tests for NiFi↔Kafka correlation and incident scribe (no live brokers / LLM)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _nifi(**overrides):
    base = {
        "agent": "workflow_nifi_monitor",
        "poll_id": "n1",
        "classification": {
            "healthy": False,
            "level": "MEDIUM",
            "score": 70,
            "severities": ["BACKPRESSURE_CRIT", "BACKPRESSURE"],
            "summary": "BACKPRESSURE_CRIT",
        },
        "health": {
            "severities": ["BACKPRESSURE_CRIT", "BACKPRESSURE"],
            "queued_connections": [{"id": "c1"}],
            "stopped_processors": [],
        },
    }
    base.update(overrides)
    return base


def _kafka(**overrides):
    base = {
        "agent": "workflow_kafka_monitor",
        "poll_id": "k1",
        "classification": {
            "healthy": False,
            "level": "HIGH",
            "score": 60,
            "severities": ["LAG_CRIT"],
            "summary": "LAG_CRIT",
        },
        "health": {
            "severities": ["LAG_CRIT"],
            "lag_crit_groups": [{"group_id": "g1", "lag": 20000}],
            "missing_topics": [],
        },
    }
    base.update(overrides)
    return base


def test_correlate_backpressure_lag() -> None:
    from ratatoskr.correlation import correlate_signals

    result = correlate_signals(_nifi(), _kafka())
    assert "pipeline_backpressure_lag" in result["matched_rules"]
    assert result["classification"]["incident_count"] >= 1
    assert result["classification"]["level"] == "HIGH"
    assert result["incidents"][0]["rule"] == "pipeline_backpressure_lag"
    assert result["agent"] == "workflow_signal_correlate"


def test_correlate_healthy_no_incidents() -> None:
    from ratatoskr.correlation import correlate_signals

    nifi = _nifi(
        classification={
            "healthy": True,
            "level": "OK",
            "score": 100,
            "severities": [],
            "summary": "healthy",
        },
        health={"severities": [], "queued_connections": [], "stopped_processors": []},
    )
    kafka = _kafka(
        classification={
            "healthy": True,
            "level": "OK",
            "score": 100,
            "severities": [],
            "summary": "healthy",
        },
        health={"severities": [], "lag_crit_groups": [], "missing_topics": []},
    )
    result = correlate_signals(nifi, kafka)
    assert result["incidents"] == []
    assert result["classification"]["healthy"] is True
    assert result["classification"]["level"] == "OK"


def test_fallback_rule_only_when_no_specific() -> None:
    from ratatoskr.correlation import correlate_signals

    # STOPPED + TOPIC_MISSING → stack_degraded (no more specific rule)
    nifi = _nifi(
        classification={
            "healthy": False,
            "level": "MEDIUM",
            "score": 80,
            "severities": ["STOPPED"],
            "summary": "STOPPED",
        },
        health={"severities": ["STOPPED"], "stopped_processors": [{"id": "p"}], "queued_connections": []},
    )
    kafka = _kafka(
        classification={
            "healthy": False,
            "level": "MEDIUM",
            "score": 80,
            "severities": ["TOPIC_MISSING"],
            "summary": "TOPIC_MISSING",
        },
        health={
            "severities": ["TOPIC_MISSING"],
            "missing_topics": [{"name": "x"}],
            "lag_crit_groups": [],
        },
    )
    result = correlate_signals(nifi, kafka)
    assert result["matched_rules"] == ["stack_degraded"]


def test_solo_nifi_unreachable_summary() -> None:
    from ratatoskr.correlation import correlate_signals

    nifi = _nifi(
        classification={
            "healthy": False,
            "level": "HIGH",
            "score": 0,
            "severities": ["NIFI_UNREACHABLE"],
            "summary": "NIFI_UNREACHABLE",
        },
        health={"severities": ["NIFI_UNREACHABLE"]},
    )
    kafka = _kafka(
        classification={
            "healthy": True,
            "level": "OK",
            "score": 100,
            "severities": [],
            "summary": "healthy",
        },
        health={"severities": [], "lag_crit_groups": [], "missing_topics": []},
    )
    result = correlate_signals(nifi, kafka)
    assert result["incidents"] == []
    assert result["classification"]["cross_signal"] is False
    assert result["classification"]["summary"] == "nifi_only:NIFI_UNREACHABLE"


def test_scribe_fallback_no_llm() -> None:
    from examples.agents.react_incident_scribe_logic import scribe_incident
    from ratatoskr.correlation import correlate_signals

    correlation = correlate_signals(_nifi(), _kafka())
    out = scribe_incident(correlation)
    assert out["agent"] == "react_incident_scribe"
    assert out["mutations"] == []
    assert out["brief"]["mode"] == "fallback"
    assert out["brief"]["headline"]
    assert out["brief"]["suggested_next_steps"]


def test_scribe_healthy() -> None:
    from examples.agents.react_incident_scribe_logic import fallback_scribe

    brief = fallback_scribe(
        {
            "incidents": [],
            "classification": {"healthy": True, "level": "OK", "score": 100},
            "signals": {
                "nifi": {"classification": {"healthy": True}},
                "kafka": {"classification": {"healthy": True}},
            },
        }
    )
    assert "healthy" in brief["headline"].lower()


def test_agents_registered() -> None:
    from ratatoskr.agents.registry import load_agent_registry

    manifest = load_agent_registry(validate=False)
    assert "workflow_signal_correlate" in manifest.agents
    assert "react_incident_scribe" in manifest.agents


def main() -> int:
    tests = [
        test_correlate_backpressure_lag,
        test_correlate_healthy_no_incidents,
        test_fallback_rule_only_when_no_specific,
        test_solo_nifi_unreachable_summary,
        test_scribe_fallback_no_llm,
        test_scribe_healthy,
        test_agents_registered,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"OK  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        print(f"FAILED {failed}/{len(tests)}")
        return 1
    print(f"PASS ({len(tests)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
