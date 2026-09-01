#!/usr/bin/env python3
"""Tests for react_cm_runbook (explain-only)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_cm_runbook_from_fixture() -> None:
    from examples.agents.react_cm_runbook_logic import build_runbook
    from ratatoskr.nifi.runbook.schema import is_valid_runbook_event

    monitor = {
        "agent": "workflow_cm_monitor",
        "poll_id": "test-poll",
        "classification": {
            "level": "HIGH",
            "score": 70,
            "severities": ["EVENT_CRITICAL"],
            "healthy": False,
        },
        "health": {
            "cluster": "worldwidebank",
            "severities": ["EVENT_CRITICAL"],
            "suppressed_events": 5,
        },
        "recommendations": [
            {
                "rule_id": "impala_spnego_auth",
                "priority": "high",
                "summary": "Impala metrics auth failure (SPNEGO) — 3 event(s)",
                "manual_steps": ["Check Kerberos config"],
            }
        ],
    }
    out = build_runbook(monitor)
    assert out["agent"] == "react_cm_runbook"
    assert out["mutations"] == []
    assert is_valid_runbook_event(out)
    assert "SPNEGO" in out["runbook"]["headline"] or "EVENT_CRITICAL" in out["runbook"]["situation"]
    assert out["runbook"]["diagnostic_steps"]


def test_cm_runbook_healthy_cluster() -> None:
    from examples.agents.react_cm_runbook_logic import build_runbook

    monitor = {
        "agent": "workflow_cm_monitor",
        "classification": {"level": "OK", "score": 100, "severities": [], "healthy": True},
        "health": {"cluster": "prod", "severities": [], "suppressed_events": 10},
        "recommendations": [],
    }
    out = build_runbook(monitor)
    assert "healthy" in out["runbook"]["headline"].lower()
    assert out["source"]["suppressed_events"] == 10


def test_cm_console_base_knox(monkeypatch) -> None:
    from ratatoskr.cm.env import cm_console_base

    monkeypatch.setenv(
        "CM_API_BASE",
        "https://gw.example/worldwidebank/cdp-proxy-token/cm-api",
    )
    monkeypatch.delenv("CM_CONSOLE_BASE", raising=False)
    assert cm_console_base() == "https://gw.example/worldwidebank/cdp-proxy/cmf"


def test_recommendation_console_url_knox(monkeypatch) -> None:
    from ratatoskr.cm.policy import build_recommendations, classify_health

    monkeypatch.setenv(
        "CM_API_BASE",
        "https://gw.example/worldwidebank/cdp-proxy-token/cm-api",
    )
    monkeypatch.delenv("CM_CONSOLE_BASE", raising=False)

    health = {
        "cluster": "worldwidebank",
        "severities": ["EVENT_CRITICAL"],
        "critical_events": [
            {
                "fingerprint": "impala_spnego",
                "event_kind": "impala_spnego",
                "content": "Must authenticate with SPNEGO",
                "count": 1,
                "first_seen": "2026-01-01T09:00:00Z",
                "last_seen": "2026-01-01T09:00:00Z",
            }
        ],
    }
    recs = build_recommendations(health, classify_health(health))
    impala = next(r for r in recs if r["rule_id"] == "impala_spnego_auth")
    assert "/cdp-proxy/cmf/" in impala["console_url"]
    assert "/cdp-proxy-token/cm-api" not in impala["console_url"]


if __name__ == "__main__":
    test_cm_runbook_from_fixture()
    test_cm_runbook_healthy_cluster()
    test_cm_console_base_knox()
    test_recommendation_console_url_knox()
    print("cm runbook tests: ok")
