#!/usr/bin/env python3
"""Gate tests for CM monitor (mocked CM API — no live Cloudera Manager required)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _health_fixture(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "cluster": "production",
        "healthy": False,
        "severities": ["ROLE_DOWN", "CONFIG_STALE", "HEALTH_CHECK_FAIL"],
        "probe": {"ok": True, "probe_ms": 120.0, "poll_ms": 450.0},
        "cluster_info": {
            "name": "production",
            "display_name": "Production",
            "health_summary": "CONCERNING",
        },
        "bad_services": [],
        "stopped_services": [],
        "stale_services": [
            {
                "cluster": "production",
                "service": "hdfs",
                "type": "HDFS",
                "health_summary": "GOOD",
                "service_state": "STARTED",
                "config_staleness": "STALE",
            }
        ],
        "stopped_roles": [
            {
                "cluster": "production",
                "service": "kafka",
                "role": "kafka-BROKER-2",
                "type": "BROKER",
                "role_state": "STOPPED",
                "health_summary": "BAD",
                "host_id": "host-12",
            }
        ],
        "failed_health_checks": [
            {
                "cluster": "production",
                "service": "hdfs",
                "name": "HDFS_SAFE_MODE",
                "summary": "BAD",
                "explanation": "NameNode is in safe mode",
            }
        ],
        "bad_hosts": [],
        "decommissioned_hosts": [],
        "critical_events": [],
        "parcel_errors": [],
        "failed_commands": [],
        "mgmt": {"health_summary": "GOOD", "service_state": "STARTED"},
        "counts": {
            "services": 5,
            "bad_services": 0,
            "stopped_roles": 1,
            "bad_hosts": 0,
            "critical_events": 0,
            "failed_commands": 0,
        },
    }
    base.update(overrides)
    return base


def test_classify_health_score_and_level() -> None:
    from ratatoskr.cm.policy import classify_health

    health = _health_fixture()
    result = classify_health(health)
    assert result["level"] == "HIGH"
    assert result["score"] < 100
    assert "ROLE_DOWN" in result["severities"]
    assert len(result["findings"]) >= 3


def test_build_recommendations_for_stopped_role_and_stale_config() -> None:
    from ratatoskr.cm.policy import build_recommendations, classify_health

    health = _health_fixture()
    classification = classify_health(health)
    recs = build_recommendations(health, classification)
    rule_ids = {r["rule_id"] for r in recs}
    assert "restart_stopped_role" in rule_ids
    assert "deploy_client_configs" in rule_ids
    assert "investigate_health_check" in rule_ids
    restart = next(r for r in recs if r["rule_id"] == "restart_stopped_role")
    assert "kafka-BROKER-2" in restart["summary"]
    assert restart.get("api_reference")
    assert restart.get("manual_steps")


def test_diff_health_tracks_new_and_resolved() -> None:
    from ratatoskr.cm.policy import diff_health

    previous = _health_fixture(
        severities=["CONFIG_STALE"],
        stopped_roles=[],
        failed_health_checks=[],
    )
    current = _health_fixture()
    delta = diff_health(previous, current)
    assert "ROLE_DOWN" in delta["severities_new"]
    assert delta["new_findings"]


def test_run_monitor_cycle_recommend_only() -> None:
    from ratatoskr.cm import CMClient, run_monitor_cycle

    client = MagicMock(spec=CMClient)
    client.cluster = "production"
    client.get_cluster_health_snapshot.return_value = _health_fixture()

    result = run_monitor_cycle(client, cluster="production")
    assert result["agent"] == "workflow_cm_monitor"
    assert result["audit"]["mode"] == "recommend_only"
    assert result["recommendations"]
    assert result["classification"]["level"] == "HIGH"
    client.get_cluster_health_snapshot.assert_called_once()


def test_unreachable_health_classifies_critical() -> None:
    from ratatoskr.cm.policy import classify_health, run_monitor_cycle

    client = MagicMock()
    client.cluster = "production"
    client.get_cluster_health_snapshot.return_value = {
        "cluster": "production",
        "healthy": False,
        "severities": ["CM_UNREACHABLE"],
        "probe": {"ok": False, "error": "HTTP 401"},
        "counts": {},
    }
    result = run_monitor_cycle(client, cluster="production")
    classification = classify_health(result["health"])
    assert classification["level"] == "HIGH"
    assert classification["score"] == 0


def test_client_knox_api_root() -> None:
    from ratatoskr.cm.client import CMClient
    from ratatoskr.cm.env import cm_knox_proxied

    base = "https://gw.example/worldwidebank/cdp-proxy-token/cm-api"
    assert cm_knox_proxied(base) is True
    client = CMClient(base_url=base, api_version="v49")
    assert client._api_root().endswith("/v49/")


def test_live_probe_requires_knox_or_basic(monkeypatch) -> None:
    import scripts.cm_monitor_live_probe as probe

    monkeypatch.delenv("KNOX_TOKEN", raising=False)
    monkeypatch.delenv("CM_PASSWORD", raising=False)
    monkeypatch.setenv("CM_API_BASE", "https://gw/cm-api")
    monkeypatch.setenv("CM_USER", "")
    assert probe.main() == 2


def test_discover_cluster_from_hosts() -> None:
    from unittest.mock import MagicMock

    from ratatoskr.cm.client import CMClient

    client = CMClient(base_url="https://gw/cm-api", cluster="")
    client.get_clusters = MagicMock(return_value=[])
    client.get_hosts = MagicMock(
        return_value=[
            {
                "hostId": "h1",
                "clusterRef": {"clusterName": "worldwidebank"},
            }
        ]
    )
    assert client.discover_cluster_name() == "worldwidebank"


def test_client_derives_severities_from_snapshot() -> None:
    from ratatoskr.cm.client import CMClient

    client = CMClient(base_url="https://cm.test:7183", cluster="prod")
    with patch.object(client, "probe", return_value={"ok": True, "probe_ms": 1.0}):
        with patch.object(
            client,
            "get_cluster",
            return_value={"name": "prod", "healthSummary": "GOOD"},
        ):
            with patch.object(
                client,
                "get_services",
                return_value=[
                    {
                        "name": "zookeeper",
                        "type": "ZOOKEEPER",
                        "healthSummary": "BAD",
                        "serviceState": "STARTED",
                        "configStalenessStatus": "FRESH",
                        "healthChecks": [],
                    }
                ],
            ):
                with patch.object(client, "get_roles", return_value=[]):
                    with patch.object(client, "get_hosts", return_value=[]):
                        with patch.object(client, "_get", return_value={"ok": True, "data": {"items": []}}):
                            with patch.object(client, "get_events", return_value=[]):
                                with patch.object(client, "get_parcels", return_value=[]):
                                    with patch.object(
                                        client,
                                        "get_mgmt_service",
                                        return_value={"healthSummary": "GOOD"},
                                    ):
                                        with patch.object(client, "get_active_commands", return_value=[]):
                                            snap = client.get_cluster_health_snapshot("prod")
    assert "SERVICE_BAD" in snap["severities"]
    assert snap["bad_services"][0]["service"] == "zookeeper"


def test_process_cm_events_suppresses_and_groups() -> None:
    from ratatoskr.cm.events import process_cm_events

    raw = [
        {
            "id": "1",
            "severity": "IMPORTANT",
            "content": "zookeeper.ssl.keyStore.location not specified on host-a",
            "timeOccurred": "2026-01-01T10:00:00Z",
        },
        {
            "id": "2",
            "severity": "IMPORTANT",
            "content": "zookeeper.ssl.keyStore.location not specified on host-b",
            "timeOccurred": "2026-01-01T11:00:00Z",
        },
        {
            "id": "3",
            "severity": "IMPORTANT",
            "content": "Must authenticate with SPNEGO to access Impala metrics",
            "timeOccurred": "2026-01-01T09:00:00Z",
        },
        {
            "id": "4",
            "severity": "IMPORTANT",
            "content": "Must authenticate with SPNEGO to access Impala metrics",
            "timeOccurred": "2026-01-01T12:00:00Z",
        },
        {
            "id": "5",
            "severity": "IMPORTANT",
            "content": "impala_IMPALA_SERVICE_STATE_FETCHER failed on host-3",
            "timeOccurred": "2026-01-01T08:00:00Z",
        },
    ]
    result = process_cm_events(raw)
    assert result["suppressed_events"] == 2
    assert result["raw_event_count"] == 5
    assert len(result["critical_events"]) == 2
    assert len(result["event_warnings"]) == 0

    by_kind = {e["event_kind"]: e for e in result["critical_events"]}
    assert by_kind["impala_spnego"]["count"] == 2
    assert by_kind["impala_state_fetcher"]["count"] == 1


def test_build_recommendations_cdp_event_kinds() -> None:
    from ratatoskr.cm.policy import build_recommendations, classify_health

    health = _health_fixture(
        severities=["EVENT_CRITICAL", "CM_SLOW"],
        critical_events=[
            {
                "fingerprint": "impala_spnego",
                "event_kind": "impala_spnego",
                "content": "Must authenticate with SPNEGO",
                "count": 12,
                "first_seen": "2026-01-01T09:00:00Z",
                "last_seen": "2026-01-01T12:00:00Z",
                "service_hint": "impala",
            },
            {
                "fingerprint": "impala_state_fetcher",
                "event_kind": "impala_state_fetcher",
                "content": "impala_IMPALA_SERVICE_STATE_FETCHER failed",
                "count": 3,
                "first_seen": "2026-01-01T08:00:00Z",
                "last_seen": "2026-01-01T10:00:00Z",
                "service_hint": "impala",
            },
        ],
    )
    classification = classify_health(health)
    recs = build_recommendations(health, classification)
    rule_ids = {r["rule_id"] for r in recs}
    assert "impala_spnego_auth" in rule_ids
    assert "impala_state_fetcher_failure" in rule_ids
    assert "review_critical_event" not in rule_ids
    assert len(recs) == 2


def test_build_recommendations_event_warnings() -> None:
    from ratatoskr.cm.policy import build_recommendations

    health = _health_fixture(
        severities=["EVENT_WARN"],
        critical_events=[],
        event_warnings=[
            {
                "fingerprint": "abc123",
                "content": "Some transient warning",
                "count": 4,
                "first_seen": "2026-01-01T09:00:00Z",
                "last_seen": "2026-01-01T12:00:00Z",
            }
        ],
    )
    recs = build_recommendations(health)
    assert any(r["rule_id"] == "review_event_warning" for r in recs)
    warn = next(r for r in recs if r["rule_id"] == "review_event_warning")
    assert "4x" in warn["summary"]


if __name__ == "__main__":
    test_classify_health_score_and_level()
    test_build_recommendations_for_stopped_role_and_stale_config()
    test_diff_health_tracks_new_and_resolved()
    test_run_monitor_cycle_recommend_only()
    test_unreachable_health_classifies_critical()
    test_process_cm_events_suppresses_and_groups()
    test_build_recommendations_cdp_event_kinds()
    test_build_recommendations_event_warnings()
    test_client_derives_severities_from_snapshot()
    print("cm monitor tests: ok")
