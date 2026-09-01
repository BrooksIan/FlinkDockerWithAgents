#!/usr/bin/env python3
"""Tests for CM timeseries metric evaluation."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_evaluate_metric_checks_detects_hdfs_breach() -> None:
    from ratatoskr.cm.metrics import evaluate_metric_checks

    checks = [
        {
            "id": "hdfs_capacity_ratio",
            "severity": "HDFS_CAPACITY_HIGH",
            "service": "hdfs",
            "metric": "hdfs_capacity_used_ratio",
            "query": "SELECT dfs_capacity_used / dfs_capacity WHERE category = SERVICE",
            "threshold_key": "hdfs_capacity_pct",
            "scale": 100.0,
            "comparison": "gte",
        }
    ]

    def fetch(_query: str, _start: datetime, _end: datetime) -> dict:
        return {
            "items": [
                {
                    "timeSeries": [
                        {
                            "data": [
                                {
                                    "timestamp": "2026-01-01T10:00:00.000Z",
                                    "value": 0.91,
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    result = evaluate_metric_checks(
        checks,
        fetch_timeseries=fetch,
        thresholds={"hdfs_capacity_pct": 85.0},
    )
    assert "HDFS_CAPACITY_HIGH" in result["severities"]
    assert len(result["breaches"]) == 1
    assert result["breaches"][0]["scaled_value"] == 91.0


def test_metric_checks_for_cluster_discovers_services() -> None:
    from ratatoskr.cm.metrics import metric_checks_for_cluster

    checks = metric_checks_for_cluster(
        cluster="prod",
        services=[
            {"name": "hdfs", "type": "HDFS"},
            {"name": "kafka", "type": "KAFKA"},
        ],
    )
    ids = {c["id"] for c in checks}
    assert "hdfs_capacity_ratio" in ids
    assert "kafka_under_replicated" in ids


def test_client_snapshot_includes_metrics(monkeypatch) -> None:
    from unittest.mock import MagicMock, patch

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
                return_value=[{"name": "hdfs", "type": "HDFS", "healthSummary": "GOOD"}],
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
                                            with patch.object(
                                                client,
                                                "get_timeseries",
                                                return_value={
                                                    "items": [
                                                        {
                                                            "timeSeries": [
                                                                {
                                                                    "data": [
                                                                        {
                                                                            "timestamp": "2026-01-01T10:00:00.000Z",
                                                                            "value": 0.5,
                                                                        }
                                                                    ]
                                                                }
                                                            ]
                                                        }
                                                    ]
                                                },
                                            ):
                                                snap = client.get_cluster_health_snapshot("prod")
    assert "metrics" in snap
    assert isinstance(snap.get("metric_breaches"), list)
