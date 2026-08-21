#!/usr/bin/env python3
"""Gate tests for Kafka monitor heal phases (mocked broker — no live Kafka required)."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _health_fixture(**overrides: Any) -> dict[str, Any]:
    base = {
        "bootstrap": "localhost:9094",
        "healthy": False,
        "severities": ["TOPIC_MISSING", "LAG_WARN"],
        "probe": {"ok": True, "metadata_ms": 1.0, "poll_ms": 2.0},
        "missing_topics": [
            {
                "name": "workflow.test.input",
                "partitions": 1,
                "replication_factor": 1,
                "description": "Studio test input",
            },
            {
                "name": "kafka.monitor.output",
                "partitions": 1,
                "replication_factor": 1,
                "description": "monitor output",
            },
        ],
        "unexpected_topics": [],
        "topic_details": [],
        "under_replicated_topics": [],
        "offline_partitions": [],
        "consumer_groups": [
            {
                "group_id": "demo-group",
                "lag": 1500,
                "members": 1,
                "partitions": [
                    {
                        "topic": "workflow.test.input",
                        "partition": 0,
                        "committed": 0,
                        "end": 1500,
                        "lag": 1500,
                    }
                ],
            }
        ],
        "lag_warn_groups": [
            {
                "group_id": "demo-group",
                "lag": 1500,
                "members": 1,
                "partitions": [
                    {
                        "topic": "workflow.test.input",
                        "partition": 0,
                        "lag": 1500,
                    }
                ],
            }
        ],
        "lag_crit_groups": [
            {
                "group_id": "lab-reset-group",
                "lag": 20000,
                "members": 1,
                "partitions": [
                    {
                        "topic": "workflow.test.input",
                        "partition": 0,
                        "lag": 20000,
                    }
                ],
            }
        ],
        "stalled_groups": [],
        "empty_lagging_groups": [
            {
                "group_id": "dead-group",
                "lag": 100,
                "members": 0,
                "partitions": [],
            }
        ],
        "catalog": {},
        "counts": {"live_topics": 0, "catalog_topics": 2, "missing": 2},
    }
    base.update(overrides)
    return base


def _clear_env() -> None:
    for key in (
        "KAFKA_HEAL_PHASE",
        "KAFKA_HEAL_DRY_RUN",
        "KAFKA_HEAL_VERIFY",
        "KAFKA_HEAL_COOLDOWN_SEC",
        "KAFKA_HEAL_MAX_MUTATIONS",
        "KAFKA_HEAL_ALLOW_TOPICS",
        "KAFKA_HEAL_ALLOW_GROUPS",
        "KAFKA_HEAL_ALLOW_GROUP_PREFIXES",
        "KAFKA_HEAL_ALLOW_NAME_REGEX",
        "KAFKA_HEAL_ALLOW_INCREASE_PARTITIONS",
        "KAFKA_HEAL_ALLOW_RECREATE",
        "KAFKA_HEAL_OFFSET_STRATEGY",
        "KAFKA_LAG_WARN",
        "KAFKA_LAG_CRIT",
        "KAFKA_WATCH_PREFIXES",
        "KAFKA_FLAG_UNEXPECTED",
        "KAFKA_CATALOG",
        "KAFKA_TOPIC_PARTITIONS",
    ):
        os.environ.pop(key, None)


def test_classify_health() -> None:
    from ratatoskr.kafka.policy import classify_health

    ok = classify_health({"healthy": True, "severities": []})
    assert ok["level"] == "OK"
    assert ok["score"] == 100

    mid = classify_health(_health_fixture())
    assert mid["level"] == "MEDIUM"
    assert mid["score"] < 100


def test_diff_health() -> None:
    from ratatoskr.kafka.policy import diff_health

    prev = _health_fixture(
        missing_topics=[{"name": "a"}],
        lag_warn_groups=[],
        lag_crit_groups=[],
        stalled_groups=[],
        under_replicated_topics=[],
        severities=["TOPIC_MISSING"],
    )
    curr = _health_fixture(
        missing_topics=[{"name": "a"}, {"name": "b"}],
        lag_warn_groups=[],
        lag_crit_groups=[],
        stalled_groups=[],
        under_replicated_topics=[],
        severities=["TOPIC_MISSING"],
    )
    d = diff_health(prev, curr)
    assert d["new"]["missing_topics"] == ["b"]
    assert d["persistent"]["missing_topics"] == ["a"]


def test_monitor_no_mutations() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, reset_heal_cooldown, run_monitor_cycle

    _clear_env()
    reset_heal_cooldown()
    client = KafkaClient(bootstrap="localhost:9094")
    client.create_topic = MagicMock()  # type: ignore[method-assign]
    client.get_cluster_health_status = MagicMock(return_value=_health_fixture())  # type: ignore[method-assign]

    assert apply_heal_policy(client, _health_fixture(), phase="monitor") == []
    result = run_monitor_cycle(client, phase="monitor")
    assert result["phase"] == "monitor"
    assert result["heal_actions"] == []
    assert result["heal_plan"] == []
    assert result["poll_id"]
    assert result["ts"]
    assert "score" in result["classification"]
    client.create_topic.assert_not_called()


def test_safe_creates_missing_topics() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, build_heal_plan, reset_heal_cooldown

    _clear_env()
    reset_heal_cooldown()
    health = _health_fixture(lag_crit_groups=[], empty_lagging_groups=[])
    plan = build_heal_plan(health, phase="safe")
    assert all(p["op"] == "create_topic" for p in plan)
    assert len(plan) == 2

    client = KafkaClient(bootstrap="localhost:9094")
    client.create_topic = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="safe", verify=False)
    assert all(a.get("ok") for a in actions)
    assert client.create_topic.call_count == 2


def test_dry_run() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, reset_heal_cooldown

    _clear_env()
    reset_heal_cooldown()
    client = KafkaClient(bootstrap="localhost:9094")
    client.create_topic = MagicMock()  # type: ignore[method-assign]
    actions = apply_heal_policy(
        client,
        _health_fixture(lag_crit_groups=[], empty_lagging_groups=[]),
        phase="safe",
        dry_run=True,
    )
    assert actions
    assert all(a.get("proposed") is True and a.get("ok") is None for a in actions)
    client.create_topic.assert_not_called()


def test_blast_radius_and_cooldown() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, reset_heal_cooldown

    _clear_env()
    reset_heal_cooldown()
    os.environ["KAFKA_HEAL_MAX_MUTATIONS"] = "1"
    health = _health_fixture(lag_crit_groups=[], empty_lagging_groups=[])
    client = KafkaClient(bootstrap="localhost:9094")
    client.create_topic = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="safe", verify=False)
    assert sum(1 for a in actions if a.get("ok")) == 1
    assert any(a.get("skipped") == "blast_radius" for a in actions)

    reset_heal_cooldown()
    os.environ.pop("KAFKA_HEAL_MAX_MUTATIONS", None)
    os.environ["KAFKA_HEAL_COOLDOWN_SEC"] = "60"
    client2 = KafkaClient(bootstrap="localhost:9094")
    client2.create_topic = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    first = apply_heal_policy(client2, health, phase="safe", verify=False)
    assert any(a.get("ok") for a in first)
    second = apply_heal_policy(client2, health, phase="safe", verify=False)
    assert any(a.get("skipped") == "cooldown" for a in second)


def test_topic_allowlist() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, reset_heal_cooldown

    _clear_env()
    reset_heal_cooldown()
    os.environ["KAFKA_HEAL_ALLOW_TOPICS"] = "workflow.test.input"
    health = _health_fixture(lag_crit_groups=[], empty_lagging_groups=[])
    client = KafkaClient(bootstrap="localhost:9094")
    client.create_topic = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="safe", verify=False)
    ok = [a for a in actions if a.get("ok")]
    assert len(ok) == 1
    assert ok[0]["id"] == "workflow.test.input"
    assert any(a.get("skipped") == "allowlist" for a in actions)


def test_lab_group_ops_require_allowlist() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, reset_heal_cooldown

    _clear_env()
    reset_heal_cooldown()
    health = _health_fixture(missing_topics=[])
    client = KafkaClient(bootstrap="localhost:9094")
    client.reset_offsets = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.delete_consumer_group = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]

    denied = apply_heal_policy(client, health, phase="lab", verify=False)
    assert denied
    assert all(a.get("skipped") == "allowlist" for a in denied)
    client.reset_offsets.assert_not_called()

    os.environ["KAFKA_HEAL_ALLOW_GROUPS"] = "lab-reset-group,dead-group"
    allowed = apply_heal_policy(client, health, phase="lab", verify=False)
    ops = [a["op"] for a in allowed if a.get("ok")]
    assert "reset_offsets" in ops
    assert "delete_group" in ops
    client.reset_offsets.assert_called()
    assert client.reset_offsets.call_args.kwargs.get("strategy") == "latest"
    client.delete_consumer_group.assert_called()


def test_lab_group_prefix_allowlist() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, reset_heal_cooldown

    _clear_env()
    reset_heal_cooldown()
    os.environ["KAFKA_HEAL_ALLOW_GROUP_PREFIXES"] = "ratatoskr-"
    health = _health_fixture(
        missing_topics=[],
        lag_crit_groups=[],
        empty_lagging_groups=[
            {
                "group_id": "ratatoskr-kafka-fault-lab",
                "lag": 40,
                "members": 0,
                "partitions": [],
            }
        ],
    )
    client = KafkaClient(bootstrap="localhost:9094")
    client.delete_consumer_group = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="lab", verify=False)
    assert any(a.get("op") == "delete_group" and a.get("ok") for a in actions)
    client.delete_consumer_group.assert_called_once()


def test_lab_increase_partitions() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, build_heal_plan, reset_heal_cooldown

    _clear_env()
    reset_heal_cooldown()
    health = _health_fixture(
        missing_topics=[],
        lag_crit_groups=[],
        empty_lagging_groups=[],
        severities=["TOPIC_PARTITIONS_LOW"],
        undersized_topics=[
            {
                "name": "nifi.kafka.demo",
                "partition_count": 1,
                "desired_partitions": 3,
                "partitions": 3,
                "replication_factor": 1,
            }
        ],
    )
    plan = build_heal_plan(health, phase="lab")
    assert any(p["op"] == "increase_partitions" and p["partitions"] == 3 for p in plan)
    assert not any(p["op"] == "increase_partitions" for p in build_heal_plan(health, phase="safe"))

    client = KafkaClient(bootstrap="localhost:9094")
    client.increase_partitions = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="lab", verify=False)
    assert any(a.get("op") == "increase_partitions" and a.get("ok") for a in actions)
    client.increase_partitions.assert_called_once_with("nifi.kafka.demo", 3)


def test_lab_recreate_requires_flag() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, build_heal_plan, reset_heal_cooldown

    _clear_env()
    reset_heal_cooldown()
    health = _health_fixture(
        missing_topics=[],
        lag_crit_groups=[],
        empty_lagging_groups=[],
        severities=["TOPIC_PARTITIONS_HIGH"],
        oversized_topics=[
            {
                "name": "nifi.kafka.demo",
                "partition_count": 5,
                "desired_partitions": 1,
                "partitions": 1,
                "replication_factor": 1,
            }
        ],
    )
    assert not any(
        p["op"] == "recreate_topic" for p in build_heal_plan(health, phase="lab")
    )
    os.environ["KAFKA_HEAL_ALLOW_RECREATE"] = "1"
    plan = build_heal_plan(health, phase="lab")
    assert any(p["op"] == "recreate_topic" for p in plan)

    client = KafkaClient(bootstrap="localhost:9094")
    client.recreate_topic = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="lab", verify=False)
    assert any(a.get("op") == "recreate_topic" and a.get("ok") for a in actions)


def test_offset_strategy_earliest() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, reset_heal_cooldown

    _clear_env()
    reset_heal_cooldown()
    os.environ["KAFKA_HEAL_ALLOW_GROUPS"] = "lab-reset-group"
    os.environ["KAFKA_HEAL_OFFSET_STRATEGY"] = "earliest"
    health = _health_fixture(missing_topics=[], empty_lagging_groups=[])
    client = KafkaClient(bootstrap="localhost:9094")
    client.reset_offsets = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    apply_heal_policy(client, health, phase="lab", verify=False)
    assert client.reset_offsets.called
    assert client.reset_offsets.call_args.kwargs.get("strategy") == "earliest"


def test_verify_after_create() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import apply_heal_policy, reset_heal_cooldown

    _clear_env()
    reset_heal_cooldown()
    health = _health_fixture(
        missing_topics=[
            {
                "name": "workflow.test.input",
                "partitions": 1,
                "replication_factor": 1,
            }
        ],
        lag_crit_groups=[],
        empty_lagging_groups=[],
    )
    after = _health_fixture(
        missing_topics=[],
        lag_crit_groups=[],
        empty_lagging_groups=[],
        severities=[],
        healthy=True,
    )
    client = KafkaClient(bootstrap="localhost:9094")
    client.create_topic = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.get_cluster_health_status = MagicMock(return_value=after)  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="safe", verify=True)
    assert all(a.get("verified") is True for a in actions if a.get("ok"))


def test_cycle_refreshes_health_after_heal() -> None:
    from ratatoskr.kafka.client import KafkaClient
    from ratatoskr.kafka.policy import reset_heal_cooldown, run_monitor_cycle

    _clear_env()
    reset_heal_cooldown()
    before = _health_fixture(
        missing_topics=[
            {
                "name": "kafka.monitor.poll",
                "partitions": 1,
                "replication_factor": 1,
            }
        ],
        lag_crit_groups=[],
        empty_lagging_groups=[],
        lag_warn_groups=[],
        severities=["TOPIC_MISSING"],
    )
    after = _health_fixture(
        missing_topics=[],
        lag_crit_groups=[],
        empty_lagging_groups=[],
        lag_warn_groups=[],
        severities=[],
        healthy=True,
        counts={"live_topics": 9, "catalog_topics": 9, "missing": 0},
    )
    client = KafkaClient(bootstrap="localhost:9094")
    client.get_cluster_health_status = MagicMock(side_effect=[before, after, after])  # type: ignore[method-assign]
    client.create_topic = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]

    result = run_monitor_cycle(client, phase="safe", verify=True)
    assert result["classification"]["healthy"] is True
    assert result["classification"]["level"] == "OK"
    assert result["health"]["missing_topics"] == []
    assert result["delta"]["resolved"]["missing_topics"] == ["kafka.monitor.poll"]
    assert any(a.get("ok") for a in result["heal_actions"])


def test_agent_registered() -> None:
    from ratatoskr.agents.registry import load_agent_registry

    manifest = load_agent_registry(validate=False)
    assert "workflow_kafka_monitor" in manifest.agents


def test_cluster_script_registered() -> None:
    from pathlib import Path

    from ratatoskr.agents.registry import get_agent_spec

    root = Path(__file__).resolve().parents[1]
    spec = get_agent_spec("workflow_kafka_monitor")
    assert spec.cluster_script.endswith("run_workflow_kafka_monitor_cluster.py")
    assert (root / spec.cluster_script).is_file()


def test_fault_inject_helpers_importable() -> None:
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "scripts" / "kafka_fault_inject.py"
    spec = importlib.util.spec_from_file_location("kafka_fault_inject", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.DEFAULT_DEMO_TOPIC == "kafka.monitor.poll"
    assert callable(mod.inject_delete_topic)
    assert callable(mod.inject_lag_group)
    assert callable(mod.restore_catalog)


def test_canonical_catalog_studio_excludes_cowrie() -> None:
    from ratatoskr.kafka.client import canonical_topic_catalog

    os.environ.pop("KAFKA_CATALOG", None)
    catalog = canonical_topic_catalog()
    assert "kafka.monitor.poll" in catalog
    assert "kafka.monitor.output" in catalog
    assert "workflow.test.input" in catalog
    assert "cowrie.events" not in catalog


def test_canonical_catalog_full_includes_cowrie() -> None:
    from ratatoskr.kafka.client import canonical_topic_catalog

    os.environ["KAFKA_CATALOG"] = "full"
    try:
        catalog = canonical_topic_catalog()
        assert "cowrie.events" in catalog
        assert "kafka.monitor.poll" in catalog
    finally:
        os.environ.pop("KAFKA_CATALOG", None)

def test_env_helpers() -> None:
    from ratatoskr.kafka import env as kafka_env

    _clear_env()
    assert kafka_env.heal_phase() == "monitor"
    assert kafka_env.lag_warn_threshold() == 1000
    assert kafka_env.offset_reset_strategy() == "latest"
    assert kafka_env.allow_increase_partitions() is True
    assert kafka_env.allow_recreate_topic() is False
    os.environ["KAFKA_LAG_WARN"] = "50"
    os.environ["KAFKA_LAG_CRIT"] = "200"
    os.environ["KAFKA_HEAL_OFFSET_STRATEGY"] = "earliest"
    assert kafka_env.lag_warn_threshold() == 50
    assert kafka_env.lag_crit_threshold() == 200
    assert kafka_env.offset_reset_strategy() == "earliest"


def main() -> int:
    tests = [
        test_classify_health,
        test_diff_health,
        test_monitor_no_mutations,
        test_safe_creates_missing_topics,
        test_dry_run,
        test_blast_radius_and_cooldown,
        test_topic_allowlist,
        test_lab_group_ops_require_allowlist,
        test_lab_group_prefix_allowlist,
        test_lab_increase_partitions,
        test_lab_recreate_requires_flag,
        test_offset_strategy_earliest,
        test_verify_after_create,
        test_cycle_refreshes_health_after_heal,
        test_agent_registered,
        test_cluster_script_registered,
        test_fault_inject_helpers_importable,
        test_canonical_catalog_studio_excludes_cowrie,
        test_canonical_catalog_full_includes_cowrie,
        test_env_helpers,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"OK  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
        finally:
            _clear_env()
            try:
                from ratatoskr.kafka.policy import reset_heal_cooldown

                reset_heal_cooldown()
            except Exception:  # noqa: BLE001
                pass
    if failed:
        print(f"FAILED {failed}/{len(tests)}")
        return 1
    print(f"PASS ({len(tests)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
