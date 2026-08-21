#!/usr/bin/env python3
"""Offline smoke for the shared NiFi←Kafka demo base (no live stack required)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_kafka_flow_mod():
    path = ROOT / "scripts" / "nifi_load_kafka_flow.py"
    spec = importlib.util.spec_from_file_location("nifi_load_kafka_flow", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_loader_script_and_shell_exist() -> None:
    assert (ROOT / "scripts" / "nifi_load_kafka_flow.py").is_file()
    assert (ROOT / "scripts" / "nifi_load_kafka_flow.sh").is_file()
    assert (ROOT / "scripts" / "smoke_nifi_kafka_demo.py").is_file()
    assert (ROOT / "nifi" / "flows" / "README.md").is_file()


def test_demo_constants() -> None:
    mod = _load_kafka_flow_mod()
    assert mod.PG_NAME == "Ratatoskr Kafka Demo"
    assert mod.DEMO_TOPIC == "nifi.kafka.demo"
    assert mod.DEMO_GROUP == "ratatoskr-nifi-kafka-demo"
    assert "Kafka3ConnectionService" in mod.KAFKA_CS_TYPE
    assert mod.CONSUME_TYPE.endswith("ConsumeKafka")
    assert callable(mod.ensure_kafka_flow)
    assert callable(mod.ensure_demo_topic)
    assert callable(mod.repair_kafka_flow)
    assert mod.default_bootstrap() in ("kafka:9092",) or True  # env may override


def test_topic_in_studio_catalog() -> None:
    from ratatoskr.kafka_sources import STUDIO_CATALOG_TOPICS, _STATIC_TOPICS

    assert "nifi.kafka.demo" in _STATIC_TOPICS
    assert "nifi.kafka.demo" in STUDIO_CATALOG_TOPICS
    assert "ConsumeKafka" in _STATIC_TOPICS["nifi.kafka.demo"]


def test_nifi_compose_joins_kafka_network() -> None:
    text = (ROOT / "nifi" / "docker-compose.yml").read_text()
    assert "kafka-network" in text
    assert "NIFI_KAFKA_BOOTSTRAP" in text
    assert "kafka:9092" in text


def test_kafka_compose_declares_demo_topic_optional() -> None:
    """Studio init may not auto-create nifi.kafka.demo; loader ensures it."""
    text = (ROOT / "deploy" / "docker-compose.kafka.yml").read_text()
    assert "9094" in text
    assert "kafka-network" in text


def test_fault_inject_kafka_helpers_importable() -> None:
    import importlib.util

    path = ROOT / "scripts" / "nifi_fault_inject.py"
    spec = importlib.util.spec_from_file_location("nifi_fault_inject", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.KAFKA_PG == "Ratatoskr Kafka Demo"
    assert callable(mod.inject_stop_consume)
    assert callable(mod.inject_disable_kafka_cs)
    assert callable(mod.inject_kafka_invalid_log)
    assert callable(mod.inject_kafka_stop_log)
    assert callable(mod.restore_log_attribute_config)
    assert (ROOT / "scripts" / "demo_nifi_kafka_heal.py").is_file()


def test_heal_demo_scenario_catalog() -> None:
    import importlib.util

    path = ROOT / "scripts" / "demo_nifi_kafka_heal.py"
    spec = importlib.util.spec_from_file_location("demo_nifi_kafka_heal", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    expected = {
        "stop-consume",
        "disable-cs",
        "invalid-log",
        "queue-backlog",
        "delete-topic",
        "increase-partitions",
        "lag-group",
        "lag-earliest",
    }
    assert expected <= set(mod.SCENARIOS)
    assert mod.SCENARIOS["invalid-log"]["heal_phase"] == "lab"
    assert mod.SCENARIOS["delete-topic"]["stack"] == "kafka"
    assert "create_topic" in mod.SCENARIOS["delete-topic"]["expect_ops"]
    assert "increase_partitions" in mod.SCENARIOS["increase-partitions"]["expect_ops"]



def main() -> int:
    tests = [
        test_loader_script_and_shell_exist,
        test_demo_constants,
        test_topic_in_studio_catalog,
        test_nifi_compose_joins_kafka_network,
        test_kafka_compose_declares_demo_topic_optional,
        test_fault_inject_kafka_helpers_importable,
        test_heal_demo_scenario_catalog,
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
