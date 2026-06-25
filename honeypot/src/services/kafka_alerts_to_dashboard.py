"""
Kafka alerts -> cowrie-dashboard-data.json

Consumes JSON alerts from one or more Kafka topics and appends them to the dashboard
JSON file (Phase 2 ``cowrie.alerts`` and Phase 3 ``cowrie.react_alerts`` by default).

Set ``KAFKA_ALERTS_TOPICS`` (comma-separated) or legacy ``KAFKA_ALERTS_TOPIC`` (single).
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List


def _env(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default


def resolve_alert_topics() -> List[str]:
    """Topics to consume for dashboard JSON merge."""
    multi = _env("KAFKA_ALERTS_TOPICS", "")
    if multi:
        return [t.strip() for t in multi.split(",") if t.strip()]
    single = _env("KAFKA_ALERTS_TOPIC", "")
    if single:
        return [single]
    from cowrie_pipeline import TOPIC_ALERTS, TOPIC_REACT_ALERTS

    return [TOPIC_ALERTS, TOPIC_REACT_ALERTS]


def _load_existing(path: str) -> List[Any]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            data = json.loads(raw) if raw else []
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _append(path: str, alert: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    existing = _load_existing(path)
    existing.append(alert)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)


def main() -> None:
    from kafka import KafkaConsumer

    bootstrap = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topics = resolve_alert_topics()
    group_id = _env("KAFKA_ALERTS_GROUP_ID", "dashboard-writer")
    dashboard_path = _env("COWRIE_DASHBOARD_JSON", "/opt/flink/cowrie-dashboard-data.json")

    print(f"✅ Writing Kafka alerts from {topics} to {dashboard_path}")
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        auto_offset_reset=_env("KAFKA_AUTO_OFFSET_RESET", "latest"),
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8", errors="ignore"),
    )

    for msg in consumer:
        raw = msg.value.strip()
        if not raw:
            continue
        try:
            alert = json.loads(raw)
            if not isinstance(alert, dict):
                continue
            _append(dashboard_path, alert)
        except Exception:
            continue
        time.sleep(0.01)


if __name__ == "__main__":
    main()

