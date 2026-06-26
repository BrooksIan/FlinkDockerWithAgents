#!/usr/bin/env python3
"""Publish sample streaming records to workflow.test.input.

Each dataset uses a Kafka message key typical of real stream processing
(partition key / entity id) plus a JSON value payload.

Usage:
  ratatoskr kafka up
  python scripts/publish_workflow_kafka_data.py --list
  python scripts/publish_workflow_kafka_data.py --dataset iot
  python scripts/publish_workflow_kafka_data.py --dataset all
  python scripts/publish_workflow_kafka_data.py --dataset clickstream --delay 0.5
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any, Callable

INPUT_TOPIC = "workflow.test.input"

DatasetBuilder = Callable[[], list[dict[str, Any]]]


def _ts(offset_sec: int = 0) -> int:
    return int(time.time()) + offset_sec


def dataset_simple() -> list[dict[str, Any]]:
    """User-scoped counter values (workflow_counter demo)."""
    return [
        {"key": "user-1001", "value": 3},
        {"key": "user-1002", "value": 10},
        {"key": "user-1001", "value": 21},
        {"key": "user-1003", "value": 7},
        {"key": "user-1002", "value": 42},
    ]


def dataset_iot() -> list[dict[str, Any]]:
    """IoT telemetry keyed by device_id."""
    base = _ts()
    devices = [
        ("sensor-hvac-01", "temperature", 21.4, 48),
        ("sensor-hvac-01", "temperature", 21.8, 47),
        ("sensor-door-07", "contact", 0, None),
        ("sensor-pump-12", "pressure", 142.5, None),
        ("sensor-hvac-01", "temperature", 22.1, 46),
        ("sensor-pump-12", "pressure", 141.9, None),
    ]
    records: list[dict[str, Any]] = []
    for i, (device_id, sensor_type, reading, humidity) in enumerate(devices):
        payload: dict[str, Any] = {
            "device_id": device_id,
            "sensor_type": sensor_type,
            "reading": reading,
            "unit": "c" if sensor_type == "temperature" else ("psi" if sensor_type == "pressure" else "bool"),
            "timestamp": base + i,
        }
        if humidity is not None:
            payload["humidity_pct"] = humidity
        records.append({"key": device_id, "value": payload})
    return records


def dataset_clickstream() -> list[dict[str, Any]]:
    """Web analytics keyed by session_id."""
    base_ms = _ts() * 1000
    events = [
        ("sess-a1b2", "user-882", "/", "page_view"),
        ("sess-a1b2", "user-882", "/pricing", "page_view"),
        ("sess-a1b2", "user-882", "/pricing", "click_cta"),
        ("sess-c3d4", "user-441", "/blog/flink", "page_view"),
        ("sess-c3d4", "user-441", "/signup", "form_submit"),
        ("sess-e5f6", "user-119", "/docs", "page_view"),
    ]
    return [
        {
            "key": session_id,
            "value": {
                "session_id": session_id,
                "user_id": user_id,
                "page_url": url,
                "event_type": event_type,
                "ts_ms": base_ms + i * 250,
            },
        }
        for i, (session_id, user_id, url, event_type) in enumerate(events)
    ]


def dataset_orders() -> list[dict[str, Any]]:
    """E-commerce order line items keyed by customer_id."""
    base = _ts()
    lines = [
        ("cust-7712", "ord-9001", "SKU-WIDGET-A", 2, 29.99),
        ("cust-7712", "ord-9001", "SKU-CABLE-B", 1, 9.50),
        ("cust-3301", "ord-9002", "SKU-WIDGET-A", 1, 29.99),
        ("cust-9900", "ord-9003", "SKU-SUB-PRO", 1, 199.00),
        ("cust-3301", "ord-9004", "SKU-WIDGET-A", 3, 29.99),
    ]
    return [
        {
            "key": customer_id,
            "value": {
                "customer_id": customer_id,
                "order_id": order_id,
                "sku": sku,
                "quantity": qty,
                "amount_usd": round(qty * unit_price, 2),
                "timestamp": base + i,
            },
        }
        for i, (customer_id, order_id, sku, qty, unit_price) in enumerate(lines)
    ]


def dataset_api_metrics() -> list[dict[str, Any]]:
    """Service request metrics keyed by service name."""
    base = _ts()
    samples = [
        ("checkout-api", "/v1/cart", 200, 34),
        ("checkout-api", "/v1/cart", 200, 41),
        ("checkout-api", "/v1/pay", 502, 1203),
        ("inventory-api", "/v1/stock", 200, 18),
        ("inventory-api", "/v1/stock", 404, 12),
        ("auth-api", "/v1/token", 200, 89),
    ]
    return [
        {
            "key": service,
            "value": {
                "service": service,
                "endpoint": endpoint,
                "status_code": status,
                "latency_ms": latency,
                "timestamp": base + i,
            },
        }
        for i, (service, endpoint, status, latency) in enumerate(samples)
    ]


def dataset_security() -> list[dict[str, Any]]:
    """Auth / honeypot-style events keyed by src_ip."""
    base = _ts()
    events = [
        ("10.0.0.42", "cowrie.login.failed", "root", "sess-brute-1"),
        ("10.0.0.42", "cowrie.login.failed", "admin", "sess-brute-1"),
        ("10.0.0.42", "cowrie.login.failed", "root", "sess-brute-1"),
        ("10.0.0.99", "cowrie.command.input", "root", "sess-probe-1"),
        ("203.0.113.8", "cowrie.login.failed", "ubuntu", "sess-scan-2"),
    ]
    return [
        {
            "key": src_ip,
            "value": {
                "eventid": eventid,
                "src_ip": src_ip,
                "username": username,
                "session": session,
                "timestamp": base + i,
            },
        }
        for i, (src_ip, eventid, username, session) in enumerate(events)
    ]


DATASETS: dict[str, tuple[str, DatasetBuilder]] = {
    "simple": ("Integer values keyed by user_id (workflow_counter)", dataset_simple),
    "iot": ("IoT sensor readings keyed by device_id", dataset_iot),
    "clickstream": ("Web events keyed by session_id", dataset_clickstream),
    "orders": ("Order line items keyed by customer_id", dataset_orders),
    "api_metrics": ("HTTP metrics keyed by service name", dataset_api_metrics),
    "security": ("Login/command events keyed by src_ip", dataset_security),
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Publish streaming sample data to Kafka topic {INPUT_TOPIC!r}.",
    )
    parser.add_argument(
        "--dataset",
        "-d",
        default="simple",
        choices=[*DATASETS.keys(), "all"],
        help="Which sample dataset to publish (default: simple)",
    )
    parser.add_argument(
        "--topic",
        default=INPUT_TOPIC,
        help=f"Kafka topic (default: {INPUT_TOPIC})",
    )
    parser.add_argument(
        "--bootstrap",
        default=None,
        help="Kafka bootstrap servers (default: auto-detect via ratatoskr)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        metavar="SEC",
        help="Sleep between messages to simulate a live stream",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available datasets and exit",
    )
    return parser.parse_args(argv)


def _publish_with_delay(
    topic: str,
    records: list[dict[str, Any]],
    *,
    bootstrap: str | None,
    delay_sec: float,
) -> int:
    from ratatoskr.kafka_sources import publish_topic_records

    if delay_sec <= 0:
        return publish_topic_records(topic, records, bootstrap=bootstrap)

    published = 0
    for record in records:
        publish_topic_records(topic, [record], bootstrap=bootstrap)
        published += 1
        if published < len(records):
            time.sleep(delay_sec)
    return published


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    if args.list:
        print(f"Topic: {INPUT_TOPIC}\n")
        for name, (description, _) in DATASETS.items():
            print(f"  {name:14}  {description}")
        print(f"  {'all':14}  Publish every dataset in sequence")
        return 0

    repo = __import__("pathlib").Path(__file__).resolve().parents[1]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    from ratatoskr.kafka_sources import kafka_bootstrap_servers

    names = list(DATASETS.keys()) if args.dataset == "all" else [args.dataset]
    bootstrap = args.bootstrap or kafka_bootstrap_servers()
    total = 0

    for name in names:
        description, builder = DATASETS[name]
        records = builder()
        count = _publish_with_delay(
            args.topic,
            records,
            bootstrap=args.bootstrap,
            delay_sec=args.delay,
        )
        total += count
        print(f"[{name}] {description}")
        print(f"  published {count} message(s)")

    print()
    print(f"Total: {total} message(s) → {args.topic!r} (bootstrap={bootstrap})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
