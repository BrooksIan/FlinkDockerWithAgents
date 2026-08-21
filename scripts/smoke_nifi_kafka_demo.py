#!/usr/bin/env python3
"""Live smoke: Studio Kafka → NiFi ConsumeKafka demo path.

Requires:
  ratatoskr kafka up
  ratatoskr up --profile nifi
  (flow may already exist; this script ensures / repairs it)

  python3 scripts/smoke_nifi_kafka_demo.py
  python3 scripts/smoke_nifi_kafka_demo.py --repair
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _bootstrap() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))


def _load_mod(name: str, rel: str):
    path = ROOT / rel
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fail(msg: str) -> int:
    print(f"FAIL  {msg}")
    return 1


def _ok(msg: str) -> None:
    print(f"OK    {msg}")


def _pg_snapshot(client, pg_id: str) -> dict:
    procs = {}
    for ent in client.list_processors(pg_id):
        comp = ent.get("component") or {}
        name = comp.get("name") or ""
        procs[name] = {
            "id": comp.get("id"),
            "state": comp.get("state"),
            "validationStatus": comp.get("validationStatus"),
        }
    services = {}
    for ent in client.get_controller_services(pg_id):
        comp = ent.get("component") or {}
        name = comp.get("name") or ""
        services[name] = {
            "id": comp.get("id"),
            "state": comp.get("state"),
            "validationStatus": comp.get("validationStatus"),
        }
    return {"processors": procs, "services": services}


def run(*, repair: bool, count: int, wait_sec: float) -> int:
    _bootstrap()
    from ratatoskr.kafka_sources import kafka_reachable
    from ratatoskr.nifi.client import NiFiClient

    flow_mod = _load_mod("nifi_load_kafka_flow", "scripts/nifi_load_kafka_flow.py")
    sample_mod = _load_mod("nifi_load_sample_flow", "scripts/nifi_load_sample_flow.py")

    print("=" * 60)
    print("NiFi ← Kafka demo smoke")
    print("=" * 60)

    if not kafka_reachable():
        return _fail("Studio Kafka not reachable at localhost:9094 — run: ratatoskr kafka up")
    _ok("Kafka reachable (localhost:9094)")

    client = NiFiClient()
    try:
        sample_mod.wait_ready(client, attempts=12, delay=2.0)
    except Exception as exc:  # noqa: BLE001
        return _fail(f"NiFi API not ready: {exc}")
    _ok("NiFi API authenticated")

    topic_info = flow_mod.ensure_demo_topic()
    _ok(f"topic {flow_mod.DEMO_TOPIC} ensure={topic_info}")

    try:
        result = flow_mod.ensure_kafka_flow(client, repair=repair)
    except Exception as exc:  # noqa: BLE001
        return _fail(f"ensure_kafka_flow: {exc}")
    pg_id = result.get("process_group_id")
    if not pg_id:
        return _fail(f"no process_group_id in {result}")
    _ok(
        f"flow PG={pg_id} created={result.get('created')} "
        f"repaired={result.get('repaired', False)}"
    )

    # If ensure returned existing without starting, optionally repair to RUNNING.
    snap = _pg_snapshot(client, pg_id)
    need_start = any(
        (snap["processors"].get(n) or {}).get("state") != "RUNNING"
        for n in ("ConsumeKafka", "UpdateAttribute", "LogAttribute")
    ) or (snap["services"].get("Studio Kafka") or {}).get("state") != "ENABLED"
    if need_start and not repair:
        _ok("processors not all RUNNING — repairing")
        result = flow_mod.repair_kafka_flow(
            client, pg_id, bootstrap=flow_mod.default_bootstrap()
        )
        snap = _pg_snapshot(client, pg_id)

    for name in ("ConsumeKafka", "UpdateAttribute", "LogAttribute"):
        p = snap["processors"].get(name)
        if not p:
            return _fail(f"missing processor {name}")
        if p.get("state") != "RUNNING":
            return _fail(f"{name} state={p.get('state')} (want RUNNING)")
        if p.get("validationStatus") not in (None, "VALID"):
            return _fail(f"{name} validationStatus={p.get('validationStatus')}")
        _ok(f"{name} RUNNING VALID")

    cs = snap["services"].get("Studio Kafka")
    if not cs:
        return _fail("missing controller service Studio Kafka")
    if cs.get("state") != "ENABLED":
        return _fail(f"Studio Kafka state={cs.get('state')} (want ENABLED)")
    _ok("Studio Kafka ENABLED")

    marker = f"smoke-{uuid.uuid4().hex[:12]}"
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(bootstrap_servers="localhost:9094")
        for i in range(count):
            payload = json.dumps(
                {"hello": "nifi", "smoke": marker, "i": i}
            ).encode()
            producer.send(flow_mod.DEMO_TOPIC, payload)
        producer.flush()
        producer.close()
    except Exception as exc:  # noqa: BLE001
        return _fail(f"publish: {exc}")
    _ok(f"published {count} message(s) marker={marker}")

    time.sleep(max(0.0, wait_sec))
    snap2 = _pg_snapshot(client, pg_id)
    consume = snap2["processors"].get("ConsumeKafka") or {}
    if consume.get("state") != "RUNNING" or consume.get("validationStatus") not in (
        None,
        "VALID",
    ):
        return _fail(
            f"ConsumeKafka after publish: state={consume.get('state')} "
            f"validation={consume.get('validationStatus')}"
        )
    _ok("ConsumeKafka still RUNNING VALID after publish")

    # Scoped health: demo PG processors only (ignore unrelated sample-flow faults).
    bad = [
        n
        for n, p in snap2["processors"].items()
        if p.get("state") == "STOPPED" or p.get("validationStatus") == "INVALID"
    ]
    if bad:
        return _fail(f"demo PG unhealthy: {bad}")
    _ok(f"demo PG healthy ({len(snap2['processors'])} processors)")

    print("=" * 60)
    print("PASS (nifi←kafka smoke)")
    print("=" * 60)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Force repair/restart of Ratatoskr Kafka Demo before checks",
    )
    parser.add_argument("--count", type=int, default=3, help="Messages to publish")
    parser.add_argument(
        "--wait",
        type=float,
        default=3.0,
        help="Seconds to wait after publish before re-check",
    )
    args = parser.parse_args()
    return run(repair=args.repair, count=args.count, wait_sec=args.wait)


if __name__ == "__main__":
    raise SystemExit(main())
