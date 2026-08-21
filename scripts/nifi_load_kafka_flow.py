#!/usr/bin/env python3
"""Create / repair the Ratatoskr Kafka→NiFi demo flow.

Flow (NiFi 2.x):
  Kafka3ConnectionService (Studio Kafka)
  ConsumeKafka (nifi.kafka.demo) → UpdateAttribute → LogAttribute

Designed as a shared base for future NiFi + Kafka monitoring demos
(stop consumer, delete topic, group lag, queue backlog).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

PG_NAME = "Ratatoskr Kafka Demo"
DEMO_TOPIC = "nifi.kafka.demo"
DEMO_GROUP = "ratatoskr-nifi-kafka-demo"
KAFKA_CS_TYPE = "org.apache.nifi.kafka.service.Kafka3ConnectionService"
CONSUME_TYPE = "org.apache.nifi.kafka.processors.ConsumeKafka"


def _bootstrap() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def default_bootstrap() -> str:
    """Reach Studio Kafka from the NiFi container (compose DNS) or host overrides."""
    return (
        os.environ.get("NIFI_KAFKA_BOOTSTRAP")
        or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or "kafka:9092"
    ).strip()


def _find_pg(client, root_id: str, name: str = PG_NAME) -> str | None:
    for ent in client.list_process_groups(root_id):
        comp = ent.get("component") or {}
        if comp.get("name") == name:
            return comp.get("id")
    return None


def _processors_by_name(client, pg_id: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for ent in client.list_processors(pg_id):
        comp = ent.get("component") or {}
        out[comp.get("name") or ""] = {
            "id": comp.get("id"),
            "revision": ent.get("revision"),
            "state": comp.get("state"),
            "entity": ent,
        }
    return out


def _services_by_name(client, pg_id: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for ent in client.get_controller_services(pg_id):
        comp = ent.get("component") or {}
        out[comp.get("name") or ""] = {
            "id": comp.get("id"),
            "revision": ent.get("revision"),
            "state": comp.get("state"),
            "entity": ent,
        }
    return out


def _wait_service_enabled(client, service_id: str, attempts: int = 20) -> None:
    for _ in range(attempts):
        det = client.get_controller_service_details(service_id)
        state = (det.get("component") or {}).get("state")
        status = (det.get("status") or {}).get("runStatus")
        if state == "ENABLED" or status == "ENABLED":
            return
        time.sleep(0.5)
    raise RuntimeError(f"Controller service {service_id} did not reach ENABLED")


def ensure_demo_topic() -> dict:
    """Create nifi.kafka.demo on Studio Kafka if missing (host-side)."""
    try:
        from ratatoskr.kafka.client import KafkaClient

        kc = KafkaClient()
        try:
            if DEMO_TOPIC in kc.list_topics():
                return {"topic": DEMO_TOPIC, "created": False}
            kc.create_topic(DEMO_TOPIC, partitions=1, replication_factor=1)
            time.sleep(0.5)
            return {"topic": DEMO_TOPIC, "created": True}
        finally:
            kc.close()
    except Exception as exc:  # noqa: BLE001
        return {"topic": DEMO_TOPIC, "created": False, "warning": str(exc)}


def repair_kafka_flow(client, pg_id: str, *, bootstrap: str) -> dict:
    """Re-point Kafka CS, fix LogAttribute auto-terminate, restart."""
    services = _services_by_name(client, pg_id)
    cs = services.get("Studio Kafka")
    if not cs:
        raise RuntimeError("Studio Kafka controller service not found")

    procs = _processors_by_name(client, pg_id)
    # Stop consumers before disabling the connection service (avoids 409).
    for name in ("ConsumeKafka", "UpdateAttribute", "LogAttribute"):
        proc = procs.get(name)
        if proc and proc.get("state") != "STOPPED":
            try:
                client.stop_processor(
                    proc["id"], (proc.get("revision") or {}).get("version")
                )
            except Exception:  # noqa: BLE001
                pass
    time.sleep(0.5)

    services = _services_by_name(client, pg_id)
    cs = services.get("Studio Kafka") or cs
    if cs.get("state") != "DISABLED":
        try:
            client.disable_controller_service(
                cs["id"], (cs.get("revision") or {}).get("version")
            )
            time.sleep(0.5)
        except Exception:  # noqa: BLE001
            pass

    client.update_controller_service_properties(
        cs["id"],
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "PLAINTEXT",
        },
    )
    det = client.get_controller_service_details(cs["id"])
    client.enable_controller_service(cs["id"], (det.get("revision") or {}).get("version"))
    _wait_service_enabled(client, cs["id"])

    procs = _processors_by_name(client, pg_id)
    log = procs.get("LogAttribute")
    if log:
        if log.get("state") != "STOPPED":
            client.stop_processor(log["id"], (log.get("revision") or {}).get("version"))
        client.update_processor_config(
            log["id"], auto_terminated_relationships=["success"]
        )

    procs = _processors_by_name(client, pg_id)
    started = []
    for name in ("ConsumeKafka", "UpdateAttribute", "LogAttribute"):
        proc = procs.get(name)
        if not proc:
            continue
        try:
            client.start_processor(proc["id"], (proc.get("revision") or {}).get("version"))
            started.append(name)
        except Exception as exc:  # noqa: BLE001
            started.append(f"{name}:ERROR:{exc}")

    return {
        "process_group_id": pg_id,
        "repaired": True,
        "bootstrap": bootstrap,
        "topic": DEMO_TOPIC,
        "group_id": DEMO_GROUP,
        "started": started,
    }


def ensure_kafka_flow(client, *, repair: bool = False, bootstrap: str | None = None) -> dict:
    from ratatoskr.nifi.client import NiFiClient

    assert isinstance(client, NiFiClient)
    boot = (bootstrap or default_bootstrap()).strip()
    topic_info = ensure_demo_topic()

    root = client.get_root_process_group()
    flow = root.get("processGroupFlow") or {}
    root_id = flow.get("id")
    if not root_id:
        raise RuntimeError("Could not resolve root process group id")

    existing = _find_pg(client, root_id)
    if existing:
        if repair:
            out = repair_kafka_flow(client, existing, bootstrap=boot)
            out["topic_ensure"] = topic_info
            return out
        return {
            "process_group_id": existing,
            "created": False,
            "bootstrap": boot,
            "topic": DEMO_TOPIC,
            "group_id": DEMO_GROUP,
            "topic_ensure": topic_info,
        }

    pg = client.create_process_group(root_id, PG_NAME, x=0, y=400)
    pg_id = (pg.get("component") or {}).get("id") or pg.get("id")
    if not pg_id:
        raise RuntimeError(f"Failed to create process group: {pg}")

    cs = client.create_controller_service(
        pg_id,
        KAFKA_CS_TYPE,
        "Studio Kafka",
        properties={
            "bootstrap.servers": boot,
            "security.protocol": "PLAINTEXT",
        },
    )
    cs_id = (cs.get("component") or {}).get("id")
    if not cs_id:
        raise RuntimeError(f"Failed to create Kafka connection service: {cs}")

    # Ensure properties applied (create may ignore some)
    client.update_controller_service_properties(
        cs_id,
        {
            "bootstrap.servers": boot,
            "security.protocol": "PLAINTEXT",
        },
    )
    det = client.get_controller_service_details(cs_id)
    client.enable_controller_service(cs_id, (det.get("revision") or {}).get("version"))
    _wait_service_enabled(client, cs_id)

    consume = client.create_processor(
        pg_id,
        CONSUME_TYPE,
        "ConsumeKafka",
        x=100,
        y=100,
        properties={
            "Kafka Connection Service": cs_id,
            "Group ID": DEMO_GROUP,
            "Topic Format": "names",
            "Topics": DEMO_TOPIC,
            "auto.offset.reset": "earliest",
            "Commit Offsets": "true",
            "Processing Strategy": "FLOW_FILE",
        },
    )
    upd = client.create_processor(
        pg_id,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        "UpdateAttribute",
        x=400,
        y=100,
        properties={
            "ratatoskr.demo": "nifi-kafka",
            "ratatoskr.pipeline": "kafka-to-nifi",
        },
    )
    log = client.create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.LogAttribute",
        "LogAttribute",
        x=700,
        y=100,
        properties={},
        auto_terminated_relationships=["success"],
    )

    consume_id = (consume.get("component") or {}).get("id")
    upd_id = (upd.get("component") or {}).get("id")
    log_id = (log.get("component") or {}).get("id")

    client.create_connection(
        pg_id,
        consume_id,
        "PROCESSOR",
        upd_id,
        "PROCESSOR",
        ["success"],
        name="consume-to-update",
    )
    client.create_connection(
        pg_id,
        upd_id,
        "PROCESSOR",
        log_id,
        "PROCESSOR",
        ["success"],
        name="update-to-log",
    )

    started = []
    for pid, name in (
        (consume_id, "ConsumeKafka"),
        (upd_id, "UpdateAttribute"),
        (log_id, "LogAttribute"),
    ):
        try:
            client.start_processor(pid)
            started.append(name)
        except Exception as exc:  # noqa: BLE001
            started.append(f"{name}:ERROR:{exc}")

    return {
        "process_group_id": pg_id,
        "created": True,
        "bootstrap": boot,
        "topic": DEMO_TOPIC,
        "group_id": DEMO_GROUP,
        "controller_service_id": cs_id,
        "processors": {
            "consume": consume_id,
            "update": upd_id,
            "log": log_id,
        },
        "started": started,
        "topic_ensure": topic_info,
    }


def main() -> int:
    _bootstrap()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait", action="store_true", help="Wait until NiFi API is ready")
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Reconfigure existing Ratatoskr Kafka Demo + restart",
    )
    parser.add_argument(
        "--bootstrap",
        default=None,
        help="Kafka bootstrap (default NIFI_KAFKA_BOOTSTRAP or kafka:9092)",
    )
    args = parser.parse_args()

    from ratatoskr.nifi.client import NiFiClient

    # Reuse wait helper from sample flow loader without package import.
    import importlib.util

    sample_path = Path(__file__).resolve().parent / "nifi_load_sample_flow.py"
    spec = importlib.util.spec_from_file_location("nifi_load_sample_flow", sample_path)
    sample = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(sample)

    client = NiFiClient()
    if args.wait:
        print("Waiting for NiFi API...")
        sample.wait_ready(client)
    result = ensure_kafka_flow(
        client, repair=args.repair, bootstrap=args.bootstrap
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
