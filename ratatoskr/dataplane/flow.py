"""Ensure Ratatoskr Data Plane NiFi flow + Kafka topics.

Layout (NiFi 2.x)::

  Schema gate (ValidateJson — real JSON Schema, not Avro):
    ConsumeRaw → ValidateJson → PublishValid | PublishViolations

  Route / enrich:
    ConsumeValid → ExtractJson → EnrichUpdate → RouteType → PublishEnriched

  Replay (dedicated group; not the live path):
    ReplayConsume → ReplayMark → ReplayPublish → events.replay.out
"""

from __future__ import annotations

import json
import time
from typing import Any

from ratatoskr.dataplane.env import default_kafka_bootstrap_for_nifi
from ratatoskr.dataplane.topics import (
    TOPIC_ENRICHED,
    TOPIC_RAW,
    TOPIC_REPLAY_OUT,
    TOPIC_VALID,
    TOPIC_VIOLATIONS,
    TOPICS,
)

PG_NAME = "Ratatoskr Data Plane"

KAFKA_CS_TYPE = "org.apache.nifi.kafka.service.Kafka3ConnectionService"
CONSUME_TYPE = "org.apache.nifi.kafka.processors.ConsumeKafka"
PUBLISH_TYPE = "org.apache.nifi.kafka.processors.PublishKafka"
VALIDATE_TYPE = "org.apache.nifi.processors.standard.ValidateJson"
UPDATE_TYPE = "org.apache.nifi.processors.attributes.UpdateAttribute"
ROUTE_TYPE = "org.apache.nifi.processors.standard.RouteOnAttribute"
EVAL_JSON_TYPE = "org.apache.nifi.processors.standard.EvaluateJsonPath"

GROUP_SCHEMA = "ratatoskr-dataplane-schema"
GROUP_ROUTE = "ratatoskr-dataplane-route"
GROUP_REPLAY = "ratatoskr-dataplane-replay"

# Minimal contract for demo events (JSON Schema Draft 2020-12 / 07 compatible).
DEFAULT_JSON_SCHEMA = json.dumps(
    {
        "type": "object",
        "required": ["id", "type", "payload"],
        "properties": {
            "id": {"type": "string"},
            "type": {"type": "string"},
            "payload": {"type": "object"},
        },
        "additionalProperties": True,
    },
    separators=(",", ":"),
)

LAB_JSON_SCHEMA = json.dumps(
    {
        "type": "object",
        "required": ["id", "type", "payload"],
        "properties": {
            "id": {"type": "string"},
            "type": {"type": "string"},
            "payload": {"type": "object"},
        },
        "additionalProperties": False,
    },
    separators=(",", ":"),
)

SCHEMA_GATE_PROCESSORS = (
    "ConsumeRaw",
    "ValidateJson",
    "PublishValid",
    "PublishViolations",
)
ROUTE_PROCESSORS = (
    "ConsumeValid",
    "ExtractJson",
    "EnrichUpdate",
    "RouteType",
    "PublishEnriched",
)
REPLAY_PROCESSORS = ("ReplayConsume", "ReplayMark", "ReplayPublish")
ALL_PROCESSORS = SCHEMA_GATE_PROCESSORS + ROUTE_PROCESSORS + REPLAY_PROCESSORS


def ensure_dataplane_topics() -> dict[str, Any]:
    """Create data-plane topics on Studio Kafka if missing (host-side)."""
    from ratatoskr.kafka.client import KafkaClient

    created: list[str] = []
    existing: list[str] = []
    warnings: list[str] = []
    kc = KafkaClient()
    try:
        known = kc.list_topics()
        for name in TOPICS:
            if name in known:
                existing.append(name)
                continue
            try:
                kc.create_topic(name, partitions=1, replication_factor=1)
                created.append(name)
                time.sleep(0.2)
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"{name}: {exc}")
    finally:
        kc.close()
    return {
        "topics": list(TOPICS),
        "created": created,
        "existing": existing,
        "warnings": warnings,
    }


def _find_pg(client: Any, root_id: str, name: str = PG_NAME) -> str | None:
    for ent in client.list_process_groups(root_id):
        comp = ent.get("component") or {}
        if comp.get("name") == name:
            return comp.get("id")
    return None


def _processors_by_name(client: Any, pg_id: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for ent in client.list_processors(pg_id):
        comp = ent.get("component") or {}
        out[comp.get("name") or ""] = {
            "id": comp.get("id"),
            "revision": ent.get("revision"),
            "state": comp.get("state"),
            "entity": ent,
        }
    return out


def _services_by_name(client: Any, pg_id: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for ent in client.get_controller_services(pg_id):
        comp = ent.get("component") or {}
        out[comp.get("name") or ""] = {
            "id": comp.get("id"),
            "revision": ent.get("revision"),
            "state": comp.get("state"),
            "entity": ent,
        }
    return out


def _wait_service_state(
    client: Any, service_id: str, want: str, attempts: int = 40
) -> None:
    want_u = want.upper()
    last = None
    for _ in range(attempts):
        det = client.get_controller_service_details(service_id)
        state = ((det.get("component") or {}).get("state") or "").upper()
        status = ((det.get("status") or {}).get("runStatus") or "").upper()
        last = state or status
        if state == want_u or status == want_u:
            return
        time.sleep(0.4)
    raise RuntimeError(
        f"Controller service {service_id} did not reach {want_u} (last={last})"
    )


def _wait_service_enabled(client: Any, service_id: str, attempts: int = 30) -> None:
    _wait_service_state(client, service_id, "ENABLED", attempts=attempts)


def _enable_cs(client: Any, service_id: str) -> None:
    det = client.get_controller_service_details(service_id)
    state = ((det.get("component") or {}).get("state") or "").upper()
    if state == "ENABLED":
        return
    # Fresh revision — do not pass a stale version from an earlier list call.
    client.enable_controller_service(service_id)
    _wait_service_enabled(client, service_id)


def _disable_cs(client: Any, service_id: str) -> None:
    det = client.get_controller_service_details(service_id)
    state = ((det.get("component") or {}).get("state") or "").upper()
    if state == "DISABLED":
        return
    client.disable_controller_service(service_id)
    _wait_service_state(client, service_id, "DISABLED")


def _stop_named(client: Any, procs: dict[str, dict[str, Any]], names: tuple[str, ...]) -> None:
    for name in names:
        proc = procs.get(name)
        if proc and proc.get("state") not in ("STOPPED", "DISABLED", None):
            try:
                client.stop_processor(proc["id"])
            except Exception:  # noqa: BLE001
                pass
    time.sleep(0.6)


def _start_named(
    client: Any, procs: dict[str, dict[str, Any]], names: tuple[str, ...]
) -> list[str]:
    started: list[str] = []
    for name in names:
        proc = procs.get(name)
        if not proc:
            continue
        try:
            client.start_processor(proc["id"])
            started.append(name)
        except Exception as exc:  # noqa: BLE001
            started.append(f"{name}:ERROR:{exc}")
    return started


def _pid(entity: dict[str, Any]) -> str:
    return str((entity.get("component") or {}).get("id") or entity.get("id") or "")


def update_schema_text(client: Any, pg_id: str, schema_text: str) -> dict[str, Any]:
    """Stop ValidateJson, patch JSON Schema property, restart. Schema-gate only mutation."""
    procs = _processors_by_name(client, pg_id)
    vr = procs.get("ValidateJson")
    if not vr:
        raise RuntimeError("ValidateJson processor not found")
    _stop_named(client, procs, ("ValidateJson",))
    client.update_processor_config(
        vr["id"],
        properties={
            "Schema Access Strategy": "SCHEMA_CONTENT_PROPERTY",
            "JSON Schema": schema_text,
            "JSON Schema Version": "DRAFT_7",
        },
    )
    time.sleep(0.3)
    client.start_processor(vr["id"])
    return {
        "ok": True,
        "op": "update_schema_text",
        "processor_id": vr["id"],
        "processor_name": "ValidateJson",
    }


def get_schema_text(client: Any, pg_id: str) -> str | None:
    procs = _processors_by_name(client, pg_id)
    vr = procs.get("ValidateJson")
    if not vr:
        return None
    det = client.get_processor_details(vr["id"])
    props = ((det.get("component") or {}).get("config") or {}).get("properties") or {}
    return props.get("JSON Schema") or props.get("json-schema")


def _create_kafka_cs(client: Any, pg_id: str, bootstrap: str) -> str:
    existing = _services_by_name(client, pg_id).get("Studio Kafka")
    if existing:
        _disable_cs(client, existing["id"])
        client.update_controller_service_properties(
            existing["id"],
            {
                "bootstrap.servers": bootstrap,
                "security.protocol": "PLAINTEXT",
            },
        )
        _enable_cs(client, existing["id"])
        return str(existing["id"])

    cs = client.create_controller_service(
        pg_id,
        KAFKA_CS_TYPE,
        "Studio Kafka",
        properties={
            "bootstrap.servers": bootstrap,
            "security.protocol": "PLAINTEXT",
        },
    )
    cs_id = _pid(cs)
    if not cs_id:
        raise RuntimeError(f"Failed to create Kafka connection service: {cs}")
    client.update_controller_service_properties(
        cs_id,
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "PLAINTEXT",
        },
    )
    _enable_cs(client, cs_id)
    return cs_id


def _consume_props(cs_id: str, group: str, topic: str) -> dict[str, str]:
    return {
        "Kafka Connection Service": cs_id,
        "Group ID": group,
        "Topic Format": "names",
        "Topics": topic,
        "auto.offset.reset": "earliest",
        "Commit Offsets": "true",
        "Processing Strategy": "FLOW_FILE",
    }


def _publish_props(cs_id: str, topic: str) -> dict[str, str]:
    return {
        "Kafka Connection Service": cs_id,
        "Topic Name": topic,
        "Failure Strategy": "Route to Failure",
        "Transactions Enabled": "false",
        "Publish Strategy": "USE_VALUE",
    }


def _wire(
    client: Any,
    pg_id: str,
    src: str,
    dst: str,
    relationships: list[str],
    name: str,
) -> None:
    client.create_connection(
        pg_id, src, "PROCESSOR", dst, "PROCESSOR", relationships, name=name
    )


def _delete_orphan_record_services(client: Any, pg_id: str) -> None:
    """Remove leftover JsonTreeReader / JsonRecordSetWriter from failed ValidateRecord setup."""
    for name in ("JsonTreeReader", "JsonRecordSetWriter"):
        svc = _services_by_name(client, pg_id).get(name)
        if not svc:
            continue
        try:
            _disable_cs(client, svc["id"])
            det = client.get_controller_service_details(svc["id"])
            rev = det.get("revision") or {}
            client._request(
                "DELETE",
                f"/controller-services/{svc['id']}",
                params={
                    "version": str(rev.get("version", 0)),
                    "clientId": str(rev.get("clientId") or "ratatoskr"),
                },
            )
        except Exception:  # noqa: BLE001
            pass


def _build_flow(
    client: Any,
    pg_id: str,
    *,
    bootstrap: str,
    schema_text: str,
) -> dict[str, Any]:
    _delete_orphan_record_services(client, pg_id)
    cs_id = _create_kafka_cs(client, pg_id, bootstrap)

    consume_raw = client.create_processor(
        pg_id,
        CONSUME_TYPE,
        "ConsumeRaw",
        x=0,
        y=0,
        properties=_consume_props(cs_id, GROUP_SCHEMA, TOPIC_RAW),
    )
    validate = client.create_processor(
        pg_id,
        VALIDATE_TYPE,
        "ValidateJson",
        x=300,
        y=0,
        properties={
            "Schema Access Strategy": "SCHEMA_CONTENT_PROPERTY",
            "JSON Schema": schema_text,
            "JSON Schema Version": "DRAFT_7",
        },
    )
    publish_valid = client.create_processor(
        pg_id,
        PUBLISH_TYPE,
        "PublishValid",
        x=600,
        y=-80,
        properties=_publish_props(cs_id, TOPIC_VALID),
        auto_terminated_relationships=["success", "failure"],
    )
    publish_viol = client.create_processor(
        pg_id,
        PUBLISH_TYPE,
        "PublishViolations",
        x=600,
        y=80,
        properties=_publish_props(cs_id, TOPIC_VIOLATIONS),
        auto_terminated_relationships=["success", "failure"],
    )

    consume_valid = client.create_processor(
        pg_id,
        CONSUME_TYPE,
        "ConsumeValid",
        x=0,
        y=280,
        properties=_consume_props(cs_id, GROUP_ROUTE, TOPIC_VALID),
    )
    extract = client.create_processor(
        pg_id,
        EVAL_JSON_TYPE,
        "ExtractJson",
        x=200,
        y=280,
        properties={
            "Destination": "flowfile-attribute",
            "Return Type": "json",
            "Path Not Found Behavior": "ignore",
            "Null Value Representation": "empty string",
            "type": "$.type",
            "id": "$.id",
        },
        auto_terminated_relationships=["failure", "unmatched"],
    )
    enrich = client.create_processor(
        pg_id,
        UPDATE_TYPE,
        "EnrichUpdate",
        x=400,
        y=280,
        properties={
            "ratatoskr.env": "lab",
            "ratatoskr.pipeline": "dataplane",
            "event.type": "${type}",
        },
    )
    route = client.create_processor(
        pg_id,
        ROUTE_TYPE,
        "RouteType",
        x=650,
        y=280,
        properties={
            "Routing Strategy": "Route to Property name",
            "enriched": "${event.type:equals('order')}",
        },
        auto_terminated_relationships=["unmatched", "failure"],
    )
    publish_enriched = client.create_processor(
        pg_id,
        PUBLISH_TYPE,
        "PublishEnriched",
        x=900,
        y=280,
        properties=_publish_props(cs_id, TOPIC_ENRICHED),
        auto_terminated_relationships=["success", "failure"],
    )

    replay_consume = client.create_processor(
        pg_id,
        CONSUME_TYPE,
        "ReplayConsume",
        x=0,
        y=520,
        properties=_consume_props(cs_id, GROUP_REPLAY, TOPIC_VALID),
    )
    replay_mark = client.create_processor(
        pg_id,
        UPDATE_TYPE,
        "ReplayMark",
        x=300,
        y=520,
        properties={
            "ratatoskr.replay": "true",
            "ratatoskr.pipeline": "replay",
        },
    )
    replay_publish = client.create_processor(
        pg_id,
        PUBLISH_TYPE,
        "ReplayPublish",
        x=600,
        y=520,
        properties=_publish_props(cs_id, TOPIC_REPLAY_OUT),
        auto_terminated_relationships=["success", "failure"],
    )

    ids = {
        "consume_raw": _pid(consume_raw),
        "validate": _pid(validate),
        "publish_valid": _pid(publish_valid),
        "publish_violations": _pid(publish_viol),
        "consume_valid": _pid(consume_valid),
        "extract_json": _pid(extract),
        "enrich": _pid(enrich),
        "route": _pid(route),
        "publish_enriched": _pid(publish_enriched),
        "replay_consume": _pid(replay_consume),
        "replay_mark": _pid(replay_mark),
        "replay_publish": _pid(replay_publish),
        "kafka_cs": cs_id,
    }

    _wire(client, pg_id, ids["consume_raw"], ids["validate"], ["success"], "raw-to-validate")
    _wire(client, pg_id, ids["validate"], ids["publish_valid"], ["valid"], "validate-to-valid")
    _wire(
        client,
        pg_id,
        ids["validate"],
        ids["publish_violations"],
        ["invalid"],
        "validate-to-violations",
    )
    # ValidateJson also has failure — auto-terminate it
    client.update_processor_config(
        ids["validate"],
        auto_terminated_relationships=["failure"],
    )
    _wire(
        client,
        pg_id,
        ids["consume_valid"],
        ids["extract_json"],
        ["success"],
        "valid-to-extract",
    )
    _wire(
        client,
        pg_id,
        ids["extract_json"],
        ids["enrich"],
        ["matched"],
        "extract-to-enrich",
    )
    _wire(client, pg_id, ids["enrich"], ids["route"], ["success"], "enrich-to-route")
    _wire(
        client,
        pg_id,
        ids["route"],
        ids["publish_enriched"],
        ["enriched"],
        "route-to-enriched",
    )
    _wire(
        client,
        pg_id,
        ids["replay_consume"],
        ids["replay_mark"],
        ["success"],
        "replay-consume-to-mark",
    )
    _wire(
        client,
        pg_id,
        ids["replay_mark"],
        ids["replay_publish"],
        ["success"],
        "replay-mark-to-publish",
    )

    live = SCHEMA_GATE_PROCESSORS + ROUTE_PROCESSORS
    procs = _processors_by_name(client, pg_id)
    started = _start_named(client, procs, live)

    return {
        "process_group_id": pg_id,
        "created": True,
        "bootstrap": bootstrap,
        "controller_service_id": cs_id,
        "processors": ids,
        "started": started,
        "replay_started": False,
        "groups": {
            "schema": GROUP_SCHEMA,
            "route": GROUP_ROUTE,
            "replay": GROUP_REPLAY,
        },
    }


def repair_dataplane_flow(
    client: Any,
    pg_id: str,
    *,
    bootstrap: str,
    schema_text: str | None = None,
) -> dict[str, Any]:
    """Rebuild processors if missing; otherwise re-point Kafka CS and restart live path."""
    procs = _processors_by_name(client, pg_id)
    schema = schema_text or DEFAULT_JSON_SCHEMA
    if len(procs) < len(SCHEMA_GATE_PROCESSORS):
        out = _build_flow(client, pg_id, bootstrap=bootstrap, schema_text=schema)
        out["repaired"] = True
        out["created"] = False
        return out

    services = _services_by_name(client, pg_id)
    cs = services.get("Studio Kafka")
    if not cs:
        raise RuntimeError("Studio Kafka controller service not found")

    _stop_named(client, procs, ALL_PROCESSORS)

    det = client.get_controller_service_details(cs["id"])
    current_boot = ((det.get("component") or {}).get("properties") or {}).get(
        "bootstrap.servers"
    )
    cs_updated = False
    if str(current_boot or "").strip() != bootstrap.strip():
        _disable_cs(client, cs["id"])
        client.update_controller_service_properties(
            cs["id"],
            {
                "bootstrap.servers": bootstrap,
                "security.protocol": "PLAINTEXT",
            },
        )
        cs_updated = True
    # Ensure enabled even if we skipped the property update
    _enable_cs(client, cs["id"])

    if schema_text is not None and procs.get("ValidateJson"):
        update_schema_text(client, pg_id, schema_text)

    procs = _processors_by_name(client, pg_id)
    started = _start_named(client, procs, SCHEMA_GATE_PROCESSORS + ROUTE_PROCESSORS)
    return {
        "process_group_id": pg_id,
        "repaired": True,
        "bootstrap": bootstrap,
        "controller_service_updated": cs_updated,
        "started": started,
        "replay_started": False,
    }


def ensure_dataplane_flow(
    client: Any,
    *,
    repair: bool = False,
    bootstrap: str | None = None,
    schema_text: str | None = None,
    ensure_topics: bool = True,
) -> dict[str, Any]:
    """Idempotent ensure of Ratatoskr Data Plane PG + topics."""
    boot = (bootstrap or default_kafka_bootstrap_for_nifi()).strip()
    schema = schema_text or DEFAULT_JSON_SCHEMA
    topic_info: dict[str, Any] = {}
    if ensure_topics:
        topic_info = ensure_dataplane_topics()

    root = client.get_root_process_group()
    flow = root.get("processGroupFlow") or {}
    root_id = flow.get("id")
    if not root_id:
        raise RuntimeError("Could not resolve root process group id")

    existing = _find_pg(client, root_id)
    if existing:
        procs = _processors_by_name(client, existing)
        needs_build = len(procs) < len(SCHEMA_GATE_PROCESSORS)
        if repair or needs_build:
            out = repair_dataplane_flow(
                client, existing, bootstrap=boot, schema_text=schema
            )
            out["topic_ensure"] = topic_info
            out["created"] = False
            return out
        return {
            "process_group_id": existing,
            "created": False,
            "bootstrap": boot,
            "topic_ensure": topic_info,
            "groups": {
                "schema": GROUP_SCHEMA,
                "route": GROUP_ROUTE,
                "replay": GROUP_REPLAY,
            },
        }

    pg = client.create_process_group(root_id, PG_NAME, x=0, y=800)
    pg_id = _pid(pg)
    if not pg_id:
        raise RuntimeError(f"Failed to create process group: {pg}")

    out = _build_flow(client, pg_id, bootstrap=boot, schema_text=schema)
    out["topic_ensure"] = topic_info
    return out


def find_dataplane_pg_id(client: Any) -> str | None:
    root = client.get_root_process_group()
    flow = root.get("processGroupFlow") or {}
    root_id = flow.get("id")
    if not root_id:
        return None
    return _find_pg(client, root_id)


def processors_by_name(client: Any, pg_id: str) -> dict[str, dict[str, Any]]:
    return _processors_by_name(client, pg_id)


def stop_replay_path(client: Any, pg_id: str) -> list[str]:
    procs = _processors_by_name(client, pg_id)
    _stop_named(client, procs, REPLAY_PROCESSORS)
    return list(REPLAY_PROCESSORS)


def start_replay_path(client: Any, pg_id: str) -> list[str]:
    procs = _processors_by_name(client, pg_id)
    return _start_named(client, procs, REPLAY_PROCESSORS)
