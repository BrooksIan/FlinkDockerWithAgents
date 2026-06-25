"""Kafka topic discovery and sampling for pipeline sources."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from typing import Any

from apemosyne.constants import FULL_PROFILE
from apemosyne.docker_utils import container_id, project_root

# Canonical Cowrie pipeline topics (env overrides applied in pipeline_kafka_topics when honeypot is loaded).
_STATIC_TOPICS: dict[str, str] = {
    "cowrie.events": "Raw Cowrie JSON line events",
    "cowrie.normalized": "Normalized Cowrie events (Phase 1)",
    "cowrie.normalized.enriched": "Actor-enriched normalized events (Phase 1.5)",
    "cowrie.session_actor": "Session actor classification stream",
    "cowrie.alerts": "Phase 2 workflow alerts (deterministic)",
    "cowrie.react_alerts": "Phase 3 ReAct enrichment alerts",
    "cowrie.disinfo_requests": "Counter-attack / disinfo request stream",
}

_HOST_BOOTSTRAP_CANDIDATES = ("localhost:9093", "localhost:9092", "127.0.0.1:9093", "127.0.0.1:9092")


def kafka_bootstrap_candidates() -> list[str]:
    explicit = (
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or os.environ.get("COWRIE_KAFKA_BOOTSTRAP")
        or ""
    ).strip()
    if explicit:
        return [explicit]
    return list(_HOST_BOOTSTRAP_CANDIDATES)


def kafka_bootstrap_servers() -> str:
    resolved = resolve_host_bootstrap()
    if resolved:
        return resolved
    return kafka_bootstrap_candidates()[0]


def _kafka_container_id() -> str | None:
    return container_id("kafka", profile=FULL_PROFILE)


def _docker_kafka_run(command: str, *, timeout_sec: float = 30) -> subprocess.CompletedProcess[str]:
    cid = _kafka_container_id()
    if not cid:
        raise RuntimeError("Kafka container not running (apemosyne up --profile full)")
    return subprocess.run(
        ["docker", "exec", cid, "bash", "-c", command],
        cwd=project_root(),
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )


def _host_kafka_reachable(*, bootstrap: str, timeout_ms: int = 2000) -> bool:
    if not bootstrap:
        return False
    try:
        from kafka.admin import KafkaAdminClient

        client = KafkaAdminClient(
            bootstrap_servers=bootstrap,
            client_id="apemosyne-kafka-probe",
            request_timeout_ms=timeout_ms,
        )
        try:
            client.list_topics()
            return True
        finally:
            client.close()
    except Exception:
        return False


def resolve_host_bootstrap(*, timeout_ms: int = 2000) -> str | None:
    for servers in kafka_bootstrap_candidates():
        if _host_kafka_reachable(bootstrap=servers, timeout_ms=timeout_ms):
            return servers
    return None


def docker_kafka_reachable() -> bool:
    if not _kafka_container_id():
        return False
    try:
        result = _docker_kafka_run(
            "kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1"
        )
        return result.returncode == 0
    except Exception:
        return False


def kafka_reachable(*, bootstrap: str | None = None, timeout_ms: int = 2000) -> bool:
    if bootstrap:
        return _host_kafka_reachable(bootstrap=bootstrap, timeout_ms=timeout_ms) or docker_kafka_reachable()
    if resolve_host_bootstrap(timeout_ms=timeout_ms):
        return True
    return docker_kafka_reachable()


def known_pipeline_topics() -> list[str]:
    """Return configured pipeline topic names (honeypot module when available)."""
    try:
        from apemosyne.paths import configure_runtime_sys_path

        configure_runtime_sys_path(include_honeypot=True)
        from cowrie_pipeline import pipeline_kafka_topics

        return pipeline_kafka_topics()
    except Exception:
        return list(_STATIC_TOPICS.keys())


def topic_description(topic: str) -> str:
    if topic in _STATIC_TOPICS:
        return _STATIC_TOPICS[topic]
    if topic.startswith("cowrie."):
        return "Cowrie pipeline topic"
    return "Kafka topic"


def _list_broker_topics_host(*, bootstrap: str, timeout_ms: int = 3000) -> set[str]:
    from kafka.admin import KafkaAdminClient

    client = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        client_id="apemosyne-kafka-list",
        request_timeout_ms=timeout_ms,
    )
    try:
        return set(client.list_topics())
    finally:
        client.close()


def _list_broker_topics_docker() -> set[str]:
    result = _docker_kafka_run("kafka-topics --bootstrap-server localhost:9092 --list")
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "Failed to list Kafka topics")
    topics: set[str] = set()
    for line in result.stdout.splitlines():
        name = line.strip()
        if name and not name.startswith("__"):
            topics.add(name)
    return topics


def list_kafka_sources(*, bootstrap: str | None = None) -> dict[str, Any]:
    """Topics for Studio palette: known pipeline topics plus live broker topics."""
    known = known_pipeline_topics()
    live: set[str] = set()
    reachable = False
    servers = bootstrap

    if bootstrap:
        try:
            live = _list_broker_topics_host(bootstrap=bootstrap)
            reachable = True
        except Exception:
            if docker_kafka_reachable():
                live = _list_broker_topics_docker()
                reachable = True
                servers = servers or "kafka:9092 (via docker)"
    else:
        host = resolve_host_bootstrap()
        if host:
            servers = host
            try:
                live = _list_broker_topics_host(bootstrap=host)
                reachable = True
            except Exception:
                pass
        if not reachable and docker_kafka_reachable():
            live = _list_broker_topics_docker()
            reachable = True
            servers = servers or "kafka:9092 (via docker)"

    if not servers:
        servers = kafka_bootstrap_candidates()[0]

    names: list[str] = []
    seen: set[str] = set()
    for topic in known:
        if topic and topic not in seen:
            seen.add(topic)
            names.append(topic)
    for topic in sorted(live):
        if topic and topic not in seen:
            seen.add(topic)
            names.append(topic)

    topics = [
        {
            "name": name,
            "description": topic_description(name),
            "present": name in live if reachable else None,
        }
        for name in names
    ]
    return {
        "bootstrap": servers,
        "reachable": reachable,
        "topics": topics,
    }


def _message_to_record(
    value: bytes | str | None,
    key: bytes | str | None,
    *,
    index: int,
) -> dict[str, Any]:
    record_key = "1"
    if key is not None:
        record_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
    elif index:
        record_key = str(index + 1)

    if value is None:
        return {"key": record_key, "value": None}

    raw = value.decode("utf-8") if isinstance(value, bytes) else value
    try:
        payload: Any = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        payload = raw

    if isinstance(payload, dict):
        if "key" in payload and ("value" in payload or "v" in payload):
            return payload
        if "session" in payload:
            return {"key": str(payload.get("session") or record_key), "value": payload}
        return {"key": record_key, "value": payload}
    return {"key": record_key, "value": payload}


def _sample_topic_records_host(
    topic: str,
    *,
    limit: int,
    bootstrap: str,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
    )
    records: list[dict[str, Any]] = []
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"Kafka topic {topic!r} not found on broker {bootstrap}")

        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)
        consumer.seek_to_end(*tps)
        for tp in tps:
            end = consumer.position(tp)
            consumer.seek(tp, max(0, end - max(limit, 1)))

        for idx, msg in enumerate(consumer):
            records.append(_message_to_record(msg.value, msg.key, index=idx))
            if len(records) >= limit:
                break
    finally:
        consumer.close()
    return records


def _sample_topic_records_docker(
    topic: str,
    *,
    limit: int,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    safe_topic = shlex.quote(topic)
    command = (
        "kafka-console-consumer --bootstrap-server localhost:9092 "
        f"--topic {safe_topic} --from-beginning --max-messages {int(limit)} "
        f"--timeout-ms {int(timeout_ms)} 2>/dev/null"
    )
    result = _docker_kafka_run(command, timeout_sec=max(timeout_ms / 1000 + 5, 10))
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(
            f"No messages read from Kafka topic {topic!r}. "
            "Publish test events or wait for traffic."
        )
    return [_message_to_record(line, None, index=idx) for idx, line in enumerate(lines[:limit])]


def sample_topic_records(
    topic: str,
    *,
    limit: int = 10,
    bootstrap: str | None = None,
    timeout_ms: int = 5000,
) -> list[dict[str, Any]]:
    """Read up to ``limit`` messages from a Kafka topic for local pipeline runs."""
    if not topic.strip():
        raise ValueError("Kafka source missing topic")

    records: list[dict[str, Any]] = []
    if bootstrap:
        try:
            records = _sample_topic_records_host(
                topic, limit=limit, bootstrap=bootstrap, timeout_ms=timeout_ms
            )
        except Exception:
            records = []
    else:
        host = resolve_host_bootstrap(timeout_ms=min(timeout_ms, 3000))
        if host:
            try:
                records = _sample_topic_records_host(
                    topic, limit=limit, bootstrap=host, timeout_ms=timeout_ms
                )
            except Exception:
                records = []

    if not records and docker_kafka_reachable():
        records = _sample_topic_records_docker(topic, limit=limit, timeout_ms=timeout_ms)

    if not records:
        servers = bootstrap or kafka_bootstrap_servers()
        raise RuntimeError(
            f"Kafka broker unreachable at {servers}. "
            "Start the full stack: apemosyne up --profile full"
        )
    return records[:limit]
