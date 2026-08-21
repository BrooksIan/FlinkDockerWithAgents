"""Kafka topic discovery and sampling for pipeline sources."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any

from ratatoskr.constants import FULL_PROFILE, KAFKA_PROFILE
from ratatoskr.docker_utils import container_id, project_root

# Canonical topic descriptions (Studio + honeypot). Monitor catalog scope is
# controlled by KAFKA_CATALOG=studio|full (default studio — excludes cowrie.*).
_STATIC_TOPICS: dict[str, str] = {
    "cowrie.events": "Raw Cowrie JSON line events",
    "cowrie.normalized": "Normalized Cowrie events (Phase 1)",
    "cowrie.normalized.enriched": "Actor-enriched normalized events (Phase 1.5)",
    "cowrie.session_actor": "Session actor classification stream",
    "cowrie.alerts": "Phase 2 workflow alerts (deterministic)",
    "cowrie.react_alerts": "Phase 3 ReAct enrichment alerts",
    "cowrie.disinfo_requests": "Counter-attack / disinfo request stream",
    "workflow.test.input": "Studio test input — integer value records for workflow_counter",
    "workflow.test.output": "Studio test sink — workflow_counter doubled output",
    "session.window.input": "Cowrie-like events for session window detect demo (keyed by src_ip)",
    "session.window.output": "Session detect agent output",
    "nasa.neo": "NASA NEO — near-earth object close-approach feed",
    "nifi.monitor.poll": "NiFi monitor poll triggers (continuous / Kafka-driven)",
    "nifi.monitor.output": "NiFi monitor agent OutputEvents (optional sink)",
    "kafka.monitor.poll": "Kafka monitor poll triggers (continuous / Kafka-driven)",
    "kafka.monitor.output": "Kafka monitor agent OutputEvents (optional sink)",
    "nifi.kafka.demo": "Shared NiFi←Kafka demo topic (ConsumeKafka lab flow)",
    "signals.correlate.output": "NiFi↔Kafka correlation incidents (workflow_signal_correlate)",
    "signals.cross_heal.output": "Cross-stack heal results (workflow_cross_stack_heal)",
    "signals.incident.brief": "Incident scribe briefs (react_incident_scribe)",
}

# Topics created by deploy/docker-compose.kafka.yml kafka-init (+ monitor topics).
STUDIO_CATALOG_TOPICS: frozenset[str] = frozenset(
    {
        "workflow.test.input",
        "workflow.test.output",
        "session.window.input",
        "session.window.output",
        "nasa.neo",
        "nifi.monitor.poll",
        "nifi.monitor.output",
        "kafka.monitor.poll",
        "kafka.monitor.output",
        "nifi.kafka.demo",
    }
)

DEFAULT_KAFKA_OUTPUT_TOPIC = "workflow.test.output"
DEFAULT_KAFKA_INPUT_TOPIC = "workflow.test.input"

STUDIO_KAFKA_EXTERNAL_PORT = 9094

_HOST_BOOTSTRAP_CANDIDATES = (
    f"localhost:{STUDIO_KAFKA_EXTERNAL_PORT}",
    "localhost:9093",
    f"127.0.0.1:{STUDIO_KAFKA_EXTERNAL_PORT}",
    "127.0.0.1:9093",
    "localhost:9092",
    "127.0.0.1:9092",
)


def kafka_bootstrap_candidates() -> list[str]:
    explicit = (
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        or os.environ.get("COWRIE_KAFKA_BOOTSTRAP")
        or ""
    ).strip()
    if explicit:
        return [explicit, *[c for c in _HOST_BOOTSTRAP_CANDIDATES if c != explicit]]
    return list(_HOST_BOOTSTRAP_CANDIDATES)


def kafka_bootstrap_servers() -> str:
    resolved = resolve_host_bootstrap()
    if resolved:
        return resolved
    return kafka_bootstrap_candidates()[0]


def _kafka_container_id() -> str | None:
    """Prefer Studio Kafka; fall back to honeypot broker when only full stack is up."""
    cid = container_id("kafka", profile=KAFKA_PROFILE)
    if cid:
        return cid
    return container_id("kafka", profile=FULL_PROFILE)


def _docker_kafka_run(command: str, *, timeout_sec: float = 30) -> subprocess.CompletedProcess[str]:
    cid = _kafka_container_id()
    if not cid:
        raise RuntimeError("Kafka container not running (ratatoskr kafka up)")
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
            client_id="ratatoskr-kafka-probe",
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
        from ratatoskr.paths import configure_runtime_sys_path

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
        client_id="ratatoskr-kafka-list",
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
            f"Start Studio Kafka: ratatoskr kafka up  (localhost:{STUDIO_KAFKA_EXTERNAL_PORT})"
        )
    return records[:limit]


def _record_key(record: dict[str, Any]) -> str:
    if "key" in record:
        return str(record["key"])
    if "k" in record:
        return str(record["k"])
    if len(record) == 1:
        only = next(iter(record))
        if only not in ("value", "v", "output"):
            return str(only)
    return "1"


def _record_to_kafka_message(record: dict[str, Any]) -> tuple[str | None, Any]:
    key = _record_key(record)
    if "output" in record:
        return key, record["output"]
    if "value" in record:
        return key, record["value"]
    if len(record) == 1:
        only_key, payload = next(iter(record.items()))
        if only_key not in ("key", "k", "value", "v", "output"):
            return str(only_key), payload
    return key, record


def _publish_topic_records_host(
    topic: str,
    records: list[dict[str, Any]],
    *,
    bootstrap: str,
) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
    )
    try:
        for record in records:
            key, payload = _record_to_kafka_message(record)
            producer.send(topic, key=key, value=payload)
        producer.flush()
    finally:
        producer.close()


def _publish_topic_records_docker(topic: str, records: list[dict[str, Any]]) -> None:
    cid = _kafka_container_id()
    if not cid:
        raise RuntimeError("Kafka container not running (ratatoskr kafka up)")

    lines: list[str] = []
    for record in records:
        _, payload = _record_to_kafka_message(record)
        lines.append(json.dumps(payload, default=str))

    if not lines:
        return

    safe_topic = shlex.quote(topic)
    command = f"kafka-console-producer --bootstrap-server localhost:9092 --topic {safe_topic}"
    result = subprocess.run(
        ["docker", "exec", "-i", cid, "bash", "-c", command],
        cwd=project_root(),
        input="\n".join(lines).encode("utf-8"),
        capture_output=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or b"").decode("utf-8", errors="replace").strip()
        raise RuntimeError(detail or f"Failed to publish to Kafka topic {topic!r}")


def cluster_kafka_bootstrap_servers() -> str:
    """Bootstrap servers reachable from Flink TaskManagers (host broker via Docker Desktop)."""
    explicit = (os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or "").strip()
    servers = explicit or f"host.docker.internal:{STUDIO_KAFKA_EXTERNAL_PORT}"
    return (
        servers.replace("localhost", "host.docker.internal")
        .replace("127.0.0.1", "host.docker.internal")
    )


def _running_in_flink_container() -> bool:
    return Path("/opt/flink").is_dir()


def _kafka_unavailable_message(*, servers: str) -> str:
    cid = _kafka_container_id()
    if cid:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", cid],
            cwd=project_root(),
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip().lower() != "true":
            return (
                f"Kafka broker unreachable at {servers}. "
                "The Studio Kafka container is stopped — run: "
                f"ratatoskr kafka up  (broker: localhost:{STUDIO_KAFKA_EXTERNAL_PORT})"
            )
    return (
        f"Kafka broker unreachable at {servers}. "
        f"Start Studio Kafka: ratatoskr kafka up  (localhost:{STUDIO_KAFKA_EXTERNAL_PORT})"
    )


def publish_topic_records(
    topic: str,
    records: list[dict[str, Any]],
    *,
    bootstrap: str | None = None,
) -> int:
    """Publish pipeline output records to a Kafka topic. Returns message count."""
    if not topic.strip():
        raise ValueError("Kafka sink missing topic")
    if not records:
        return 0

    if bootstrap and _host_kafka_reachable(bootstrap=bootstrap):
        _publish_topic_records_host(topic, records, bootstrap=bootstrap)
        return len(records)

    host = resolve_host_bootstrap()
    if host:
        _publish_topic_records_host(topic, records, bootstrap=host)
        return len(records)

    if _running_in_flink_container():
        cluster_bs = cluster_kafka_bootstrap_servers()
        if _host_kafka_reachable(bootstrap=cluster_bs):
            _publish_topic_records_host(topic, records, bootstrap=cluster_bs)
            return len(records)

    if docker_kafka_reachable():
        _publish_topic_records_docker(topic, records)
        return len(records)

    servers = bootstrap or kafka_bootstrap_servers()
    raise RuntimeError(_kafka_unavailable_message(servers=servers))
