"""
Production Cowrie pipeline topic and detection-source conventions.

Typical split::

    Cowrie → Kafka → Phase 1 normalize
                  ├→ Phase 2 workflow  → cowrie.alerts (deterministic)
                  └→ Phase 3 ReAct     → cowrie.react_alerts (LLM + counter_attack_actions)
    log-processor (parallel)           → dashboard JSON (+ Kafka only when not using Phase 1–3)
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional, Sequence

TOPIC_EVENTS = "cowrie.events"
TOPIC_NORMALIZED = "cowrie.normalized"
TOPIC_NORMALIZED_ENRICHED = "cowrie.normalized.enriched"
TOPIC_SESSION_ACTOR = "cowrie.session_actor"
TOPIC_DISINFO_REQUESTS = "cowrie.disinfo_requests"
TOPIC_ALERTS = "cowrie.alerts"
TOPIC_REACT_ALERTS = "cowrie.react_alerts"

DETECTION_WORKFLOW = "pure_python"
DETECTION_FLINK_AGENTS = "flink_agents"
DETECTION_REACT = "cloudera_react"
DETECTION_SUMMARY = "cloudera_summary"

PHASE2_ENGINE_PURE_PYTHON = DETECTION_WORKFLOW
PHASE2_ENGINE_FLINK_AGENTS = DETECTION_FLINK_AGENTS


def _env_truthy(name: str, default: str = "0") -> bool:
    v = os.environ.get(name, default).strip().lower()
    return v in ("1", "true", "yes", "on")


def actor_classify_active() -> bool:
    """True when Phase 1.5 actor classification enriches the normalized stream."""
    return _env_truthy("COWRIE_ACTOR_CLASSIFY", "0")


def resolve_phase2_engine() -> str:
    """
    Select Phase 2 Flink job detection graph.

    - ``pure_python`` (default): PyFlink ``map(workflow_map_line)`` — production default.
    - ``flink_agents``: ``AgentsExecutionEnvironment.from_datastream`` + ``CowrieResponseAgent`` spike.
    """
    raw = (os.environ.get("COWRIE_PHASE2_ENGINE") or PHASE2_ENGINE_PURE_PYTHON).strip().lower()
    if raw in (PHASE2_ENGINE_FLINK_AGENTS, "agents", "flink"):
        return PHASE2_ENGINE_FLINK_AGENTS
    return PHASE2_ENGINE_PURE_PYTHON


def normalized_input_topic() -> str:
    """Kafka topic consumed by Phase 2/3 when actor classification is enabled."""
    if actor_classify_active():
        return (
            os.environ.get("KAFKA_NORMALIZED_ENRICHED_TOPIC", TOPIC_NORMALIZED_ENRICHED).strip()
            or TOPIC_NORMALIZED_ENRICHED
        )
    return os.environ.get("KAFKA_NORMALIZED_TOPIC", TOPIC_NORMALIZED).strip() or TOPIC_NORMALIZED


def pipeline_kafka_topics() -> list[str]:
    """
    All Cowrie pipeline Kafka topic names (env overrides respected).

    Used at compose startup so Flink sources and Python consumers do not fail on
    missing topic metadata before the first producer write.
    """
    topics = [
        os.environ.get("KAFKA_COWRIE_TOPIC", TOPIC_EVENTS).strip() or TOPIC_EVENTS,
        os.environ.get("KAFKA_NORMALIZED_TOPIC", TOPIC_NORMALIZED).strip() or TOPIC_NORMALIZED,
        os.environ.get("KAFKA_NORMALIZED_ENRICHED_TOPIC", TOPIC_NORMALIZED_ENRICHED).strip()
        or TOPIC_NORMALIZED_ENRICHED,
        os.environ.get("KAFKA_SESSION_ACTOR_TOPIC", TOPIC_SESSION_ACTOR).strip() or TOPIC_SESSION_ACTOR,
        os.environ.get("KAFKA_ALERTS_TOPIC", TOPIC_ALERTS).strip() or TOPIC_ALERTS,
        os.environ.get("KAFKA_REACT_ALERTS_TOPIC", TOPIC_REACT_ALERTS).strip() or TOPIC_REACT_ALERTS,
        os.environ.get("KAFKA_DISINFO_TOPIC", TOPIC_DISINFO_REQUESTS).strip() or TOPIC_DISINFO_REQUESTS,
        os.environ.get("COWRIE_KAFKA_TOPIC", TOPIC_ALERTS).strip() or TOPIC_ALERTS,
        os.environ.get("COWRIE_KAFKA_REACT_TOPIC", TOPIC_REACT_ALERTS).strip() or TOPIC_REACT_ALERTS,
    ]
    # Preserve order, drop duplicates/empties.
    seen: set[str] = set()
    out: list[str] = []
    for topic in topics:
        if not topic or topic in seen:
            continue
        seen.add(topic)
        out.append(topic)
    return out


def ensure_pipeline_kafka_topics(
    *,
    bootstrap: Optional[str] = None,
    retries: int = 12,
    retry_delay_sec: float = 2.0,
) -> list[str]:
    """Create all standard pipeline topics if missing. Returns topic names ensured."""
    topics = pipeline_kafka_topics()
    ensure_kafka_topics(
        topics,
        bootstrap=bootstrap,
        retries=retries,
        retry_delay_sec=retry_delay_sec,
    )
    return topics


def ensure_kafka_topics(
    topics: Sequence[str],
    *,
    bootstrap: Optional[str] = None,
    retries: int = 12,
    retry_delay_sec: float = 2.0,
) -> None:
    """
    Create Kafka topics if they do not exist.

    Flink's Kafka source lists topic metadata at startup and fails when a topic
    is missing (auto-create on produce is too late for Phase 2/3 consumers).
    """
    servers = (bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")).strip()
    wanted = [t.strip() for t in topics if t and t.strip()]
    if not wanted:
        return

    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    last_error: Optional[Exception] = None
    for attempt in range(retries):
        admin: Optional[KafkaAdminClient] = None
        try:
            admin = KafkaAdminClient(bootstrap_servers=servers, client_id="cowrie-topic-init")
            existing = set(admin.list_topics())
            missing = [t for t in wanted if t not in existing]
            if not missing:
                return
            futures = admin.create_topics(
                [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in missing],
                validate_only=False,
            )
            for topic, future in futures.items():
                try:
                    future.result()
                    print(f"Created Kafka topic: {topic}")
                except TopicAlreadyExistsError:
                    pass
            return
        except Exception as exc:
            last_error = exc
            time.sleep(retry_delay_sec)
        finally:
            if admin is not None:
                try:
                    admin.close()
                except Exception:
                    pass

    raise RuntimeError(
        f"Could not ensure Kafka topics {wanted} on {servers}: {last_error}"
    )


def kafka_pipeline_active() -> bool:
    """
    True when Phase 1–3 Kafka sidecars own alert topics.

    When active, log-processor should not publish workflow alerts to ``cowrie.alerts``.
    """
    return _env_truthy("COWRIE_KAFKA_PIPELINE", "0")


def kafka_publish_enabled() -> bool:
    """Whether log-processor may publish alerts to Kafka at all."""
    if not _env_truthy("COWRIE_KAFKA_ENABLED", "0"):
        return False
    if _env_truthy("COWRIE_KAFKA_PUBLISH_ALERTS", "1") and kafka_pipeline_active():
        return False
    return True


def kafka_topic_for_alert(alert: Dict[str, Any]) -> str:
    """
    Route an alert dict to the correct Kafka topic.

    ReAct / LLM alerts → ``cowrie.react_alerts``; workflow → ``cowrie.alerts``.
    """
    source = str(alert.get("detection_source") or "").strip().lower()
    if source == DETECTION_REACT or str(alert.get("alert_id", "")).startswith("REACT-"):
        return os.environ.get("COWRIE_KAFKA_REACT_TOPIC", TOPIC_REACT_ALERTS).strip() or TOPIC_REACT_ALERTS
    return os.environ.get("COWRIE_KAFKA_TOPIC", TOPIC_ALERTS).strip() or TOPIC_ALERTS


def is_workflow_alert(obj: Dict[str, Any]) -> bool:
    source = str(obj.get("detection_source") or "").strip().lower()
    return source in (DETECTION_WORKFLOW, DETECTION_FLINK_AGENTS, "")


def is_react_alert(obj: Dict[str, Any]) -> bool:
    return str(obj.get("detection_source") or "").strip().lower() == DETECTION_REACT


def is_react_agent_alert(alert: Any) -> bool:
    """True when this alert was produced by the Cloudera ReAct agent (UI + tests)."""
    if not isinstance(alert, dict):
        return False
    if is_react_alert(alert):
        return True
    if str(alert.get("alert_id", "")).startswith("REACT-"):
        return True
    ad = alert.get("attack_details") or {}
    if isinstance(ad, dict) and (
        ad.get("react_reasoning") is not None
        or ad.get("react_confidence") is not None
        or ad.get("react_actions_taken")
    ):
        return True
    rec = str(alert.get("recommended_action") or "")
    if "cloudera react" in rec.lower() or rec.startswith("Cloudera ReAct"):
        return True
    return False


def hot_path_allows_react() -> bool:
    """
    Whether synchronous paths (log tail, per-event Flink jobs) may invoke Cloudera ReAct.

    Default **False**. ReAct belongs in the Phase 3 Kafka sidecar (async), not the hot path.
    Set ``COWRIE_ALLOW_REACT_ON_HOT_PATH=1`` only for lab/debug.
    """
    if _env_truthy("COWRIE_ALLOW_REACT_ON_HOT_PATH", "0"):
        return True
    return False


def resolve_hot_path_engine(*, cloudera_config_ok: Optional[bool] = None) -> str:
    """
    Select detection engine for the **hot path** (log-processor / synchronous Flink).

    Production rules:
    - When ``COWRIE_KAFKA_PIPELINE=1``, Phase 2/3 sidecars own detection → always ``workflow`` here.
    - Otherwise default ``workflow`` unless ``COWRIE_HOT_PATH_ENGINE`` / ``COWRIE_ALLOW_REACT_ON_HOT_PATH`` override.
    - ReAct for enrichment runs in ``kafka-react-augmentor`` (Phase 3), not inline with auto-block.
    """
    if kafka_pipeline_active() and not hot_path_allows_react():
        return "workflow"

    raw = (os.environ.get("COWRIE_HOT_PATH_ENGINE") or os.environ.get("COWRIE_COUNTER_ATTACK_ENGINE") or "workflow")
    raw = raw.strip().lower()
    if raw == "cloudera_react":
        raw = "react"
    if raw not in ("workflow", "react", "auto"):
        raw = "workflow"

    if raw == "workflow":
        return "workflow"

    if not hot_path_allows_react() and raw in ("react", "auto"):
        return "workflow"

    if cloudera_config_ok is None:
        cloudera_config_ok = _cloudera_config_ok_fallback()

    if raw == "react":
        return "react" if cloudera_config_ok else "workflow"

    return "react" if cloudera_config_ok else "workflow"


def _cloudera_config_ok_fallback() -> bool:
    try:
        from cloudera_llm_config import get_cloudera_config, validate_config

        return bool(validate_config(get_cloudera_config()))
    except Exception:
        base = (os.getenv("CLOUDERA_AI_BASE_URL") or "").strip()
        token = (os.getenv("CLOUDERA_JWT_TOKEN") or os.getenv("CLOUDERA_API_KEY") or "").strip()
        return bool(base.startswith("http") and len(token) >= 10)
