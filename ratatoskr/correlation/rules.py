"""Cross-signal correlation rules for NiFi ↔ Kafka monitor OutputEvents."""

from __future__ import annotations

from typing import Any

# (nifi_severities_any, kafka_severities_any, rule_id, level, title)
CORRELATION_RULES: list[dict[str, Any]] = [
    {
        "id": "pipeline_backpressure_lag",
        "nifi_any": frozenset(
            {"BACKPRESSURE", "BACKPRESSURE_WARN", "BACKPRESSURE_CRIT"}
        ),
        "kafka_any": frozenset({"LAG_WARN", "LAG_CRIT", "CONSUMER_STALLED"}),
        "level": "HIGH",
        "title": "NiFi queue pressure with Kafka consumer lag",
        "hint": "Flow is buffering while downstream consumers fall behind — check sink processors and consumer groups.",
    },
    {
        "id": "dual_unreachable",
        "nifi_any": frozenset({"NIFI_UNREACHABLE"}),
        "kafka_any": frozenset({"BROKER_UNREACHABLE"}),
        "level": "HIGH",
        "title": "NiFi API and Kafka broker both unreachable",
        "hint": "Likely shared infra / Docker network outage — verify compose stack and host networking.",
    },
    {
        "id": "nifi_stopped_kafka_lag",
        "nifi_any": frozenset({"STOPPED", "DISABLED_SERVICE"}),
        "kafka_any": frozenset({"LAG_WARN", "LAG_CRIT", "CONSUMER_STALLED", "GROUP_EMPTY"}),
        "level": "HIGH",
        "title": "Stopped NiFi components with Kafka lag",
        "hint": "Upstream processors may be stopped while consumers wait on stale offsets or empty input.",
    },
    {
        "id": "nifi_invalid_kafka_missing",
        "nifi_any": frozenset({"INVALID", "BULLETIN_ERROR"}),
        "kafka_any": frozenset({"TOPIC_MISSING"}),
        "level": "MEDIUM",
        "title": "Invalid NiFi flow with missing Kafka catalog topics",
        "hint": "Misconfigured processors plus absent Studio topics — fix flow validation and ensure kafka-init topics.",
    },
    {
        "id": "kafka_topic_nifi_consumer",
        "nifi_any": frozenset(
            {"STOPPED", "INVALID", "BULLETIN_ERROR", "DISABLED_SERVICE"}
        ),
        "kafka_any": frozenset({"TOPIC_MISSING"}),
        "level": "HIGH",
        "title": "Missing Kafka topic with unhealthy NiFi consumer path",
        "hint": "Cross-stack: recreate catalog topic, then start ConsumeKafka / enable CS.",
    },
    {
        "id": "stack_degraded",
        "nifi_any": frozenset(
            {
                "STOPPED",
                "INVALID",
                "BACKPRESSURE",
                "BACKPRESSURE_WARN",
                "BACKPRESSURE_CRIT",
                "BULLETIN_ERROR",
                "DISABLED_SERVICE",
                "NIFI_SLOW",
            }
        ),
        "kafka_any": frozenset(
            {
                "TOPIC_MISSING",
                "LAG_WARN",
                "LAG_CRIT",
                "BROKER_SLOW",
                "UNDER_REPLICATED",
                "OFFLINE_PARTITION",
            }
        ),
        "level": "MEDIUM",
        "title": "Combined NiFi and Kafka degradation",
        "hint": "Both sides report non-OK severities — triage the higher individual score first.",
        # Only fire if no more specific rule matched (handled in engine)
        "fallback": True,
    },
]

# Data-plane observe-only rules (schema gate / route enrich OutputEvents).
DATAPLANE_CORRELATION_RULES: list[dict[str, Any]] = [
    {
        "id": "schema_violation_spike",
        "schema_any": frozenset({"SCHEMA_VIOLATIONS"}),
        "level": "MEDIUM",
        "title": "Schema violations quarantined on schema.violations",
        "hint": "Bad events are gated — inspect samples, tighten producers, or propose a schema fix via dataplane.propose.",
    },
    {
        "id": "route_config_drift",
        "route_any": frozenset({"ROUTE_DRIFT:EnrichUpdate", "ROUTE_DRIFT:RouteType"}),
        "route_prefix": "ROUTE_DRIFT:",
        "level": "MEDIUM",
        "title": "Route/enrich desired-state drifts from live NiFi properties",
        "hint": "Publish a route proposal and ack on dataplane.ack before applying property patches.",
    },
    {
        "id": "schema_violations_with_lag",
        "schema_any": frozenset({"SCHEMA_VIOLATIONS"}),
        "kafka_any": frozenset({"LAG_WARN", "LAG_CRIT", "CONSUMER_STALLED"}),
        "level": "HIGH",
        "title": "Schema violations with Kafka consumer lag",
        "hint": "Invalid traffic may be starving valid downstream consumers — check quarantine volume and consumer groups.",
    },
]

_LEVEL_RANK = {"OK": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}


def level_max(*levels: str) -> str:
    best = "OK"
    for level in levels:
        if _LEVEL_RANK.get(level, 0) > _LEVEL_RANK.get(best, 0):
            best = level
    return best
