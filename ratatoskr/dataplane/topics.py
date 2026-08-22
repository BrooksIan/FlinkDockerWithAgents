"""Canonical data-plane Kafka topic names."""

from __future__ import annotations

TOPIC_RAW = "events.raw"
TOPIC_VALID = "events.valid"
TOPIC_VIOLATIONS = "schema.violations"
TOPIC_ENRICHED = "events.enriched"
TOPIC_REPLAY_OUT = "events.replay.out"
TOPIC_PROPOSE = "dataplane.propose"
TOPIC_ACK = "dataplane.ack"

TOPICS: tuple[str, ...] = (
    TOPIC_RAW,
    TOPIC_VALID,
    TOPIC_VIOLATIONS,
    TOPIC_ENRICHED,
    TOPIC_REPLAY_OUT,
    TOPIC_PROPOSE,
    TOPIC_ACK,
)
