#!/usr/bin/env python3
"""Cross-stack heal env gates (NiFi ↔ Kafka coordinated playbooks)."""

from __future__ import annotations

import os

HEAL_PHASES = frozenset({"monitor", "lab"})


def _truthy(name: str, default: str = "") -> bool:
    return (os.environ.get(name) or default).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def cross_heal_phase() -> str:
    """monitor = correlate only; lab = run coordinated playbooks."""
    raw = (os.environ.get("CROSS_HEAL_PHASE") or "monitor").strip().lower()
    return raw if raw in HEAL_PHASES else "monitor"


def cross_heal_dry_run() -> bool:
    return _truthy("CROSS_HEAL_DRY_RUN")


def cross_heal_allow_empty_queue() -> bool:
    """Pass through to NiFi empty-queue during backpressure playbook."""
    return _truthy("CROSS_HEAL_ALLOW_EMPTY_QUEUE") or _truthy(
        "NIFI_HEAL_ALLOW_EMPTY_QUEUE"
    )


def demo_kafka_topic() -> str:
    return (os.environ.get("CROSS_HEAL_DEMO_TOPIC") or "nifi.kafka.demo").strip()


def demo_nifi_pg_name() -> str:
    return (
        os.environ.get("CROSS_HEAL_NIFI_PG") or "Ratatoskr Kafka Demo"
    ).strip()


def demo_consume_names() -> frozenset[str]:
    raw = (os.environ.get("CROSS_HEAL_CONSUME_NAMES") or "ConsumeKafka").strip()
    return frozenset(p.strip() for p in raw.split(",") if p.strip())
