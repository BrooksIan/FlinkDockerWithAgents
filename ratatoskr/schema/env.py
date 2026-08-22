"""Schema / contract gate env gates (separate from heal)."""

from __future__ import annotations

import os

from ratatoskr.dataplane.env import (
    DATAPLANE_PHASES,
    dataplane_cooldown_sec,
    dataplane_dry_run,
    dataplane_max_mutations,
    dataplane_phase,
    dataplane_verify,
    phase_at_least,
)

HEAL_LIKE_OPS = frozenset(
    {
        "start_processor",
        "stop_processor",
        "restart_processor",
        "terminate_processor",
        "empty_connection_queue",
        "enable_controller_service",
        "disable_controller_service",
    }
)

ALLOWED_OPS = frozenset({"ensure_topics", "update_schema_text", "ensure_flow"})


def schema_phase() -> str:
    raw = (os.environ.get("SCHEMA_GATE_PHASE") or "").strip().lower()
    if raw in DATAPLANE_PHASES:
        return raw
    return dataplane_phase("DATAPLANE_PHASE")


def schema_dry_run() -> bool:
    if os.environ.get("SCHEMA_GATE_DRY_RUN"):
        return dataplane_dry_run("SCHEMA_GATE_DRY_RUN")
    return dataplane_dry_run("DATAPLANE_DRY_RUN")


def schema_verify() -> bool:
    if os.environ.get("SCHEMA_GATE_VERIFY"):
        return dataplane_verify("SCHEMA_GATE_VERIFY")
    return dataplane_verify("DATAPLANE_VERIFY")


def schema_max_mutations() -> int:
    if os.environ.get("SCHEMA_GATE_MAX_MUTATIONS"):
        return dataplane_max_mutations("SCHEMA_GATE_MAX_MUTATIONS")
    return dataplane_max_mutations("DATAPLANE_MAX_MUTATIONS")


def schema_cooldown_sec() -> float:
    if os.environ.get("SCHEMA_GATE_COOLDOWN_SEC"):
        return dataplane_cooldown_sec("SCHEMA_GATE_COOLDOWN_SEC")
    return dataplane_cooldown_sec("DATAPLANE_COOLDOWN_SEC")


def lab_schema_text() -> str | None:
    """Optional override for lab schema swap (else LAB_JSON_SCHEMA)."""
    raw = (os.environ.get("SCHEMA_GATE_LAB_SCHEMA") or "").strip()
    return raw or None


__all__ = [
    "ALLOWED_OPS",
    "DATAPLANE_PHASES",
    "HEAL_LIKE_OPS",
    "lab_schema_text",
    "phase_at_least",
    "schema_cooldown_sec",
    "schema_dry_run",
    "schema_max_mutations",
    "schema_phase",
    "schema_verify",
]
