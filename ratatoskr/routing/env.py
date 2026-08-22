"""Routing / enrichment env gates."""

from __future__ import annotations

import os

from ratatoskr.dataplane.env import (
    DATAPLANE_PHASES,
    dataplane_dry_run,
    dataplane_max_mutations,
    dataplane_phase,
    dataplane_verify,
    phase_at_least,
)

# Allowlisted NiFi property keys the agent may patch.
SAFE_ENRICH_KEYS = frozenset(
    {
        "ratatoskr.env",
        "ratatoskr.pipeline",
        "event.type",
    }
)
LAB_ENRICH_KEYS = SAFE_ENRICH_KEYS | frozenset(
    {
        "ratatoskr.region",
        "ratatoskr.team",
    }
)
SAFE_ROUTE_KEYS = frozenset(
    {
        "Routing Strategy",
        "enriched",
    }
)
LAB_ROUTE_KEYS = SAFE_ROUTE_KEYS | frozenset(
    {
        "alert",
        "metrics",
    }
)


def route_phase() -> str:
    raw = (os.environ.get("ROUTE_PHASE") or "").strip().lower()
    if raw in DATAPLANE_PHASES:
        return raw
    return dataplane_phase("DATAPLANE_PHASE")


def route_dry_run() -> bool:
    if os.environ.get("ROUTE_DRY_RUN"):
        return dataplane_dry_run("ROUTE_DRY_RUN")
    return dataplane_dry_run("DATAPLANE_DRY_RUN")


def route_verify() -> bool:
    if os.environ.get("ROUTE_VERIFY"):
        return dataplane_verify("ROUTE_VERIFY")
    return dataplane_verify("DATAPLANE_VERIFY")


def route_max_mutations() -> int:
    if os.environ.get("ROUTE_MAX_MUTATIONS"):
        return dataplane_max_mutations("ROUTE_MAX_MUTATIONS")
    return dataplane_max_mutations("DATAPLANE_MAX_MUTATIONS")


__all__ = [
    "DATAPLANE_PHASES",
    "LAB_ENRICH_KEYS",
    "LAB_ROUTE_KEYS",
    "SAFE_ENRICH_KEYS",
    "SAFE_ROUTE_KEYS",
    "phase_at_least",
    "route_dry_run",
    "route_max_mutations",
    "route_phase",
    "route_verify",
]
