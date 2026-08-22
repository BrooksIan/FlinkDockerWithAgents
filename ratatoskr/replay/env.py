"""Backfill / replay job env gates (not heal)."""

from __future__ import annotations

import os

from ratatoskr.dataplane.env import (
    DATAPLANE_PHASES,
    dataplane_dry_run,
    dataplane_phase,
    dataplane_verify,
    phase_at_least,
)
from ratatoskr.dataplane.flow import GROUP_REPLAY
from ratatoskr.dataplane.topics import TOPIC_REPLAY_OUT, TOPIC_VALID


def replay_phase() -> str:
    """monitor = plan only; lab = execute. (safe unused — no intermediate.)"""
    raw = (os.environ.get("REPLAY_PHASE") or "").strip().lower()
    if raw in DATAPLANE_PHASES:
        return raw
    return dataplane_phase("DATAPLANE_PHASE")


def replay_dry_run() -> bool:
    if os.environ.get("REPLAY_DRY_RUN"):
        return dataplane_dry_run("REPLAY_DRY_RUN")
    return dataplane_dry_run("DATAPLANE_DRY_RUN")


def replay_verify() -> bool:
    if os.environ.get("REPLAY_VERIFY"):
        return dataplane_verify("REPLAY_VERIFY")
    return dataplane_verify("DATAPLANE_VERIFY")


def default_replay_source() -> str:
    return (os.environ.get("REPLAY_SOURCE_TOPIC") or TOPIC_VALID).strip()


def default_replay_dest() -> str:
    return (os.environ.get("REPLAY_DEST_TOPIC") or TOPIC_REPLAY_OUT).strip()


def default_replay_group() -> str:
    return (os.environ.get("REPLAY_GROUP") or GROUP_REPLAY).strip()


def default_replay_hours() -> float:
    raw = (os.environ.get("REPLAY_HOURS") or "1").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 1.0


def replay_catchup_timeout_sec() -> float:
    raw = (os.environ.get("REPLAY_CATCHUP_TIMEOUT_SEC") or "30").strip()
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 30.0


__all__ = [
    "DATAPLANE_PHASES",
    "default_replay_dest",
    "default_replay_group",
    "default_replay_hours",
    "default_replay_source",
    "phase_at_least",
    "replay_catchup_timeout_sec",
    "replay_dry_run",
    "replay_phase",
    "replay_verify",
]
