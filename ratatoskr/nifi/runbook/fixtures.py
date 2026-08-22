"""NiFi monitor OutputEvent fixtures for runbook Phase 0 (no live NiFi)."""

from __future__ import annotations

from typing import Any


def _base_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "agent": "workflow_nifi_monitor",
        "poll_id": "fixture-poll",
        "ts": "2026-01-01T00:00:00+00:00",
        "phase": "monitor",
        "classification": {
            "healthy": True,
            "level": "OK",
            "score": 100,
            "severities": [],
            "summary": "healthy",
            "bulletin_groups": [],
        },
        "delta": None,
        "health": {
            "process_group_id": "pg-fixture",
            "healthy": True,
            "severities": [],
            "counts": {"processors": 3, "connections": 2, "controller_services": 1},
            "stopped_processors": [],
            "invalid_processors": [],
            "disabled_controller_services": [],
            "queued_connections": [],
            "bulletins": [],
            "stale_bulletins": [],
            "probe": {"ok": True, "poll_ms": 1.0},
        },
        "heal_plan": [],
        "heal_actions": [],
        "audit": {"phase": "monitor", "dry_run": False, "mutations": []},
        "mutations": [],
    }
    event.update(overrides)
    return event


def fixture_healthy() -> dict[str, Any]:
    return _base_event()


def fixture_stop_generate() -> dict[str, Any]:
    """Mirrors ``nifi_fault_inject.py --stop-generate``."""
    return _base_event(
        poll_id="fixture-stop-generate",
        classification={
            "healthy": False,
            "level": "MEDIUM",
            "score": 80,
            "severities": ["STOPPED"],
            "summary": "STOPPED",
            "bulletin_groups": [],
        },
        health={
            "process_group_id": "pg-fixture",
            "healthy": False,
            "severities": ["STOPPED"],
            "counts": {"processors": 3, "connections": 2, "controller_services": 0},
            "stopped_processors": [
                {
                    "id": "proc-generate",
                    "name": "GenerateFlowFile",
                    "state": "STOPPED",
                    "revision": {"version": 1},
                }
            ],
            "invalid_processors": [],
            "disabled_controller_services": [],
            "queued_connections": [],
            "bulletins": [],
            "stale_bulletins": [],
            "probe": {"ok": True},
        },
        heal_plan=[
            {
                "op": "start_processor",
                "id": "proc-generate",
                "name": "GenerateFlowFile",
                "proposed": True,
            }
        ],
    )


def fixture_invalid_log() -> dict[str, Any]:
    """Mirrors ``nifi_fault_inject.py --invalid-log``."""
    return _base_event(
        poll_id="fixture-invalid-log",
        classification={
            "healthy": False,
            "level": "HIGH",
            "score": 70,
            "severities": ["INVALID", "STOPPED"],
            "summary": "INVALID, STOPPED",
            "bulletin_groups": [],
        },
        health={
            "process_group_id": "pg-fixture",
            "healthy": False,
            "severities": ["INVALID", "STOPPED"],
            "counts": {"processors": 3, "connections": 2, "controller_services": 0},
            "stopped_processors": [
                {
                    "id": "proc-log",
                    "name": "LogAttribute",
                    "state": "STOPPED",
                    "validationStatus": "INVALID",
                    "revision": {"version": 2},
                }
            ],
            "invalid_processors": [
                {
                    "id": "proc-log",
                    "name": "LogAttribute",
                    "state": "STOPPED",
                    "validationStatus": "INVALID",
                    "revision": {"version": 2},
                }
            ],
            "disabled_controller_services": [],
            "queued_connections": [],
            "bulletins": [],
            "stale_bulletins": [],
            "probe": {"ok": True},
        },
        heal_plan=[
            {
                "op": "fix_processor_config",
                "id": "proc-log",
                "name": "LogAttribute",
                "proposed": True,
            },
            {
                "op": "terminate_processor",
                "id": "proc-log",
                "name": "LogAttribute",
                "proposed": True,
            },
        ],
    )


def fixture_queue_backlog() -> dict[str, Any]:
    """Mirrors ``nifi_fault_inject.py --queue-backlog``."""
    return _base_event(
        poll_id="fixture-queue-backlog",
        classification={
            "healthy": False,
            "level": "HIGH",
            "score": 65,
            "severities": ["BACKPRESSURE_CRIT", "BACKPRESSURE", "STOPPED"],
            "summary": "BACKPRESSURE_CRIT, BACKPRESSURE, STOPPED",
            "bulletin_groups": [],
        },
        health={
            "process_group_id": "pg-fixture",
            "healthy": False,
            "severities": ["BACKPRESSURE_CRIT", "BACKPRESSURE", "STOPPED"],
            "counts": {"processors": 3, "connections": 2, "controller_services": 0},
            "stopped_processors": [
                {
                    "id": "proc-log",
                    "name": "LogAttribute",
                    "state": "STOPPED",
                    "revision": {"version": 1},
                }
            ],
            "invalid_processors": [],
            "disabled_controller_services": [],
            "queued_connections": [
                {
                    "id": "conn-update-to-log",
                    "name": "update-to-log",
                    "flowFilesQueued": 120,
                    "backpressure_level": "crit",
                    "sourceId": "proc-update",
                    "sourceName": "UpdateAttribute",
                }
            ],
            "bulletins": [],
            "stale_bulletins": [],
            "probe": {"ok": True},
        },
        heal_plan=[
            {
                "op": "start_processor",
                "id": "proc-log",
                "name": "LogAttribute",
                "proposed": True,
            },
            {
                "op": "stop_processor",
                "id": "proc-update",
                "name": "UpdateAttribute",
                "proposed": True,
                "reason": "safer_queue_relief",
            },
            {
                "op": "empty_connection_queue",
                "id": "conn-update-to-log",
                "name": "update-to-log",
                "proposed": True,
            },
        ],
    )


def fixture_stop_consume() -> dict[str, Any]:
    """Mirrors kafka-flow fault ``--target kafka --stop-consume``."""
    return _base_event(
        poll_id="fixture-stop-consume",
        classification={
            "healthy": False,
            "level": "MEDIUM",
            "score": 80,
            "severities": ["STOPPED"],
            "summary": "STOPPED",
            "bulletin_groups": [],
        },
        health={
            "process_group_id": "pg-kafka-demo",
            "healthy": False,
            "severities": ["STOPPED"],
            "counts": {"processors": 3, "connections": 2, "controller_services": 1},
            "stopped_processors": [
                {
                    "id": "proc-consume",
                    "name": "ConsumeKafka",
                    "state": "STOPPED",
                    "revision": {"version": 4},
                }
            ],
            "invalid_processors": [],
            "disabled_controller_services": [],
            "queued_connections": [],
            "bulletins": [],
            "stale_bulletins": [],
            "probe": {"ok": True},
        },
        heal_plan=[
            {
                "op": "start_processor",
                "id": "proc-consume",
                "name": "ConsumeKafka",
                "proposed": True,
            }
        ],
    )


def fixture_nifi_unreachable() -> dict[str, Any]:
    return _base_event(
        poll_id="fixture-unreachable",
        classification={
            "healthy": False,
            "level": "HIGH",
            "score": 0,
            "severities": ["NIFI_UNREACHABLE"],
            "summary": "NIFI_UNREACHABLE",
            "bulletin_groups": [],
        },
        health={
            "process_group_id": None,
            "healthy": False,
            "severities": ["NIFI_UNREACHABLE"],
            "counts": {},
            "stopped_processors": [],
            "invalid_processors": [],
            "disabled_controller_services": [],
            "queued_connections": [],
            "bulletins": [],
            "stale_bulletins": [],
            "probe": {"ok": False, "error": "connection refused"},
        },
        heal_plan=[],
    )


FIXTURE_PACKS: dict[str, Any] = {
    "healthy": fixture_healthy,
    "stop-generate": fixture_stop_generate,
    "invalid-log": fixture_invalid_log,
    "queue-backlog": fixture_queue_backlog,
    "stop-consume": fixture_stop_consume,
    "nifi-unreachable": fixture_nifi_unreachable,
}


def list_fixture_ids() -> list[str]:
    return sorted(FIXTURE_PACKS.keys())


def load_fixture(fixture_id: str) -> dict[str, Any]:
    key = fixture_id.strip().lower()
    if key not in FIXTURE_PACKS:
        raise KeyError(f"Unknown fixture {fixture_id!r}; choose from {list_fixture_ids()}")
    return FIXTURE_PACKS[key]()
