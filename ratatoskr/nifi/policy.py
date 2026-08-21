"""Deterministic heal policy for NiFi monitoring workflow agent."""

from __future__ import annotations

from typing import Any, Callable

from ratatoskr.nifi.client import NiFiClient, allow_empty_queue, heal_phase


def classify_health(health: dict[str, Any]) -> dict[str, Any]:
    """Derive a compact alert classification from get_flow_health_status()."""
    severities = list(health.get("severities") or [])
    level = "OK"
    if "BULLETIN_ERROR" in severities or "INVALID" in severities:
        level = "HIGH"
    elif "STOPPED" in severities or "BACKPRESSURE" in severities:
        level = "MEDIUM"
    elif severities:
        level = "LOW"
    return {
        "healthy": bool(health.get("healthy")),
        "level": level,
        "severities": severities,
        "summary": (
            "healthy"
            if not severities
            else ", ".join(severities)
        ),
    }


def apply_heal_policy(
    client: NiFiClient,
    health: dict[str, Any],
    *,
    phase: str | None = None,
) -> list[dict[str, Any]]:
    """
    Apply heal actions according to NIFI_HEAL_PHASE.

    - monitor: no mutations
    - safe: start STOPPED processors; enable DISABLED controller services
    - lab: safe + terminate INVALID stuck processors; empty queues if allowed
    """
    active = (phase or heal_phase()).lower()
    actions: list[dict[str, Any]] = []

    if active == "monitor":
        return actions

    if active in ("safe", "lab"):
        for proc in health.get("stopped_processors") or []:
            pid = proc.get("id")
            if not pid:
                continue
            version = (proc.get("revision") or {}).get("version")
            try:
                client.start_processor(pid, version)
                actions.append(
                    {
                        "op": "start_processor",
                        "id": pid,
                        "name": proc.get("name"),
                        "ok": True,
                    }
                )
            except Exception as exc:  # noqa: BLE001 — surface in OutputEvent
                actions.append(
                    {
                        "op": "start_processor",
                        "id": pid,
                        "name": proc.get("name"),
                        "ok": False,
                        "error": str(exc),
                    }
                )

        for svc in health.get("disabled_controller_services") or []:
            sid = svc.get("id")
            if not sid:
                continue
            version = (svc.get("revision") or {}).get("version")
            try:
                client.enable_controller_service(sid, version)
                actions.append(
                    {
                        "op": "enable_controller_service",
                        "id": sid,
                        "name": svc.get("name"),
                        "ok": True,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                actions.append(
                    {
                        "op": "enable_controller_service",
                        "id": sid,
                        "name": svc.get("name"),
                        "ok": False,
                        "error": str(exc),
                    }
                )

    if active == "lab":
        for proc in health.get("invalid_processors") or []:
            pid = proc.get("id")
            if not pid:
                continue
            version = (proc.get("revision") or {}).get("version")
            try:
                client.terminate_processor(pid, version)
                actions.append(
                    {
                        "op": "terminate_processor",
                        "id": pid,
                        "name": proc.get("name"),
                        "ok": True,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                actions.append(
                    {
                        "op": "terminate_processor",
                        "id": pid,
                        "name": proc.get("name"),
                        "ok": False,
                        "error": str(exc),
                    }
                )

        if allow_empty_queue():
            for conn in health.get("queued_connections") or []:
                cid = conn.get("id")
                if not cid:
                    continue
                try:
                    client.empty_connection_queue(cid)
                    actions.append(
                        {
                            "op": "empty_connection_queue",
                            "id": cid,
                            "name": conn.get("name"),
                            "ok": True,
                            "warning": "flowfiles permanently dropped",
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    actions.append(
                        {
                            "op": "empty_connection_queue",
                            "id": cid,
                            "name": conn.get("name"),
                            "ok": False,
                            "error": str(exc),
                        }
                    )

    return actions


def run_monitor_cycle(
    client: NiFiClient,
    process_group_id: str = "root",
    *,
    phase: str | None = None,
    client_factory: Callable[[], NiFiClient] | None = None,
) -> dict[str, Any]:
    """One poll → classify → optional heal cycle."""
    _ = client_factory  # reserved for tests
    health = client.get_flow_health_status(process_group_id)
    classification = classify_health(health)
    heal_actions = apply_heal_policy(client, health, phase=phase)
    return {
        "agent": "workflow_nifi_monitor",
        "phase": (phase or heal_phase()),
        "classification": classification,
        "health": {
            "process_group_id": health.get("process_group_id"),
            "healthy": health.get("healthy"),
            "severities": health.get("severities"),
            "counts": health.get("counts"),
            "stopped_processors": health.get("stopped_processors"),
            "invalid_processors": health.get("invalid_processors"),
            "disabled_controller_services": health.get("disabled_controller_services"),
            "queued_connections": health.get("queued_connections"),
            "bulletins": health.get("bulletins"),
            "stale_bulletins": health.get("stale_bulletins"),
        },
        "heal_actions": heal_actions,
        "mutations": list(client.mutations),
    }
