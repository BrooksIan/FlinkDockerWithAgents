"""Deterministic heal policy for NiFi monitoring workflow agent."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from ratatoskr.nifi.client import NiFiClient
from ratatoskr.nifi.env import (
    allow_empty_queue,
    empty_queue_min_flowfiles,
    heal_allow_ids,
    heal_allow_name_regex,
    heal_cooldown_sec,
    heal_dry_run,
    heal_max_mutations,
    heal_phase,
    heal_verify,
    phase_at_least,
)

# Severity → score penalty (100 = perfect). Caps at 0.
_SEVERITY_PENALTIES: dict[str, int] = {
    "NIFI_UNREACHABLE": 100,
    "BULLETIN_ERROR": 35,
    "INVALID": 30,
    "BACKPRESSURE_CRIT": 25,
    "STOPPED": 20,
    "DISABLED_SERVICE": 15,
    "BACKPRESSURE_WARN": 10,
    "BACKPRESSURE": 10,
    "NIFI_SLOW": 5,
}

_HIGH = frozenset(
    {"BULLETIN_ERROR", "INVALID", "NIFI_UNREACHABLE", "BACKPRESSURE_CRIT"}
)
_MEDIUM = frozenset(
    {"STOPPED", "BACKPRESSURE", "BACKPRESSURE_WARN", "DISABLED_SERVICE"}
)

# Declarative heal rules (order = execute order).
# min_phase: monitor < safe < lab
HEAL_RULES: list[dict[str, Any]] = [
    {
        "op": "enable_controller_service",
        "min_phase": "safe",
        "source": "disabled_controller_services",
    },
    {
        "op": "start_processor",
        "min_phase": "safe",
        "source": "stopped_processors",
        "skip_if_invalid": True,
    },
    {
        "op": "stop_processor",
        "min_phase": "lab",
        "source": "backpressure_sources",
        "reason": "safer_queue_relief",
    },
    {
        "op": "terminate_processor",
        "min_phase": "lab",
        "source": "invalid_processors",
    },
    {
        "op": "empty_connection_queue",
        "min_phase": "lab",
        "source": "queued_connections",
        "requires_allow_empty": True,
        "reason": "destructive_queue_drain",
    },
]

# (op, id) → monotonic timestamp of last successful/proposed apply
_COOLDOWN: dict[tuple[str, str], float] = {}


def reset_heal_cooldown() -> None:
    """Clear flap-guard state (tests / new sessions)."""
    _COOLDOWN.clear()


def _entity_ids(health: dict[str, Any], key: str) -> set[str]:
    ids: set[str] = set()
    for item in health.get(key) or []:
        eid = item.get("id")
        if eid:
            ids.add(str(eid))
    return ids


def _bulletin_groups(health: dict[str, Any]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for b in health.get("bulletins") or []:
        fp = b.get("fingerprint")
        if not fp:
            continue
        g = groups.get(fp)
        if g is None:
            groups[fp] = {
                "fingerprint": fp,
                "count": 1,
                "level": b.get("level"),
                "sourceId": b.get("sourceId"),
                "sourceName": b.get("sourceName"),
                "message": b.get("message"),
            }
        else:
            g["count"] = int(g["count"]) + 1
    return list(groups.values())


def classify_health(health: dict[str, Any]) -> dict[str, Any]:
    """Derive alert classification, score, and bulletin fingerprints."""
    severities = list(health.get("severities") or [])
    level = "OK"
    if any(s in _HIGH for s in severities):
        level = "HIGH"
    elif any(s in _MEDIUM for s in severities):
        level = "MEDIUM"
    elif severities:
        level = "LOW"

    penalty = 0
    seen: set[str] = set()
    has_graded_bp = (
        "BACKPRESSURE_WARN" in severities or "BACKPRESSURE_CRIT" in severities
    )
    for s in severities:
        if s in seen:
            continue
        if s == "BACKPRESSURE" and has_graded_bp:
            continue
        seen.add(s)
        penalty += _SEVERITY_PENALTIES.get(s, 5)
    score = max(0, 100 - penalty)

    healthy = bool(health.get("healthy")) and not severities
    return {
        "healthy": healthy,
        "level": level,
        "score": score,
        "severities": severities,
        "summary": ("healthy" if not severities else ", ".join(severities)),
        "bulletin_groups": _bulletin_groups(health),
    }


def diff_health(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> dict[str, Any]:
    """Compare two health snapshots → new / persistent / resolved entity ids."""
    keys = (
        "stopped_processors",
        "invalid_processors",
        "disabled_controller_services",
        "queued_connections",
    )
    prev = previous or {}
    new: dict[str, list[str]] = {}
    persistent: dict[str, list[str]] = {}
    resolved: dict[str, list[str]] = {}

    prev_sev = set(prev.get("severities") or [])
    curr_sev = set(current.get("severities") or [])

    for key in keys:
        p_ids = _entity_ids(prev, key)
        c_ids = _entity_ids(current, key)
        n = sorted(c_ids - p_ids)
        p = sorted(c_ids & p_ids)
        r = sorted(p_ids - c_ids)
        if n:
            new[key] = n
        if p:
            persistent[key] = p
        if r:
            resolved[key] = r

    return {
        "new": new,
        "persistent": persistent,
        "resolved": resolved,
        "severities_new": sorted(curr_sev - prev_sev),
        "severities_resolved": sorted(prev_sev - curr_sev),
    }


def _invalid_ids(health: dict[str, Any]) -> set[str]:
    ids = _entity_ids(health, "invalid_processors")
    for proc in health.get("stopped_processors") or []:
        if (proc.get("validationStatus") or "").upper() == "INVALID" and proc.get("id"):
            ids.add(str(proc["id"]))
    return ids


def _backpressure_source_entities(health: dict[str, Any]) -> list[dict[str, Any]]:
    """Upstream processors feeding queued connections (safer relief targets)."""
    stopped = _entity_ids(health, "stopped_processors")
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for conn in health.get("queued_connections") or []:
        sid = conn.get("sourceId")
        if not sid or sid in seen or sid in stopped:
            continue
        seen.add(str(sid))
        out.append(
            {
                "id": sid,
                "name": conn.get("sourceName") or sid,
                "revision": None,
                "from_connection": conn.get("id"),
            }
        )
    return out


def _source_entities(health: dict[str, Any], source: str) -> list[dict[str, Any]]:
    if source == "backpressure_sources":
        return _backpressure_source_entities(health)
    return list(health.get(source) or [])


def build_heal_plan(
    health: dict[str, Any],
    *,
    phase: str | None = None,
) -> list[dict[str, Any]]:
    """Build ordered heal proposals from HEAL_RULES (no mutations yet)."""
    active = (phase or heal_phase()).lower()
    if active == "monitor" or not phase_at_least(active, "safe"):
        return []

    invalid = _invalid_ids(health)
    empty_min = empty_queue_min_flowfiles()
    plan: list[dict[str, Any]] = []

    for rule in HEAL_RULES:
        min_phase = str(rule.get("min_phase") or "lab")
        if not phase_at_least(active, min_phase):
            continue
        if rule.get("requires_allow_empty") and not allow_empty_queue():
            continue

        op = str(rule["op"])
        for ent in _source_entities(health, str(rule["source"])):
            eid = ent.get("id")
            if not eid:
                continue
            if rule.get("skip_if_invalid") and str(eid) in invalid:
                continue
            if op == "empty_connection_queue":
                queued = int(ent.get("flowFilesQueued") or 0)
                if queued < empty_min:
                    continue

            version = (ent.get("revision") or {}).get("version")
            item: dict[str, Any] = {
                "op": op,
                "id": eid,
                "name": ent.get("name"),
                "version": version,
                "proposed": True,
            }
            if rule.get("reason"):
                item["reason"] = rule["reason"]
            if ent.get("from_connection"):
                item["from_connection"] = ent["from_connection"]
            plan.append(item)

    return plan


def _allowlisted(action: dict[str, Any]) -> bool:
    ids = heal_allow_ids()
    name_re = heal_allow_name_regex()
    if ids is None and name_re is None:
        return True
    eid = str(action.get("id") or "")
    name = str(action.get("name") or "")
    ok_id = ids is not None and eid in ids
    ok_name = name_re is not None and bool(name_re.search(name))
    if ids is not None and name_re is not None:
        return ok_id or ok_name
    if ids is not None:
        return ok_id
    return ok_name


def _cooldown_blocked(op: str, eid: str, now: float) -> bool:
    cd = heal_cooldown_sec()
    if cd <= 0:
        return False
    last = _COOLDOWN.get((op, eid))
    if last is None:
        return False
    return (now - last) < cd


def _mark_cooldown(op: str, eid: str, now: float) -> None:
    if heal_cooldown_sec() > 0:
        _COOLDOWN[(op, eid)] = now


def _execute_action(client: NiFiClient, action: dict[str, Any]) -> dict[str, Any]:
    op = action["op"]
    eid = action["id"]
    version = action.get("version")
    out = {k: v for k, v in action.items() if k not in ("proposed", "version")}
    out["proposed"] = False
    try:
        if op == "start_processor":
            client.start_processor(eid, version)
        elif op == "stop_processor":
            client.stop_processor(eid, version)
        elif op == "enable_controller_service":
            client.enable_controller_service(eid, version)
        elif op == "terminate_processor":
            client.terminate_processor(eid, version)
        elif op == "empty_connection_queue":
            client.empty_connection_queue(eid)
            out["warning"] = "flowfiles permanently dropped"
        else:
            out["ok"] = False
            out["error"] = f"unknown op {op}"
            return out
        out["ok"] = True
    except Exception as exc:  # noqa: BLE001 — surface in OutputEvent
        out["ok"] = False
        out["error"] = str(exc)
    return out


def _verify_action(action: dict[str, Any], health_after: dict[str, Any]) -> bool:
    """True if the problem entity is no longer present in the relevant list."""
    op = action.get("op")
    eid = str(action.get("id") or "")
    if not eid or not action.get("ok"):
        return False
    if op == "start_processor":
        return eid not in _entity_ids(health_after, "stopped_processors")
    if op == "enable_controller_service":
        return eid not in _entity_ids(health_after, "disabled_controller_services")
    if op == "terminate_processor":
        return eid not in _entity_ids(health_after, "invalid_processors")
    if op == "empty_connection_queue":
        return eid not in _entity_ids(health_after, "queued_connections")
    if op == "stop_processor":
        # Verified if source no longer appears as a backpressure feeder or is stopped
        return eid in _entity_ids(health_after, "stopped_processors") or eid not in {
            str(c.get("sourceId"))
            for c in (health_after.get("queued_connections") or [])
            if c.get("sourceId")
        }
    return False


def apply_heal_policy(
    client: NiFiClient,
    health: dict[str, Any],
    *,
    phase: str | None = None,
    dry_run: bool | None = None,
    verify: bool | None = None,
    process_group_id: str | None = None,
) -> list[dict[str, Any]]:
    """
    Build plan → filter (allowlist, cooldown, blast) → dry-run or execute → verify.

    - monitor: no mutations
    - safe: enable services, start processors
    - lab: safe + stop upstream (queue relief), terminate invalid, empty queues if allowed
    """
    active = (phase or heal_phase()).lower()
    is_dry = heal_dry_run() if dry_run is None else dry_run
    do_verify = heal_verify() if verify is None else verify
    plan = build_heal_plan(health, phase=active)
    if not plan:
        return []

    now = time.monotonic()
    max_mut = heal_max_mutations()
    actions: list[dict[str, Any]] = []
    executed = 0

    for proposed in plan:
        op = str(proposed["op"])
        eid = str(proposed["id"])

        if not _allowlisted(proposed):
            actions.append(
                {
                    **{k: v for k, v in proposed.items() if k != "proposed"},
                    "ok": False,
                    "skipped": "allowlist",
                    "proposed": is_dry,
                }
            )
            continue

        if _cooldown_blocked(op, eid, now):
            actions.append(
                {
                    **{k: v for k, v in proposed.items() if k != "proposed"},
                    "ok": False,
                    "skipped": "cooldown",
                    "proposed": is_dry,
                }
            )
            continue

        if max_mut and executed >= max_mut:
            actions.append(
                {
                    **{k: v for k, v in proposed.items() if k != "proposed"},
                    "ok": False,
                    "skipped": "blast_radius",
                    "proposed": is_dry,
                }
            )
            continue

        if is_dry:
            actions.append(
                {
                    **proposed,
                    "ok": None,
                    "proposed": True,
                }
            )
            executed += 1
            _mark_cooldown(op, eid, now)
            continue

        result = _execute_action(client, proposed)
        actions.append(result)
        if result.get("ok"):
            executed += 1
            _mark_cooldown(op, eid, now)

    if do_verify and not is_dry and any(a.get("ok") for a in actions):
        pg = process_group_id or health.get("process_group_id") or "root"
        try:
            after = client.get_flow_health_status(str(pg))
        except Exception as exc:  # noqa: BLE001
            for a in actions:
                if a.get("ok"):
                    a["verified"] = False
                    a["verify_error"] = str(exc)
            return actions
        for a in actions:
            if a.get("ok"):
                a["verified"] = _verify_action(a, after)

    return actions


def _unreachable_health(exc: BaseException) -> dict[str, Any]:
    return {
        "process_group_id": None,
        "healthy": False,
        "severities": ["NIFI_UNREACHABLE"],
        "stopped_processors": [],
        "invalid_processors": [],
        "disabled_controller_services": [],
        "queued_connections": [],
        "bulletins": [],
        "stale_bulletins": [],
        "probe": {"ok": False, "error": str(exc)},
        "counts": {},
    }


def run_monitor_cycle(
    client: NiFiClient,
    process_group_id: str = "root",
    *,
    phase: str | None = None,
    previous_health: dict[str, Any] | None = None,
    dry_run: bool | None = None,
    verify: bool | None = None,
    client_factory: Callable[[], NiFiClient] | None = None,
) -> dict[str, Any]:
    """One poll → classify → diff → plan → optional heal cycle."""
    _ = client_factory  # reserved for tests
    poll_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat()
    active = phase or heal_phase()
    is_dry = heal_dry_run() if dry_run is None else dry_run

    try:
        health = client.get_flow_health_status(process_group_id)
    except Exception as exc:  # noqa: BLE001
        health = _unreachable_health(exc)

    classification = classify_health(health)
    delta = diff_health(previous_health, health) if previous_health is not None else None
    heal_plan = build_heal_plan(health, phase=active)
    health_before = health
    heal_actions = apply_heal_policy(
        client,
        health,
        phase=active,
        dry_run=is_dry,
        verify=verify,
        process_group_id=str(health.get("process_group_id") or process_group_id),
    )

    if not is_dry and any(a.get("ok") for a in heal_actions):
        try:
            health = client.get_flow_health_status(
                str(health.get("process_group_id") or process_group_id)
            )
            classification = classify_health(health)
            if previous_health is not None:
                delta = diff_health(previous_health, health)
            else:
                delta = diff_health(health_before, health)
        except Exception:  # noqa: BLE001
            pass

    health_out = {
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
        "probe": health.get("probe"),
    }

    return {
        "agent": "workflow_nifi_monitor",
        "poll_id": poll_id,
        "ts": ts,
        "phase": active,
        "classification": classification,
        "delta": delta,
        "health": health_out,
        "heal_plan": heal_plan,
        "heal_actions": heal_actions,
        "audit": {
            "poll_id": poll_id,
            "phase": active,
            "dry_run": bool(is_dry),
            "mutations": list(client.mutations),
            "action_count": len(heal_actions),
            "executed_ok": sum(1 for a in heal_actions if a.get("ok") is True),
        },
        "mutations": list(client.mutations),
    }
