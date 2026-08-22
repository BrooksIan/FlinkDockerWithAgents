"""Route / enrich — propose property patches; NiFi executes via update_processor_config."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from ratatoskr.dataplane.flow import (
    find_dataplane_pg_id,
    processors_by_name,
)
from ratatoskr.routing.env import (
    LAB_ENRICH_KEYS,
    LAB_ROUTE_KEYS,
    SAFE_ENRICH_KEYS,
    SAFE_ROUTE_KEYS,
    phase_at_least,
    route_dry_run,
    route_max_mutations,
    route_phase,
    route_verify,
)

DEFAULT_RULE: dict[str, Any] = {
    "match": {"type": "order"},
    "set": {"env": "lab", "pipeline": "dataplane"},
    "route": "enriched",
}


def rules_to_properties(rule: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Map declarative rule → EnrichUpdate / RouteType property patches."""
    match = rule.get("match") if isinstance(rule.get("match"), dict) else {}
    sets = rule.get("set") if isinstance(rule.get("set"), dict) else {}
    route_name = str(rule.get("route") or "enriched")
    event_type = str(match.get("type") or "order")

    enrich: dict[str, str] = {
        "event.type": "${type}",
    }
    if "env" in sets:
        enrich["ratatoskr.env"] = str(sets["env"])
    if "pipeline" in sets:
        enrich["ratatoskr.pipeline"] = str(sets["pipeline"])
    if "region" in sets:
        enrich["ratatoskr.region"] = str(sets["region"])
    if "team" in sets:
        enrich["ratatoskr.team"] = str(sets["team"])

    route_props: dict[str, str] = {
        "Routing Strategy": "Route to Property name",
        route_name: f"${{event.type:equals('{event_type}')}}",
    }
    return {"EnrichUpdate": enrich, "RouteType": route_props}


def _live_props(client: Any, pg_id: str, name: str) -> dict[str, str]:
    procs = processors_by_name(client, pg_id)
    proc = procs.get(name)
    if not proc:
        return {}
    det = client.get_processor_details(proc["id"])
    config = (det.get("component") or {}).get("config") or {}
    props = config.get("properties") or {}
    return {str(k): str(v) for k, v in props.items() if v is not None}


def diff_properties(
    live: dict[str, str], desired: dict[str, str]
) -> dict[str, Any]:
    changes: dict[str, dict[str, str | None]] = {}
    for key, val in desired.items():
        cur = live.get(key)
        if cur != val:
            changes[key] = {"from": cur, "to": val}
    return {"changes": changes, "identical": not changes}


def poll_route_snapshot(
    client: Optional[Any] = None,
    *,
    rule: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from ratatoskr.nifi.client import NiFiClient

    nifi = client or NiFiClient()
    pg_id = find_dataplane_pg_id(nifi)
    desired = rules_to_properties(rule or DEFAULT_RULE)
    if not pg_id:
        return {
            "process_group_id": None,
            "flow_missing": True,
            "desired": desired,
            "live": {},
            "diff": {},
        }
    live = {
        "EnrichUpdate": _live_props(nifi, pg_id, "EnrichUpdate"),
        "RouteType": _live_props(nifi, pg_id, "RouteType"),
    }
    diffs = {
        name: diff_properties(live.get(name) or {}, desired.get(name) or {})
        for name in desired
    }
    return {
        "process_group_id": pg_id,
        "flow_missing": False,
        "rule": rule or DEFAULT_RULE,
        "desired": desired,
        "live": live,
        "diff": diffs,
    }


def classify_route_health(snapshot: dict[str, Any]) -> dict[str, Any]:
    severities: list[str] = []
    if snapshot.get("flow_missing"):
        severities.append("DATAPLANE_FLOW_MISSING")
    drift = False
    for name, d in (snapshot.get("diff") or {}).items():
        if not d.get("identical", True):
            drift = True
            severities.append(f"ROUTE_DRIFT:{name}")
    level = "OK"
    if "DATAPLANE_FLOW_MISSING" in severities:
        level = "HIGH"
    elif drift:
        level = "MEDIUM"
    return {
        "healthy": level == "OK",
        "level": level,
        "score": 100 if level == "OK" else 65,
        "severities": severities,
        "summary": ", ".join(severities) if severities else "ok",
    }


def _allowed_keys(processor: str, phase: str) -> frozenset[str]:
    if processor == "EnrichUpdate":
        return LAB_ENRICH_KEYS if phase_at_least(phase, "lab") else SAFE_ENRICH_KEYS
    if processor == "RouteType":
        return LAB_ROUTE_KEYS if phase_at_least(phase, "lab") else SAFE_ROUTE_KEYS
    return frozenset()


def build_route_plan(
    snapshot: dict[str, Any],
    *,
    phase: str | None = None,
) -> list[dict[str, Any]]:
    active = (phase or route_phase()).strip().lower()
    if not phase_at_least(active, "safe"):
        return []
    if snapshot.get("flow_missing"):
        return []
    plan: list[dict[str, Any]] = []
    desired = snapshot.get("desired") or {}
    for proc_name, props in desired.items():
        allowed = _allowed_keys(proc_name, active)
        filtered = {k: v for k, v in props.items() if k in allowed}
        if not filtered:
            continue
        diff = (snapshot.get("diff") or {}).get(proc_name) or {}
        if diff.get("identical"):
            continue
        # Only patch keys that differ and are allowlisted
        changes = {
            k: v
            for k, v in filtered.items()
            if k in (diff.get("changes") or {})
        }
        if not changes:
            continue
        plan.append(
            {
                "op": "config_apply",
                "processor": proc_name,
                "properties": changes,
                "min_phase": "safe",
                "reason": "route_enrich_drift",
            }
        )
    return plan


def apply_route_plan(
    client: Any,
    plan: list[dict[str, Any]],
    *,
    phase: str | None = None,
    dry_run: bool | None = None,
) -> list[dict[str, Any]]:
    active = (phase or route_phase()).strip().lower()
    is_dry = route_dry_run() if dry_run is None else bool(dry_run)
    max_m = route_max_mutations()
    actions: list[dict[str, Any]] = []
    pg_id = find_dataplane_pg_id(client)
    if not pg_id:
        return [{"op": "config_apply", "ok": False, "error": "dataplane pg missing"}]

    applied = 0
    for proposed in plan:
        if not phase_at_least(active, str(proposed.get("min_phase") or "safe")):
            actions.append({**proposed, "ok": False, "skipped": True, "reason": "phase"})
            continue
        if max_m and applied >= max_m:
            actions.append({**proposed, "ok": False, "skipped": True, "reason": "max_mutations"})
            continue
        if is_dry:
            actions.append({**proposed, "ok": True, "dry_run": True})
            applied += 1
            continue
        name = str(proposed.get("processor") or "")
        props = proposed.get("properties") or {}
        procs = processors_by_name(client, pg_id)
        proc = procs.get(name)
        if not proc:
            actions.append({**proposed, "ok": False, "error": f"processor {name} missing"})
            continue
        try:
            # Stop → patch → start (config_apply, not heal)
            state = proc.get("state")
            if state not in ("STOPPED", "DISABLED"):
                client.stop_processor(proc["id"])
                time.sleep(0.4)
            client.update_processor_config(proc["id"], properties=dict(props))
            time.sleep(0.3)
            client.start_processor(proc["id"])
            actions.append(
                {
                    **proposed,
                    "ok": True,
                    "id": proc["id"],
                    "config_apply": True,
                }
            )
            applied += 1
        except Exception as exc:  # noqa: BLE001
            actions.append({**proposed, "ok": False, "error": str(exc)})
    return actions


def run_route_enrich_cycle(
    client: Optional[Any] = None,
    *,
    phase: str | None = None,
    dry_run: bool | None = None,
    rule: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from ratatoskr.nifi.client import NiFiClient

    nifi = client or NiFiClient()
    active = (phase or route_phase()).strip().lower()
    snapshot = poll_route_snapshot(nifi, rule=rule)
    classification = classify_route_health(snapshot)
    plan = build_route_plan(snapshot, phase=active)
    actions: list[dict[str, Any]] = []
    if active != "monitor":
        actions = apply_route_plan(nifi, plan, phase=active, dry_run=dry_run)

    verify: dict[str, Any] | None = None
    if route_verify() and actions and any(a.get("ok") and not a.get("dry_run") for a in actions):
        time.sleep(0.5)
        after = poll_route_snapshot(nifi, rule=rule)
        verify = {
            "classification": classify_route_health(after),
            "diff": after.get("diff"),
        }

    return {
        "agent": "workflow_route_enrich",
        "poll_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "phase": active,
        "classification": classification,
        "health": snapshot,
        "actions": actions,
        "plan": plan,
        "mutations": [
            a for a in actions if a.get("ok") and not a.get("dry_run") and not a.get("skipped")
        ],
        "verify": verify,
    }
