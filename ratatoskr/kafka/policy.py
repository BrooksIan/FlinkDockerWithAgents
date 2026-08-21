"""Deterministic heal policy for Kafka monitoring workflow agent."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from ratatoskr.kafka.client import KafkaClient
from ratatoskr.kafka.env import (
    heal_allow_groups,
    heal_allow_name_regex,
    heal_allow_topics,
    heal_cooldown_sec,
    heal_dry_run,
    heal_max_mutations,
    heal_phase,
    heal_verify,
    phase_at_least,
)

_SEVERITY_PENALTIES: dict[str, int] = {
    "BROKER_UNREACHABLE": 100,
    "OFFLINE_PARTITION": 40,
    "UNDER_REPLICATED": 35,
    "LAG_CRIT": 30,
    "CONSUMER_STALLED": 25,
    "TOPIC_MISSING": 20,
    "LAG_WARN": 10,
    "GROUP_EMPTY": 10,
    "BROKER_SLOW": 5,
    "TOPIC_UNEXPECTED": 5,
}

_HIGH = frozenset(
    {
        "BROKER_UNREACHABLE",
        "OFFLINE_PARTITION",
        "UNDER_REPLICATED",
        "LAG_CRIT",
        "CONSUMER_STALLED",
    }
)
_MEDIUM = frozenset({"TOPIC_MISSING", "LAG_WARN", "GROUP_EMPTY", "BROKER_SLOW"})

# Ordered heal rules (min_phase: monitor < safe < lab).
HEAL_RULES: list[dict[str, Any]] = [
    {
        "op": "create_topic",
        "min_phase": "safe",
        "source": "missing_topics",
    },
    {
        "op": "reset_offsets",
        "min_phase": "lab",
        "source": "lag_crit_groups",
        "requires_group_allowlist": True,
        "reason": "lag_relief",
    },
    {
        "op": "delete_group",
        "min_phase": "lab",
        "source": "empty_lagging_groups",
        "requires_group_allowlist": True,
        "reason": "stalled_empty_group",
    },
]

_COOLDOWN: dict[tuple[str, str], float] = {}


def reset_heal_cooldown() -> None:
    _COOLDOWN.clear()


def classify_health(health: dict[str, Any]) -> dict[str, Any]:
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
    for s in severities:
        if s in seen:
            continue
        seen.add(s)
        penalty += _SEVERITY_PENALTIES.get(s, 5)
    score = max(0, 100 - penalty)

    return {
        "healthy": bool(health.get("healthy")) and not severities,
        "level": level,
        "score": score,
        "severities": severities,
        "summary": ("healthy" if not severities else ", ".join(severities)),
    }


def _ids_from(health: dict[str, Any], key: str, id_field: str = "name") -> set[str]:
    ids: set[str] = set()
    for item in health.get(key) or []:
        if key.endswith("groups") or "group" in key:
            eid = item.get("group_id") or item.get("id")
        else:
            eid = item.get(id_field) or item.get("name") or item.get("id")
        if eid:
            ids.add(str(eid))
    return ids


def diff_health(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> dict[str, Any]:
    keys = (
        ("missing_topics", "name"),
        ("lag_warn_groups", "group_id"),
        ("lag_crit_groups", "group_id"),
        ("stalled_groups", "group_id"),
        ("under_replicated_topics", "name"),
    )
    prev = previous or {}
    new: dict[str, list[str]] = {}
    persistent: dict[str, list[str]] = {}
    resolved: dict[str, list[str]] = {}

    for key, field in keys:
        p_ids = _ids_from(prev, key, field)
        c_ids = _ids_from(current, key, field)
        n = sorted(c_ids - p_ids)
        p = sorted(c_ids & p_ids)
        r = sorted(p_ids - c_ids)
        if n:
            new[key] = n
        if p:
            persistent[key] = p
        if r:
            resolved[key] = r

    prev_sev = set(prev.get("severities") or [])
    curr_sev = set(current.get("severities") or [])
    return {
        "new": new,
        "persistent": persistent,
        "resolved": resolved,
        "severities_new": sorted(curr_sev - prev_sev),
        "severities_resolved": sorted(prev_sev - curr_sev),
    }


def _source_entities(health: dict[str, Any], source: str) -> list[dict[str, Any]]:
    return list(health.get(source) or [])


def build_heal_plan(
    health: dict[str, Any],
    *,
    phase: str | None = None,
) -> list[dict[str, Any]]:
    active = (phase or heal_phase()).lower()
    if active == "monitor" or not phase_at_least(active, "safe"):
        return []

    plan: list[dict[str, Any]] = []
    for rule in HEAL_RULES:
        min_phase = str(rule.get("min_phase") or "lab")
        if not phase_at_least(active, min_phase):
            continue
        op = str(rule["op"])
        for ent in _source_entities(health, str(rule["source"])):
            if op == "create_topic":
                target = ent.get("name")
                if not target:
                    continue
                item: dict[str, Any] = {
                    "op": op,
                    "id": target,
                    "name": target,
                    "partitions": ent.get("partitions"),
                    "replication_factor": ent.get("replication_factor"),
                    "proposed": True,
                }
            elif op == "reset_offsets":
                gid = ent.get("group_id")
                if not gid:
                    continue
                # Prefer first lagged topic partition's topic
                topic = None
                for p in ent.get("partitions") or []:
                    if int(p.get("lag") or 0) > 0:
                        topic = p.get("topic")
                        break
                if not topic and (ent.get("partitions") or []):
                    topic = (ent.get("partitions") or [{}])[0].get("topic")
                if not topic:
                    continue
                item = {
                    "op": op,
                    "id": gid,
                    "name": gid,
                    "group_id": gid,
                    "topic": topic,
                    "proposed": True,
                    "requires_group_allowlist": True,
                }
            elif op == "delete_group":
                gid = ent.get("group_id")
                if not gid:
                    continue
                item = {
                    "op": op,
                    "id": gid,
                    "name": gid,
                    "group_id": gid,
                    "proposed": True,
                    "requires_group_allowlist": True,
                }
            else:
                continue
            if rule.get("reason"):
                item["reason"] = rule["reason"]
            plan.append(item)
    return plan


def _allowlisted(action: dict[str, Any]) -> bool:
    op = action.get("op")
    name = str(action.get("name") or action.get("id") or "")
    topic_allow = heal_allow_topics()
    group_allow = heal_allow_groups()
    name_re = heal_allow_name_regex()

    if op == "create_topic":
        if topic_allow is None and name_re is None:
            return True
        ok_topic = topic_allow is not None and name in topic_allow
        ok_re = name_re is not None and bool(name_re.search(name))
        if topic_allow is not None and name_re is not None:
            return ok_topic or ok_re
        if topic_allow is not None:
            return ok_topic
        return ok_re

    # Group mutations: require explicit allowlist (deny by default).
    if action.get("requires_group_allowlist") or op in ("reset_offsets", "delete_group"):
        if group_allow is None:
            return False
        return name in group_allow or str(action.get("group_id") or "") in group_allow

    if name_re is not None:
        return bool(name_re.search(name))
    return True


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


def _execute_action(client: KafkaClient, action: dict[str, Any]) -> dict[str, Any]:
    op = action["op"]
    out = {k: v for k, v in action.items() if k not in ("proposed",)}
    out["proposed"] = False
    try:
        if op == "create_topic":
            client.create_topic(
                str(action["id"]),
                partitions=action.get("partitions"),
                replication_factor=action.get("replication_factor"),
            )
        elif op == "reset_offsets":
            client.reset_offsets_to_end(
                str(action.get("group_id") or action["id"]),
                str(action["topic"]),
            )
        elif op == "delete_group":
            client.delete_consumer_group(str(action.get("group_id") or action["id"]))
        else:
            out["ok"] = False
            out["error"] = f"unknown op {op}"
            return out
        out["ok"] = True
    except Exception as exc:  # noqa: BLE001
        out["ok"] = False
        out["error"] = str(exc)
    return out


def _verify_action(action: dict[str, Any], health_after: dict[str, Any]) -> bool:
    op = action.get("op")
    eid = str(action.get("id") or "")
    if not eid or not action.get("ok"):
        return False
    if op == "create_topic":
        missing = {m.get("name") for m in (health_after.get("missing_topics") or [])}
        return eid not in missing
    if op == "reset_offsets":
        crit = {g.get("group_id") for g in (health_after.get("lag_crit_groups") or [])}
        return eid not in crit
    if op == "delete_group":
        groups = {g.get("group_id") for g in (health_after.get("consumer_groups") or [])}
        empty = {
            g.get("group_id") for g in (health_after.get("empty_lagging_groups") or [])
        }
        return eid not in groups or eid not in empty
    return False


def apply_heal_policy(
    client: KafkaClient,
    health: dict[str, Any],
    *,
    phase: str | None = None,
    dry_run: bool | None = None,
    verify: bool | None = None,
) -> list[dict[str, Any]]:
    """
    Build plan → filter (allowlist, cooldown, blast) → dry-run or execute → verify.

    - monitor: none
    - safe: create missing catalog topics
    - lab: safe + allowlisted offset reset / delete empty lagging groups
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
            actions.append({**proposed, "ok": None, "proposed": True})
            executed += 1
            _mark_cooldown(op, eid, now)
            continue

        result = _execute_action(client, proposed)
        actions.append(result)
        if result.get("ok"):
            executed += 1
            _mark_cooldown(op, eid, now)

    if do_verify and not is_dry and any(a.get("ok") for a in actions):
        try:
            after = client.get_cluster_health_status()
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


def _unreachable_health(exc: BaseException, bootstrap: str = "") -> dict[str, Any]:
    return {
        "bootstrap": bootstrap,
        "healthy": False,
        "severities": ["BROKER_UNREACHABLE"],
        "probe": {"ok": False, "error": str(exc), "bootstrap": bootstrap},
        "missing_topics": [],
        "unexpected_topics": [],
        "topic_details": [],
        "under_replicated_topics": [],
        "offline_partitions": [],
        "consumer_groups": [],
        "lag_warn_groups": [],
        "lag_crit_groups": [],
        "stalled_groups": [],
        "empty_lagging_groups": [],
        "catalog": {},
        "counts": {},
    }


def run_monitor_cycle(
    client: KafkaClient,
    *,
    phase: str | None = None,
    previous_health: dict[str, Any] | None = None,
    dry_run: bool | None = None,
    verify: bool | None = None,
    client_factory: Callable[[], KafkaClient] | None = None,
) -> dict[str, Any]:
    """One poll → classify → diff → plan → optional heal cycle."""
    _ = client_factory
    poll_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc).isoformat()
    active = phase or heal_phase()
    is_dry = heal_dry_run() if dry_run is None else dry_run

    try:
        health = client.get_cluster_health_status()
    except Exception as exc:  # noqa: BLE001
        health = _unreachable_health(exc, bootstrap=getattr(client, "bootstrap", ""))

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
    )

    # Refresh snapshot after successful mutations so OutputEvent isn't stale
    # (verify already re-polled inside apply; one more read is cheap and keeps the
    # top-level health/classification aligned with heal_actions[].verified).
    if not is_dry and any(a.get("ok") for a in heal_actions):
        try:
            health = client.get_cluster_health_status()
            classification = classify_health(health)
            if previous_health is not None:
                delta = diff_health(previous_health, health)
            else:
                delta = diff_health(health_before, health)
        except Exception:  # noqa: BLE001
            pass

    health_out = {
        "bootstrap": health.get("bootstrap"),
        "healthy": health.get("healthy"),
        "severities": health.get("severities"),
        "counts": health.get("counts"),
        "probe": health.get("probe"),
        "missing_topics": health.get("missing_topics"),
        "unexpected_topics": health.get("unexpected_topics"),
        "under_replicated_topics": health.get("under_replicated_topics"),
        "offline_partitions": health.get("offline_partitions"),
        "lag_warn_groups": health.get("lag_warn_groups"),
        "lag_crit_groups": health.get("lag_crit_groups"),
        "stalled_groups": health.get("stalled_groups"),
        "empty_lagging_groups": health.get("empty_lagging_groups"),
        "consumer_groups": health.get("consumer_groups"),
    }

    return {
        "agent": "workflow_kafka_monitor",
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
