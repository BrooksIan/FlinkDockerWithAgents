"""Phase 2 runbook context: proposed heal refs, severity hints, remediation constraints."""

from __future__ import annotations

from typing import Any

# Preferred remediation order (lower = earlier). Unknown ops sort last.
_OP_ORDER = {
    "enable_controller_service": 10,
    "fix_processor_config": 20,
    "start_processor": 30,
    "restart_processor": 40,
    "stop_processor": 50,
    "terminate_processor": 60,
    "empty_connection_queue": 70,
}

_SAFE_OPS = frozenset({"enable_controller_service", "start_processor"})
_LAB_OPS = frozenset(
    {
        "fix_processor_config",
        "stop_processor",
        "restart_processor",
        "terminate_processor",
        "empty_connection_queue",
    }
)


def ref_key(op: str, name: str | None, eid: str | None = None) -> str:
    """Canonical remediation citation: prefer name, else id."""
    label = (name or eid or "").strip()
    return f"{op}:{label}" if label else op


def _plan_ref(item: dict[str, Any]) -> str:
    return ref_key(
        str(item.get("op") or ""),
        str(item["name"]) if item.get("name") else None,
        str(item["id"]) if item.get("id") else None,
    )


def order_refs(refs: list[str]) -> list[str]:
    """Stable sort: enable services before starts; lab containment last."""

    def sort_key(ref: str) -> tuple[int, str]:
        op = ref.split(":", 1)[0] if ref else ""
        return (_OP_ORDER.get(op, 100), ref)

    seen: set[str] = set()
    ordered: list[str] = []
    for r in sorted(refs, key=sort_key):
        if r and r not in seen:
            seen.add(r)
            ordered.append(r)
    return ordered


def split_plan_refs(plan: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    """Split heal_plan rows into ordered safe vs lab remediation strings."""
    safe: list[str] = []
    lab: list[str] = []
    for item in plan:
        op = str(item.get("op") or "")
        ref = _plan_ref(item)
        if not op or ref == op:
            continue
        if op in _SAFE_OPS:
            safe.append(ref)
        elif op in _LAB_OPS:
            lab.append(ref)
        else:
            lab.append(ref)
    return order_refs(safe), order_refs(lab)


def proposed_heal_plan(event: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Heal ops the runbook may cite.

    Uses event ``heal_plan`` when non-empty; otherwise builds the lab-phase
    proposal from health (monitor phase intentionally leaves heal_plan empty).
    """
    existing = [a for a in (event.get("heal_plan") or []) if isinstance(a, dict) and a.get("op")]
    if existing:
        return existing
    health = event.get("health")
    if not isinstance(health, dict):
        return []
    from ratatoskr.nifi.policy import build_heal_plan

    return build_heal_plan(health, phase="lab")


def allowed_remediation(event: dict[str, Any]) -> dict[str, list[str]]:
    """Allowed safe/lab option strings derived from proposed heal plan."""
    safe, lab = split_plan_refs(proposed_heal_plan(event))
    return {"safe_options": safe, "lab_options": lab}


def severity_guidance(severities: list[str]) -> list[str]:
    """Short operator hints keyed by observed severities."""
    sevs = {str(s) for s in severities}
    hints: list[str] = []
    if "DISABLED_SERVICE" in sevs:
        hints.append(
            "DISABLED_SERVICE: enable controller services before starting dependent processors."
        )
    if "STOPPED" in sevs:
        hints.append(
            "STOPPED: start VALID processors only after required services are ENABLED."
        )
    if "INVALID" in sevs:
        hints.append(
            "INVALID: prefer lab fix_processor_config templates (e.g. LogAttribute "
            "auto-terminate) before terminate_processor."
        )
    if any(s.startswith("BACKPRESSURE") for s in sevs):
        hints.append(
            "BACKPRESSURE: diagnose downstream STOPPED/slow consumers before empty_connection_queue; "
            "queue drain requires NIFI_HEAL_ALLOW_EMPTY_QUEUE=1."
        )
    if "BULLETIN_ERROR" in sevs:
        hints.append(
            "BULLETIN_ERROR: use bulletin fingerprints; lab restart_processor only for repeated sources."
        )
    if "NIFI_UNREACHABLE" in sevs or "NIFI_SLOW" in sevs:
        hints.append(
            "NIFI_UNREACHABLE/SLOW: fix connectivity/credentials first — no canvas mutations."
        )
    return hints


def constrain_remediation(
    remediation: dict[str, Any],
    *,
    allowed_safe: list[str],
    allowed_lab: list[str],
) -> dict[str, list[str]]:
    """
    Keep only remediation lines that match allowed heal refs.

    If the LLM invents ops/names, drop them. If filtering leaves safe/lab empty
    while allowed lists are non-empty, fill from allowed (ordered).
    """
    allowed_safe_set = set(allowed_safe)
    allowed_lab_set = set(allowed_lab)
    allowed_all = allowed_safe_set | allowed_lab_set

    def _filter(raw: Any, bucket: set[str]) -> list[str]:
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list):
            return []
        out: list[str] = []
        for item in raw:
            ref = str(item).strip()
            if not ref:
                continue
            # Exact match preferred; also accept op:id if plan used names (and vice versa) — exact only for Phase 2.
            if ref in bucket or (not bucket and ref in allowed_all):
                out.append(ref)
        return order_refs(out)

    safe = _filter(remediation.get("safe_options"), allowed_safe_set)
    lab = _filter(remediation.get("lab_options"), allowed_lab_set)

    if not safe and allowed_safe:
        safe = list(allowed_safe)
    if not lab and allowed_lab and not remediation.get("lab_options"):
        # Only auto-fill lab when LLM omitted lab entirely; if LLM listed lab but all
        # hallucinated, leave empty rather than surprising the operator with lab ops.
        pass
    if not lab and allowed_lab and not _filter(remediation.get("lab_options"), allowed_lab_set):
        # LLM had no valid lab refs — if severity implies lab (allowed_lab non-empty) and
        # LLM put nothing usable, fill from allowed lab catalog.
        raw_lab = remediation.get("lab_options")
        if not raw_lab:
            lab = list(allowed_lab)

    do_not = remediation.get("do_not") or []
    if isinstance(do_not, str):
        do_not = [do_not]
    if not isinstance(do_not, list):
        do_not = []
    do_not_out = [str(x) for x in do_not]
    if not do_not_out:
        do_not_out = [
            "Do not empty queues without NIFI_HEAL_ALLOW_EMPTY_QUEUE=1",
            "Do not invent component names outside heal_plan / health lists",
            "ReAct runbook must not mutate NiFi — use workflow_nifi_monitor heal phases",
        ]

    return {
        "safe_options": safe,
        "lab_options": lab,
        "do_not": do_not_out,
    }


def enrich_monitor_context(event: dict[str, Any]) -> dict[str, Any]:
    """Slim + allowed remediation catalog + severity guidance for the LLM prompt."""
    classification = event.get("classification") or {}
    health = event.get("health") or {}
    sevs = list(classification.get("severities") or health.get("severities") or [])

    def _brief(items: list[Any] | None, *, limit: int = 10, extra: tuple[str, ...] = ()) -> list[dict[str, Any]]:
        keys = ("id", "name", "state", "validationStatus", "runStatus") + extra
        out: list[dict[str, Any]] = []
        for it in items or []:
            if not isinstance(it, dict):
                continue
            out.append({k: it.get(k) for k in keys if it.get(k) is not None})
            if len(out) >= limit:
                break
        return out

    plan = proposed_heal_plan(event)
    allowed = allowed_remediation(event)
    plan_brief = [
        {
            k: a.get(k)
            for k in ("op", "id", "name", "reason", "template", "from_connection")
            if a.get(k) is not None
        }
        for a in plan
        if isinstance(a, dict)
    ]

    return {
        "poll_id": event.get("poll_id"),
        "phase": event.get("phase"),
        "classification": {
            "healthy": classification.get("healthy"),
            "level": classification.get("level"),
            "score": classification.get("score"),
            "severities": sevs,
            "summary": classification.get("summary"),
            "bulletin_groups": (classification.get("bulletin_groups") or [])[:8],
        },
        "health": {
            "severities": health.get("severities") or sevs,
            "stopped_processors": _brief(health.get("stopped_processors")),
            "invalid_processors": _brief(health.get("invalid_processors")),
            "disabled_controller_services": _brief(health.get("disabled_controller_services")),
            "queued_connections": _brief(
                health.get("queued_connections"),
                extra=("flowFilesQueued", "percentUsedCount", "percentUsedBytes", "sourceName", "destinationName"),
            ),
            "bulletins": _brief(
                health.get("bulletins"),
                limit=6,
                extra=("level", "fingerprint", "sourceName", "sourceId", "message"),
            ),
            "probe": health.get("probe"),
            "counts": health.get("counts"),
        },
        "heal_plan": plan_brief,
        "heal_plan_source": "event" if (event.get("heal_plan") or []) else "proposed_lab",
        "allowed_remediation": allowed,
        "severity_guidance": severity_guidance([str(s) for s in sevs]),
        "remediation_rules": [
            "Cite remediation ONLY from allowed_remediation (exact op:name strings).",
            "Order safe_options: enable_controller_service before start_processor.",
            "Do not invent component names or ids.",
        ],
    }
