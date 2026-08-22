"""NiFi runbook OutputEvent contract (Phase 0).

Frozen JSON shape for ``react_nifi_runbook`` — LLM and fallback both emit this.
Mutations are never part of the runbook agent contract.
"""

from __future__ import annotations

from typing import Any

RUNBOOK_SCHEMA_VERSION = "1"

# Required top-level keys on a validated runbook body (inside agent wrapper).
REQUIRED_RUNBOOK_KEYS = frozenset(
    {
        "headline",
        "situation",
        "likely_causes",
        "diagnostic_steps",
        "remediation",
        "verify",
        "mode",
    }
)

REQUIRED_REMEDIATION_KEYS = frozenset({"safe_options", "lab_options", "do_not"})

ALLOWED_MODES = frozenset({"llm", "fallback"})
ALLOWED_CONFIDENCE = frozenset({"high", "medium", "low"})
ALLOWED_RUNBOOK_AGENTS = frozenset({"react_nifi_runbook", "react_cross_runbook"})


def empty_runbook(*, mode: str = "fallback") -> dict[str, Any]:
    """Minimal valid runbook body."""
    if mode not in ALLOWED_MODES:
        mode = "fallback"
    return {
        "schema_version": RUNBOOK_SCHEMA_VERSION,
        "headline": "",
        "situation": "",
        "likely_causes": [],
        "diagnostic_steps": [],
        "remediation": {
            "safe_options": [],
            "lab_options": [],
            "do_not": [],
        },
        "verify": [],
        "mode": mode,
    }


def wrap_runbook_event(
    runbook: dict[str, Any],
    *,
    source: dict[str, Any] | None = None,
    agent: str = "react_nifi_runbook",
) -> dict[str, Any]:
    """Agent OutputEvent envelope — mutations always empty."""
    if agent not in ALLOWED_RUNBOOK_AGENTS:
        agent = "react_nifi_runbook"
    return {
        "agent": agent,
        "schema_version": RUNBOOK_SCHEMA_VERSION,
        "mutations": [],
        "runbook": runbook,
        "source": source or {},
    }


def validate_runbook(runbook: dict[str, Any]) -> list[str]:
    """Return a list of validation errors (empty = valid)."""
    errors: list[str] = []
    if not isinstance(runbook, dict):
        return ["runbook must be an object"]

    missing = REQUIRED_RUNBOOK_KEYS - set(runbook.keys())
    if missing:
        errors.append(f"missing keys: {sorted(missing)}")

    mode = runbook.get("mode")
    if mode is not None and mode not in ALLOWED_MODES:
        errors.append(f"mode must be one of {sorted(ALLOWED_MODES)}, got {mode!r}")

    for key in ("headline", "situation"):
        if key in runbook and not isinstance(runbook[key], str):
            errors.append(f"{key} must be a string")

    causes = runbook.get("likely_causes")
    if causes is not None:
        if not isinstance(causes, list):
            errors.append("likely_causes must be a list")
        else:
            for i, c in enumerate(causes):
                if not isinstance(c, dict):
                    errors.append(f"likely_causes[{i}] must be an object")
                    continue
                if "cause" not in c:
                    errors.append(f"likely_causes[{i}] missing cause")
                conf = c.get("confidence")
                if conf is not None and conf not in ALLOWED_CONFIDENCE:
                    errors.append(
                        f"likely_causes[{i}].confidence must be one of "
                        f"{sorted(ALLOWED_CONFIDENCE)}"
                    )
                ev = c.get("evidence")
                if ev is not None and not isinstance(ev, list):
                    errors.append(f"likely_causes[{i}].evidence must be a list")

    steps = runbook.get("diagnostic_steps")
    if steps is not None:
        if not isinstance(steps, list):
            errors.append("diagnostic_steps must be a list")
        else:
            for i, s in enumerate(steps):
                if not isinstance(s, dict):
                    errors.append(f"diagnostic_steps[{i}] must be an object")
                    continue
                if "step" not in s:
                    errors.append(f"diagnostic_steps[{i}] missing step")

    rem = runbook.get("remediation")
    if rem is not None:
        if not isinstance(rem, dict):
            errors.append("remediation must be an object")
        else:
            miss_r = REQUIRED_REMEDIATION_KEYS - set(rem.keys())
            if miss_r:
                errors.append(f"remediation missing keys: {sorted(miss_r)}")
            for k in REQUIRED_REMEDIATION_KEYS:
                if k in rem and not isinstance(rem[k], list):
                    errors.append(f"remediation.{k} must be a list")

    verify = runbook.get("verify")
    if verify is not None and not isinstance(verify, list):
        errors.append("verify must be a list")

    return errors


def validate_runbook_event(event: dict[str, Any]) -> list[str]:
    """Validate full agent OutputEvent wrapper."""
    errors: list[str] = []
    if not isinstance(event, dict):
        return ["event must be an object"]
    if event.get("agent") not in ALLOWED_RUNBOOK_AGENTS:
        errors.append(
            f"agent must be one of {sorted(ALLOWED_RUNBOOK_AGENTS)}, "
            f"got {event.get('agent')!r}"
        )
    if event.get("mutations") not in ([], None):
        if event.get("mutations") != []:
            errors.append("mutations must be an empty list")
    if "mutations" not in event:
        errors.append("mutations key required")
    elif event["mutations"] != []:
        errors.append("mutations must be []")
    rb = event.get("runbook")
    if rb is None:
        errors.append("runbook key required")
    else:
        errors.extend(validate_runbook(rb))
    return errors


def is_valid_runbook(runbook: dict[str, Any]) -> bool:
    return not validate_runbook(runbook)


def is_valid_runbook_event(event: dict[str, Any]) -> bool:
    return not validate_runbook_event(event)
