#!/usr/bin/env python3
"""Phase 2 tests: proposed heal refs, remediation constraints, severity guidance."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _monitor_replay_disabled() -> dict:
    """Mirrors live monitor phase=monitor with empty heal_plan (Replay + JsonTreeReader)."""
    return {
        "agent": "workflow_nifi_monitor",
        "poll_id": "test-replay",
        "phase": "monitor",
        "classification": {
            "healthy": False,
            "level": "MEDIUM",
            "score": 65,
            "severities": ["STOPPED", "DISABLED_SERVICE"],
            "summary": "STOPPED, DISABLED_SERVICE",
        },
        "health": {
            "severities": ["STOPPED", "DISABLED_SERVICE"],
            "stopped_processors": [
                {"id": "p1", "name": "ReplayPublish", "state": "STOPPED", "validationStatus": "VALID"},
                {"id": "p2", "name": "ReplayMark", "state": "STOPPED", "validationStatus": "VALID"},
                {"id": "p3", "name": "ReplayConsume", "state": "STOPPED", "validationStatus": "VALID"},
            ],
            "invalid_processors": [],
            "disabled_controller_services": [
                {"id": "s1", "name": "JsonTreeReader", "state": "DISABLED"},
            ],
            "queued_connections": [],
            "bulletins": [],
        },
        "heal_plan": [],
    }


def test_proposed_heal_plan_when_event_plan_empty() -> None:
    from ratatoskr.nifi.runbook import allowed_remediation, proposed_heal_plan

    event = _monitor_replay_disabled()
    assert event["heal_plan"] == []
    plan = proposed_heal_plan(event)
    ops = {(a["op"], a["name"]) for a in plan}
    assert ("enable_controller_service", "JsonTreeReader") in ops
    assert ("start_processor", "ReplayPublish") in ops
    allowed = allowed_remediation(event)
    assert allowed["safe_options"][0] == "enable_controller_service:JsonTreeReader"
    assert "start_processor:ReplayPublish" in allowed["safe_options"]


def test_order_refs_enable_before_start() -> None:
    from ratatoskr.nifi.runbook import order_refs

    ordered = order_refs(
        [
            "start_processor:ReplayPublish",
            "enable_controller_service:JsonTreeReader",
            "start_processor:ReplayMark",
        ]
    )
    assert ordered[0] == "enable_controller_service:JsonTreeReader"
    assert ordered[1].startswith("start_processor:")


def test_constrain_drops_hallucinated_ops() -> None:
    from ratatoskr.nifi.runbook import constrain_remediation

    out = constrain_remediation(
        {
            "safe_options": [
                "start_processor:FakeProcessor",
                "enable_controller_service:JsonTreeReader",
                "start_processor:ReplayPublish",
            ],
            "lab_options": ["empty_connection_queue:InventedQueue"],
            "do_not": ["keep me"],
        },
        allowed_safe=[
            "enable_controller_service:JsonTreeReader",
            "start_processor:ReplayPublish",
        ],
        allowed_lab=[],
    )
    assert out["safe_options"][0] == "enable_controller_service:JsonTreeReader"
    assert "FakeProcessor" not in ";".join(out["safe_options"])
    assert out["lab_options"] == []
    assert "keep me" in out["do_not"]


def test_constrain_fills_from_allowed_when_llm_empty() -> None:
    from ratatoskr.nifi.runbook import constrain_remediation

    out = constrain_remediation(
        {"safe_options": [], "lab_options": [], "do_not": []},
        allowed_safe=["enable_controller_service:JsonTreeReader"],
        allowed_lab=["fix_processor_config:LogAttribute"],
    )
    assert out["safe_options"] == ["enable_controller_service:JsonTreeReader"]
    assert out["lab_options"] == ["fix_processor_config:LogAttribute"]


def test_fallback_monitor_empty_plan_proposes_safe_ops() -> None:
    from ratatoskr.nifi.runbook import fallback_runbook, is_valid_runbook_event

    out = fallback_runbook(_monitor_replay_disabled())
    assert is_valid_runbook_event(out)
    safe = out["runbook"]["remediation"]["safe_options"]
    assert safe[0] == "enable_controller_service:JsonTreeReader"
    assert "start_processor:ReplayConsume" in safe
    assert out["source"]["heal_plan_source"] == "proposed_lab"


def test_enrich_includes_guidance_and_queues() -> None:
    from ratatoskr.nifi.runbook import enrich_monitor_context, load_fixture, severity_guidance

    hints = severity_guidance(["INVALID", "BACKPRESSURE_CRIT"])
    assert any("INVALID" in h for h in hints)
    assert any("BACKPRESSURE" in h for h in hints)

    slim = enrich_monitor_context(load_fixture("queue-backlog"))
    assert slim["health"]["queued_connections"]
    assert "allowed_remediation" in slim
    assert slim["severity_guidance"]


def test_llm_path_constrained() -> None:
    from examples.agents.react_nifi_runbook_logic import build_runbook
    from ratatoskr.nifi.runbook import is_valid_runbook_event

    llm_payload = {
        "headline": "Replay stopped",
        "situation": "Replay processors stopped; JsonTreeReader disabled.",
        "likely_causes": [{"cause": "Disabled service", "confidence": "high", "evidence": []}],
        "diagnostic_steps": [{"step": "Check JsonTreeReader", "where": "UI", "expect": "DISABLED"}],
        "remediation": {
            "safe_options": [
                "start_processor:ReplayPublish",
                "enable_controller_service:JsonTreeReader",
                "start_processor:Hallucinated",
            ],
            "lab_options": [],
            "do_not": ["no empty"],
        },
        "verify": ["re-poll"],
    }

    with patch(
        "ratatoskr.designer.llm_settings.get_react_llm_settings"
    ) as mock_settings, patch(
        "ratatoskr.designer.llm_client.chat_completion_json",
        return_value=llm_payload,
    ):
        settings = MagicMock()
        settings.is_complete.return_value = True
        mock_settings.return_value = settings
        out = build_runbook(_monitor_replay_disabled())

    assert is_valid_runbook_event(out)
    assert out["runbook"]["mode"] == "llm"
    safe = out["runbook"]["remediation"]["safe_options"]
    assert safe[0] == "enable_controller_service:JsonTreeReader"
    assert "Hallucinated" not in ";".join(safe)
    assert out["source"]["heal_plan_source"] == "proposed_lab"


def test_invalid_fixture_lab_options() -> None:
    from ratatoskr.nifi.runbook import allowed_remediation, load_fixture

    allowed = allowed_remediation(load_fixture("invalid-log"))
    assert any(x.startswith("fix_processor_config:LogAttribute") for x in allowed["lab_options"]) or any(
        x.startswith("terminate_processor:LogAttribute") for x in allowed["lab_options"]
    )


def main() -> int:
    tests = [
        test_proposed_heal_plan_when_event_plan_empty,
        test_order_refs_enable_before_start,
        test_constrain_drops_hallucinated_ops,
        test_constrain_fills_from_allowed_when_llm_empty,
        test_fallback_monitor_empty_plan_proposes_safe_ops,
        test_enrich_includes_guidance_and_queues,
        test_llm_path_constrained,
        test_invalid_fixture_lab_options,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"OK  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    if failed:
        print(f"FAILED {failed}/{len(tests)}")
        return 1
    print(f"PASS ({len(tests)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
