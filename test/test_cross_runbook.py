#!/usr/bin/env python3
"""Cross-signal runbook tests (correlate → checklist)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _corr_backpressure_lag():
    from examples.agents.run_workflow_signal_correlate_local import _demo_events
    from ratatoskr.correlation import correlate_signals

    return correlate_signals(*_demo_events())


def _corr_topic_missing():
    from ratatoskr.correlation import correlate_signals

    nifi = {
        "classification": {
            "healthy": False,
            "level": "MEDIUM",
            "score": 75,
            "severities": ["STOPPED"],
        },
        "health": {"severities": ["STOPPED"]},
    }
    kafka = {
        "classification": {
            "healthy": False,
            "level": "HIGH",
            "score": 50,
            "severities": ["TOPIC_MISSING"],
        },
        "health": {
            "severities": ["TOPIC_MISSING"],
            "missing_topics": [{"name": "nifi.kafka.demo"}],
        },
    }
    return correlate_signals(nifi, kafka)


def test_fallback_backpressure_lag() -> None:
    from ratatoskr.correlation import fallback_cross_runbook
    from ratatoskr.nifi.runbook import is_valid_runbook_event

    out = fallback_cross_runbook(_corr_backpressure_lag())
    assert is_valid_runbook_event(out)
    assert out["agent"] == "react_cross_runbook"
    assert out["mutations"] == []
    assert out["runbook"]["mode"] == "fallback"
    lab = out["runbook"]["remediation"]["lab_options"]
    assert any(x.startswith("nifi:") for x in lab)


def test_fallback_topic_missing_safe_ops() -> None:
    from ratatoskr.correlation import allowed_cross_remediation, fallback_cross_runbook

    corr = _corr_topic_missing()
    allowed = allowed_cross_remediation(corr)
    assert "kafka:create_topic" in allowed["safe_options"]
    assert any(x.startswith("nifi:") for x in allowed["safe_options"])
    out = fallback_cross_runbook(corr)
    assert "kafka:create_topic" in out["runbook"]["remediation"]["safe_options"]


def test_fallback_healthy_no_incidents() -> None:
    from ratatoskr.correlation import correlate_signals, fallback_cross_runbook

    ok = {
        "classification": {"healthy": True, "level": "OK", "score": 100, "severities": []},
        "health": {"severities": []},
    }
    corr = correlate_signals(ok, ok)
    out = fallback_cross_runbook(corr)
    assert "healthy" in out["runbook"]["headline"].lower()
    assert out["runbook"]["remediation"]["safe_options"] == []


def test_build_cross_runbook_no_llm() -> None:
    from examples.agents.react_cross_runbook_logic import build_cross_runbook

    with patch(
        "ratatoskr.designer.llm_settings.get_react_llm_settings"
    ) as mock_settings:
        settings = MagicMock()
        settings.is_complete.return_value = False
        mock_settings.return_value = settings
        out = build_cross_runbook(_corr_backpressure_lag())
    assert out["agent"] == "react_cross_runbook"
    assert out["mutations"] == []
    assert out["runbook"]["mode"] == "fallback"


def test_llm_path_constrained() -> None:
    from examples.agents.react_cross_runbook_logic import build_cross_runbook

    corr = _corr_topic_missing()
    llm_payload = {
        "headline": "Topic missing + consumer stopped",
        "situation": "Kafka topic missing; ConsumeKafka stopped.",
        "likely_causes": [{"cause": "Deleted topic", "confidence": "high", "evidence": []}],
        "diagnostic_steps": [{"step": "Check topic", "where": "CLI", "expect": "missing"}],
        "remediation": {
            "safe_options": ["kafka:create_topic", "kafka:invent_op", "nifi:start_processor"],
            "lab_options": [],
            "do_not": ["no empty"],
        },
        "verify": ["re-correlate"],
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
        out = build_cross_runbook(corr)
    assert out["runbook"]["mode"] == "llm"
    safe = out["runbook"]["remediation"]["safe_options"]
    assert "kafka:create_topic" in safe
    assert "kafka:invent_op" not in safe


def test_agent_registered() -> None:
    from ratatoskr.agents.catalog import load_agent_catalog
    from ratatoskr.agents.registry import load_agent_registry

    manifest = load_agent_registry(validate=False)
    assert "react_cross_runbook" in manifest.agents
    catalog = load_agent_catalog(validate=False)
    found = any(
        a.manifest == "react_cross_runbook"
        for cat in catalog.categories
        for sub in cat.subcategories
        for a in sub.agents
    )
    assert found


def test_demo_script() -> None:
    import subprocess

    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "demo_cross_runbook.py"), "--scenario", "topic-missing"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "kafka:create_topic" in proc.stdout
    assert "mutations" in proc.stdout


def test_hitl_proposal_and_reject() -> None:
    from ratatoskr.correlation import fallback_cross_runbook
    from ratatoskr.correlation.runbook.hitl import (
        ACK_TOPIC,
        PROPOSE_TOPIC,
        apply_approved_cross_heal,
        attach_cross_hitl,
        build_cross_heal_proposal,
        decide_cross_approval,
        format_cross_apply_status,
    )

    assert PROPOSE_TOPIC == "signals.cross_runbook.propose"
    assert ACK_TOPIC == "signals.cross_runbook.ack"

    runbook = fallback_cross_runbook(_corr_topic_missing())
    proposal = build_cross_heal_proposal(
        runbook, dry_run=True, scenario="topic-missing"
    )
    assert proposal["kind"] == "cross_runbook_heal_propose"
    assert proposal["heal_phase"] == "lab"
    assert proposal["dry_run"] is True
    assert proposal["mutations"] == []
    assert "kafka:create_topic" in (proposal.get("proposed_ops") or [])

    ack = decide_cross_approval(proposal, auto_approve=False)
    assert ack["approved"] is False
    assert ack["mutations"] == []

    skipped = apply_approved_cross_heal(ack, _corr_topic_missing())
    assert skipped.get("skipped") == "not_approved"
    assert skipped.get("heal_actions") == []

    attached = attach_cross_hitl(
        runbook, proposal, status="rejected", approved=False
    )
    assert attached["mutations"] == []
    assert attached["hitl"]["status"] == "rejected"


def test_hitl_status_line_and_approve_gate() -> None:
    from ratatoskr.correlation.runbook.hitl import (
        build_cross_heal_proposal,
        decide_cross_approval,
        format_cross_apply_status,
    )
    from ratatoskr.correlation import fallback_cross_runbook
    from unittest.mock import patch

    runbook = fallback_cross_runbook(_corr_topic_missing())
    proposal = build_cross_heal_proposal(runbook, dry_run=False)
    ack = decide_cross_approval(proposal, auto_approve=True)
    assert ack["approved"] is True

    fake = {
        "ok": True,
        "dry_run": False,
        "phase": "lab",
        "cross_heal_plan": [{"id": "s1"}],
        "heal_actions": [{"ok": True}, {"ok": True}],
        "executed_ok": 2,
    }
    line = format_cross_apply_status(fake)
    assert line.startswith("cross heal status:")
    assert "executed_ok=2" in line
    assert "dry_run=False" in line

    with patch(
        "ratatoskr.correlation.heal.apply_cross_heal_policy",
        return_value={
            "cross_heal_phase": "lab",
            "cross_heal_dry_run": False,
            "cross_heal_plan": [{"id": "s1"}],
            "heal_actions": [{"ok": True, "op": "create_topic"}],
            "step_results": [],
        },
    ) as mock_apply:
        from ratatoskr.correlation.runbook.hitl import apply_approved_cross_heal

        applied = apply_approved_cross_heal(ack, _corr_topic_missing())
    assert applied["ok"] is True
    assert applied["executed_ok"] == 1
    assert applied["mutations"]
    mock_apply.assert_called_once()
    assert mock_apply.call_args.kwargs.get("phase") == "lab"


def test_demo_hitl_offline() -> None:
    import subprocess

    proc = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "demo_cross_runbook.py"),
            "--scenario",
            "topic-missing",
            "--heal",
            "--approve",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr + proc.stdout
    assert "HITL" in proc.stdout
    assert "approval recorded only" in proc.stdout
    assert "cross_runbook_heal_propose" in proc.stdout or "proposal_id" in proc.stdout


def main() -> int:
    tests = [
        test_fallback_backpressure_lag,
        test_fallback_topic_missing_safe_ops,
        test_fallback_healthy_no_incidents,
        test_build_cross_runbook_no_llm,
        test_llm_path_constrained,
        test_agent_registered,
        test_demo_script,
        test_hitl_proposal_and_reject,
        test_hitl_status_line_and_approve_gate,
        test_demo_hitl_offline,
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
