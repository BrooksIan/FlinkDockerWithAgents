#!/usr/bin/env python3
"""Phase 4 tests: HITL propose → approve/reject → apply gate."""

from __future__ import annotations

import io
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _runbook_event() -> dict:
    from ratatoskr.nifi.runbook import fallback_runbook, load_fixture

    return fallback_runbook(load_fixture("stop-generate"))


def test_build_proposal_from_runbook() -> None:
    from ratatoskr.nifi.runbook import build_heal_proposal

    prop = build_heal_proposal(_runbook_event(), heal_phase="safe", dry_run=False)
    assert prop["kind"] == "nifi_runbook_heal_propose"
    assert prop["status"] == "pending"
    assert prop["mutations"] == []
    assert "start_processor:GenerateFlowFile" in prop["proposed_ops"]
    assert prop["heal_phase"] == "safe"


def test_decide_auto_approve_reject() -> None:
    from ratatoskr.nifi.runbook import build_heal_proposal, decide_approval

    prop = build_heal_proposal(_runbook_event(), heal_phase="safe")
    ack_yes = decide_approval(prop, auto_approve=True)
    assert ack_yes["approved"] is True
    ack_no = decide_approval(prop, auto_approve=False)
    assert ack_no["approved"] is False
    ack_default = decide_approval(prop, auto_approve=None, interactive=False)
    assert ack_default["approved"] is False


def test_prompt_approve_yes() -> None:
    from ratatoskr.nifi.runbook import build_heal_proposal, prompt_approve

    prop = build_heal_proposal(_runbook_event(), heal_phase="safe")
    ack = prompt_approve(prop, stdin=io.StringIO("y\n"), stdout=io.StringIO())
    assert ack["approved"] is True
    ack_n = prompt_approve(prop, stdin=io.StringIO("\n"), stdout=io.StringIO())
    assert ack_n["approved"] is False


def test_attach_hitl_keeps_mutations_empty() -> None:
    from ratatoskr.nifi.runbook import (
        attach_hitl,
        build_heal_proposal,
        is_valid_runbook_event,
    )

    rb = _runbook_event()
    prop = build_heal_proposal(rb, heal_phase="safe")
    out = attach_hitl(rb, prop, status="approved", approved=True)
    assert out["mutations"] == []
    assert out["hitl"]["approved"] is True
    assert is_valid_runbook_event(out)


def test_apply_skipped_when_not_approved() -> None:
    from ratatoskr.nifi.runbook import apply_approved_heal, build_heal_proposal, decide_approval

    prop = build_heal_proposal(_runbook_event(), heal_phase="safe")
    ack = decide_approval(prop, auto_approve=False)
    result = apply_approved_heal(ack)
    assert result["ok"] is False
    assert result["skipped"] == "not_approved"
    assert result["heal_actions"] == []


def test_apply_approved_calls_monitor() -> None:
    from ratatoskr.nifi.runbook import apply_approved_heal, build_heal_proposal, decide_approval

    prop = build_heal_proposal(_runbook_event(), heal_phase="safe", dry_run=True)
    ack = decide_approval(prop, auto_approve=True)
    fake_cycle = {
        "audit": {"dry_run": True, "phase": "safe"},
        "heal_actions": [
            {"op": "start_processor", "name": "GenerateFlowFile", "ok": None, "proposed": True}
        ],
    }
    with patch("ratatoskr.nifi.client.NiFiClient"), patch(
        "ratatoskr.nifi.policy.run_monitor_cycle", return_value=fake_cycle
    ) as mock_cycle:
        result = apply_approved_heal(ack)
    assert result["ok"] is True
    assert result["dry_run"] is True
    assert mock_cycle.called
    assert result["heal_actions"][0]["name"] == "GenerateFlowFile"


def test_offline_demo_hitl_approve() -> None:
    import subprocess

    proc = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "demo_nifi_runbook.py"),
            "--offline",
            "--scenario",
            "stop-generate",
            "--heal",
            "--approve",
            "--dry-run-heal",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "HITL" in proc.stdout or "proposal" in proc.stdout.lower()
    assert "approved" in proc.stdout.lower() or '"approved": true' in proc.stdout.lower()


def test_topics_registered() -> None:
    from ratatoskr.kafka_sources import STUDIO_CATALOG_TOPICS
    from ratatoskr.nifi.runbook import ACK_TOPIC, PROPOSE_TOPIC

    assert PROPOSE_TOPIC in STUDIO_CATALOG_TOPICS
    assert ACK_TOPIC in STUDIO_CATALOG_TOPICS


def main() -> int:
    tests = [
        test_build_proposal_from_runbook,
        test_decide_auto_approve_reject,
        test_prompt_approve_yes,
        test_attach_hitl_keeps_mutations_empty,
        test_apply_skipped_when_not_approved,
        test_apply_approved_calls_monitor,
        test_offline_demo_hitl_approve,
        test_topics_registered,
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
