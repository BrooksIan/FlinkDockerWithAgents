#!/usr/bin/env python3
"""POC polish tests: scenario heal scope + apply status line."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_scenario_heal_scope_stop_generate() -> None:
    from ratatoskr.nifi.runbook.demo import (
        SCENARIOS,
        filter_ops_by_scenario,
        scenario_heal_regex,
        scenario_watch_regex,
    )

    sc = SCENARIOS["stop-generate"]
    assert "GenerateFlowFile" in (scenario_watch_regex(sc) or "")
    assert scenario_heal_regex(sc) == r"^(?:GenerateFlowFile)$"
    ops = [
        "enable_controller_service:JsonTreeReader",
        "start_processor:GenerateFlowFile",
        "start_processor:ReplayPublish",
    ]
    assert filter_ops_by_scenario(ops, sc) == ["start_processor:GenerateFlowFile"]


def test_scoped_nifi_env_restores() -> None:
    from ratatoskr.nifi.runbook.demo import scoped_nifi_env

    os.environ.pop("NIFI_WATCH_NAME_REGEX", None)
    os.environ["NIFI_HEAL_ALLOW_NAME_REGEX"] = "KeepMe"
    with scoped_nifi_env(watch_regex="^Foo$", heal_regex="^Bar$"):
        assert os.environ["NIFI_WATCH_NAME_REGEX"] == "^Foo$"
        assert os.environ["NIFI_HEAL_ALLOW_NAME_REGEX"] == "^Bar$"
    assert "NIFI_WATCH_NAME_REGEX" not in os.environ
    assert os.environ["NIFI_HEAL_ALLOW_NAME_REGEX"] == "KeepMe"
    os.environ.pop("NIFI_HEAL_ALLOW_NAME_REGEX", None)


def test_format_apply_status() -> None:
    from ratatoskr.nifi.runbook.demo import format_apply_status

    line = format_apply_status(
        {
            "dry_run": True,
            "phase": "safe",
            "executed_ok": 0,
            "heal_actions": [
                {"ok": None, "proposed": True},
                {"ok": None, "proposed": True},
            ],
            "audit": {"dry_run": True, "phase": "safe"},
        }
    )
    assert "dry_run=True" in line
    assert "planned_only=2" in line
    assert "executed_ok=0" in line


def test_build_proposal_allow_ops() -> None:
    from ratatoskr.nifi.runbook import build_heal_proposal, fallback_runbook, load_fixture

    # Synthesize a runbook that lists extra ops, then filter
    rb = fallback_runbook(load_fixture("stop-generate"))
    rb["runbook"]["remediation"]["safe_options"] = [
        "start_processor:GenerateFlowFile",
        "start_processor:ReplayPublish",
    ]
    prop = build_heal_proposal(
        rb,
        heal_phase="safe",
        allow_ops=["start_processor:GenerateFlowFile"],
    )
    assert prop["proposed_ops"] == ["start_processor:GenerateFlowFile"]


def test_apply_passes_heal_regex() -> None:
    from ratatoskr.nifi.runbook.hitl import apply_approved_heal

    ack = {
        "approved": True,
        "heal_phase": "safe",
        "dry_run": True,
        "proposal_id": "p1",
    }
    seen: dict = {}

    def _cycle(*_a, **_k):
        seen["allow"] = os.environ.get("NIFI_HEAL_ALLOW_NAME_REGEX")
        return {"audit": {"dry_run": True}, "heal_actions": []}

    with patch("ratatoskr.nifi.client.NiFiClient"), patch(
        "ratatoskr.nifi.policy.run_monitor_cycle", side_effect=_cycle
    ):
        apply_approved_heal(ack, heal_name_regex=r"^(?:GenerateFlowFile)$")
    assert seen["allow"] == r"^(?:GenerateFlowFile)$"
    assert os.environ.get("NIFI_HEAL_ALLOW_NAME_REGEX") is None


def test_offline_scoped_proposal() -> None:
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
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "start_processor:GenerateFlowFile" in proc.stdout
    assert '"approved": true' in proc.stdout.lower() or "approved" in proc.stdout.lower()


def main() -> int:
    tests = [
        test_scenario_heal_scope_stop_generate,
        test_scoped_nifi_env_restores,
        test_format_apply_status,
        test_build_proposal_allow_ops,
        test_apply_passes_heal_regex,
        test_offline_scoped_proposal,
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
