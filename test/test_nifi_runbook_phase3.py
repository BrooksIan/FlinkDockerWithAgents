#!/usr/bin/env python3
"""Phase 3 tests: demo orchestration helpers + offline scenario path."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_list_scenarios() -> None:
    from ratatoskr.nifi.runbook import RUNBOOK_BRIEF_TOPIC, list_scenarios

    rows = list_scenarios()
    ids = {r["id"] for r in rows}
    assert "stop-generate" in ids
    assert "invalid-log" in ids
    assert RUNBOOK_BRIEF_TOPIC == "nifi.runbook.brief"


def test_offline_stop_generate() -> None:
    from ratatoskr.nifi.runbook import is_valid_runbook_event, run_offline_scenario

    with patch(
        "ratatoskr.designer.llm_settings.get_react_llm_settings"
    ) as mock_settings:
        settings = MagicMock()
        settings.is_complete.return_value = False
        mock_settings.return_value = settings
        result = run_offline_scenario("stop-generate")

    assert result["scenario"] == "stop-generate"
    assert is_valid_runbook_event(result["runbook"])
    assert result["runbook"]["mutations"] == []
    assert "start_processor:GenerateFlowFile" in result["runbook_summary"]["safe_options"]
    assert any("did not mutate" in t.lower() or "mutations" in t.lower() for t in result["talking_points"])


def test_offline_invalid_log_lab() -> None:
    from ratatoskr.nifi.runbook import run_offline_scenario

    with patch(
        "ratatoskr.designer.llm_settings.get_react_llm_settings"
    ) as mock_settings:
        settings = MagicMock()
        settings.is_complete.return_value = False
        mock_settings.return_value = settings
        result = run_offline_scenario("invalid-log")

    lab = result["runbook_summary"]["lab_options"]
    assert any("LogAttribute" in x for x in lab)
    assert result["meta"]["heal_phase"] == "lab"


def test_summarize_helpers() -> None:
    from ratatoskr.nifi.runbook import (
        load_fixture,
        summarize_monitor,
        summarize_runbook,
        fallback_runbook,
    )

    mon = load_fixture("stop-generate")
    ms = summarize_monitor(mon)
    assert "STOPPED" in (ms.get("severities") or [])
    rb = fallback_runbook(mon)
    rs = summarize_runbook(rb)
    assert rs["headline"]
    assert rs["mutations"] == []


def test_topic_in_catalog() -> None:
    from ratatoskr.kafka_sources import STUDIO_CATALOG_TOPICS, topic_description

    assert "nifi.runbook.brief" in STUDIO_CATALOG_TOPICS
    assert "runbook" in topic_description("nifi.runbook.brief").lower()


def test_demo_script_list() -> None:
    import subprocess

    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "demo_nifi_runbook.py"), "--list"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "stop-generate" in proc.stdout


def main() -> int:
    tests = [
        test_list_scenarios,
        test_offline_stop_generate,
        test_offline_invalid_log_lab,
        test_summarize_helpers,
        test_topic_in_catalog,
        test_demo_script_list,
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
