#!/usr/bin/env python3
"""Phase 1 tests: react_nifi_runbook agent, LLM path mocked, manifest registration."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_build_runbook_fallback_no_llm() -> None:
    from examples.agents.react_nifi_runbook_logic import build_runbook
    from ratatoskr.nifi.runbook import is_valid_runbook_event, load_fixture

    with patch(
        "ratatoskr.designer.llm_settings.get_react_llm_settings"
    ) as mock_settings:
        settings = MagicMock()
        settings.is_complete.return_value = False
        mock_settings.return_value = settings
        out = build_runbook(load_fixture("stop-generate"))

    assert is_valid_runbook_event(out)
    assert out["agent"] == "react_nifi_runbook"
    assert out["mutations"] == []
    assert out["runbook"]["mode"] == "fallback"
    assert "start_processor:GenerateFlowFile" in out["runbook"]["remediation"]["safe_options"]


def test_build_runbook_llm_path_mocked() -> None:
    from examples.agents.react_nifi_runbook_logic import build_runbook
    from ratatoskr.nifi.runbook import is_valid_runbook_event, load_fixture

    llm_payload = {
        "headline": "Stopped GenerateFlowFile",
        "situation": "Monitor reports STOPPED on GenerateFlowFile.",
        "likely_causes": [
            {
                "cause": "Processor stopped",
                "confidence": "high",
                "evidence": ["STOPPED", "stopped:GenerateFlowFile"],
            }
        ],
        "diagnostic_steps": [
            {
                "step": "Confirm GenerateFlowFile run status in UI",
                "where": "UI",
                "expect": "STOPPED until started",
            }
        ],
        "remediation": {
            "safe_options": ["start_processor:GenerateFlowFile"],
            "lab_options": [],
            "do_not": ["Do not empty queues"],
        },
        "verify": ["classification.healthy true after start"],
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
        out = build_runbook(load_fixture("stop-generate"))

    assert is_valid_runbook_event(out)
    assert out["mutations"] == []
    assert out["runbook"]["mode"] == "llm"
    assert out["runbook"]["headline"] == "Stopped GenerateFlowFile"
    assert "start_processor:GenerateFlowFile" in out["runbook"]["remediation"]["safe_options"]


def test_llm_failure_falls_back() -> None:
    from examples.agents.react_nifi_runbook_logic import build_runbook
    from ratatoskr.nifi.runbook import is_valid_runbook_event, load_fixture

    with patch(
        "ratatoskr.designer.llm_settings.get_react_llm_settings"
    ) as mock_settings, patch(
        "ratatoskr.designer.llm_client.chat_completion_json",
        side_effect=RuntimeError("inference down"),
    ):
        settings = MagicMock()
        settings.is_complete.return_value = True
        mock_settings.return_value = settings
        out = build_runbook(load_fixture("invalid-log"))

    assert is_valid_runbook_event(out)
    assert out["runbook"]["mode"] == "fallback"
    assert "llm_error" in out
    assert out["mutations"] == []


def test_parse_llm_runbook_coerces_strings() -> None:
    from examples.agents.react_nifi_runbook_logic import parse_llm_runbook
    from ratatoskr.nifi.runbook import is_valid_runbook

    monitor = {
        "health": {
            "stopped_processors": [
                {"id": "1", "name": "Foo", "state": "STOPPED", "validationStatus": "VALID"}
            ],
            "disabled_controller_services": [],
            "invalid_processors": [],
            "queued_connections": [],
        },
        "heal_plan": [],
        "classification": {"severities": ["STOPPED"], "healthy": False},
    }
    rb = parse_llm_runbook(
        {
            "headline": "x",
            "situation": "y",
            "likely_causes": ["plain string cause"],
            "diagnostic_steps": ["look at canvas"],
            "remediation": {
                "safe_options": "start_processor:Foo",
                "lab_options": [],
                "do_not": "no empty",
            },
            "verify": "re-poll",
        },
        monitor_event=monitor,
    )
    assert is_valid_runbook(rb)
    assert rb["mode"] == "llm"
    assert rb["likely_causes"][0]["cause"] == "plain string cause"
    assert "start_processor:Foo" in rb["remediation"]["safe_options"]


def test_slim_monitor_event() -> None:
    from examples.agents.react_nifi_runbook_logic import slim_monitor_event
    from ratatoskr.nifi.runbook import load_fixture

    slim = slim_monitor_event(load_fixture("stop-generate"))
    assert "classification" in slim
    assert slim["heal_plan"]
    assert slim["heal_plan"][0]["op"] == "start_processor"


def test_agent_registered() -> None:
    from ratatoskr.agents.catalog import load_agent_catalog
    from ratatoskr.agents.registry import load_agent_registry

    manifest = load_agent_registry(validate=False)
    assert "react_nifi_runbook" in manifest.agents
    entry = manifest.agents["react_nifi_runbook"]
    assert entry.type == "react"
    assert "NiFiRunbookAgent" in entry.entry

    catalog = load_agent_catalog(validate=False)
    found = False
    for cat in catalog.categories:
        for sub in cat.subcategories:
            for agent in sub.agents:
                if agent.manifest == "react_nifi_runbook":
                    found = True
    assert found, "react_nifi_runbook missing from agent-catalog.yaml"


def test_runner_fixture_path() -> None:
    """Smoke: runner module imports and fixture path works via logic."""
    from examples.agents.react_nifi_runbook_logic import build_runbook
    from ratatoskr.nifi.runbook import list_fixture_ids, load_fixture

    assert "stop-generate" in list_fixture_ids()
    with patch(
        "ratatoskr.designer.llm_settings.get_react_llm_settings"
    ) as mock_settings:
        settings = MagicMock()
        settings.is_complete.return_value = False
        mock_settings.return_value = settings
        out = build_runbook(load_fixture("queue-backlog"))
    assert out["mutations"] == []
    assert out["runbook"]["headline"]


def main() -> int:
    tests = [
        test_build_runbook_fallback_no_llm,
        test_build_runbook_llm_path_mocked,
        test_llm_failure_falls_back,
        test_parse_llm_runbook_coerces_strings,
        test_slim_monitor_event,
        test_agent_registered,
        test_runner_fixture_path,
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
