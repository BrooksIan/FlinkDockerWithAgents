#!/usr/bin/env python3
"""Gate tests for NiFi monitor heal phases (mocked HTTP — no live NiFi required)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _health_fixture(**overrides: Any) -> dict[str, Any]:
    base = {
        "process_group_id": "pg-1",
        "healthy": False,
        "severities": ["STOPPED"],
        "stopped_processors": [
            {
                "id": "proc-stopped",
                "name": "GenerateFlowFile",
                "state": "STOPPED",
                "revision": {"version": 3},
            }
        ],
        "invalid_processors": [
            {
                "id": "proc-invalid",
                "name": "Broken",
                "state": "STOPPED",
                "validationStatus": "INVALID",
                "revision": {"version": 1},
            }
        ],
        "disabled_controller_services": [
            {
                "id": "svc-1",
                "name": "DemoService",
                "state": "DISABLED",
                "revision": {"version": 2},
            }
        ],
        "queued_connections": [
            {
                "id": "conn-1",
                "name": "gen-to-update",
                "flowFilesQueued": 42,
                "bytesQueued": 100,
            }
        ],
        "bulletins": [],
        "counts": {"processors": 3, "connections": 2, "controller_services": 1},
    }
    base.update(overrides)
    return base


def test_classify_health() -> None:
    from ratatoskr.nifi.policy import classify_health

    healthy = classify_health({"healthy": True, "severities": []})
    assert healthy["level"] == "OK"
    assert healthy["healthy"] is True

    stopped = classify_health(_health_fixture())
    assert stopped["level"] == "MEDIUM"
    assert "STOPPED" in stopped["severities"]


def test_phase_1a_monitor_no_mutations() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, run_monitor_cycle

    client = NiFiClient()
    client.start_processor = MagicMock()  # type: ignore[method-assign]
    client.enable_controller_service = MagicMock()  # type: ignore[method-assign]
    client.terminate_processor = MagicMock()  # type: ignore[method-assign]
    client.empty_connection_queue = MagicMock()  # type: ignore[method-assign]
    client.get_flow_health_status = MagicMock(return_value=_health_fixture())  # type: ignore[method-assign]

    actions = apply_heal_policy(client, _health_fixture(), phase="monitor")
    assert actions == []
    client.start_processor.assert_not_called()
    client.enable_controller_service.assert_not_called()
    client.terminate_processor.assert_not_called()
    client.empty_connection_queue.assert_not_called()

    result = run_monitor_cycle(client, "root", phase="monitor")
    assert result["phase"] == "monitor"
    assert result["heal_actions"] == []
    assert result["mutations"] == []
    assert result["classification"]["level"] == "MEDIUM"


def test_phase_1b_safe_start_and_enable() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy

    client = NiFiClient()
    client.start_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.enable_controller_service = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.terminate_processor = MagicMock()  # type: ignore[method-assign]
    client.empty_connection_queue = MagicMock()  # type: ignore[method-assign]

    actions = apply_heal_policy(client, _health_fixture(), phase="safe")
    ops = [a["op"] for a in actions if a.get("ok")]
    assert "start_processor" in ops
    assert "enable_controller_service" in ops
    client.start_processor.assert_called_once_with("proc-stopped", 3)
    client.enable_controller_service.assert_called_once_with("svc-1", 2)
    client.terminate_processor.assert_not_called()
    client.empty_connection_queue.assert_not_called()


def test_phase_1c_lab_without_empty_flag() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy

    os.environ.pop("NIFI_HEAL_ALLOW_EMPTY_QUEUE", None)
    client = NiFiClient()
    client.start_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.enable_controller_service = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.terminate_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.empty_connection_queue = MagicMock()  # type: ignore[method-assign]

    actions = apply_heal_policy(client, _health_fixture(), phase="lab")
    ops = [a["op"] for a in actions if a.get("ok")]
    assert "terminate_processor" in ops
    assert "empty_connection_queue" not in ops
    client.terminate_processor.assert_called_once_with("proc-invalid", 1)
    client.empty_connection_queue.assert_not_called()


def test_phase_1c_lab_with_empty_flag() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy

    os.environ["NIFI_HEAL_ALLOW_EMPTY_QUEUE"] = "1"
    try:
        client = NiFiClient()
        client.start_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        client.enable_controller_service = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        client.terminate_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        client.empty_connection_queue = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]

        actions = apply_heal_policy(client, _health_fixture(), phase="lab")
        ops = [a["op"] for a in actions if a.get("ok")]
        assert "empty_connection_queue" in ops
        client.empty_connection_queue.assert_called_once_with("conn-1")
    finally:
        os.environ.pop("NIFI_HEAL_ALLOW_EMPTY_QUEUE", None)


def test_agent_registered() -> None:
    from ratatoskr.agents.registry import load_agent_registry

    manifest = load_agent_registry(validate=False)
    assert "workflow_nifi_monitor" in manifest.agents


def test_compose_nifi_profile_files() -> None:
    from ratatoskr.constants import NIFI_PROFILE
    from ratatoskr.docker_utils import compose_files

    files = compose_files(NIFI_PROFILE)
    assert len(files) == 2
    assert files[0].name == "docker-compose.yml"
    assert "deploy" in str(files[0])
    assert files[1].name == "docker-compose.yml"
    assert "nifi" in str(files[1])
    assert all(p.is_file() for p in files)


def main() -> int:
    tests = [
        test_classify_health,
        test_phase_1a_monitor_no_mutations,
        test_phase_1b_safe_start_and_enable,
        test_phase_1c_lab_without_empty_flag,
        test_phase_1c_lab_with_empty_flag,
        test_agent_registered,
        test_compose_nifi_profile_files,
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
