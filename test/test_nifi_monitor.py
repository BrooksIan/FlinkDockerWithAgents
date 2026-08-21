#!/usr/bin/env python3
"""Gate tests for NiFi monitor heal phases (mocked HTTP — no live NiFi required)."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

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
                "backpressure_level": "warn",
            }
        ],
        "bulletins": [],
        "probe": {"ok": True, "login_ms": 1.0, "poll_ms": 2.0},
        "counts": {"processors": 3, "connections": 2, "controller_services": 1},
    }
    base.update(overrides)
    return base


def _clear_heal_env() -> None:
    for key in (
        "NIFI_HEAL_ALLOW_EMPTY_QUEUE",
        "NIFI_HEAL_ALLOW_CONFIG_FIX",
        "NIFI_HEAL_ALLOW_RESTART",
        "NIFI_HEAL_RESTART_MIN_BULLETINS",
        "NIFI_HEAL_DRY_RUN",
        "NIFI_HEAL_VERIFY",
        "NIFI_HEAL_COOLDOWN_SEC",
        "NIFI_HEAL_MAX_MUTATIONS",
        "NIFI_HEAL_ALLOW_IDS",
        "NIFI_HEAL_ALLOW_NAME_REGEX",
        "NIFI_BP_WARN",
        "NIFI_BP_CRIT",
        "NIFI_EMPTY_QUEUE_MIN_FLOWFILES",
        "NIFI_WATCH_NAME_REGEX",
        "NIFI_WATCH_ID_REGEX",
    ):
        os.environ.pop(key, None)


def test_classify_health() -> None:
    from ratatoskr.nifi.policy import classify_health

    healthy = classify_health({"healthy": True, "severities": []})
    assert healthy["level"] == "OK"
    assert healthy["healthy"] is True
    assert healthy["score"] == 100

    stopped = classify_health(_health_fixture())
    assert stopped["level"] == "MEDIUM"
    assert "STOPPED" in stopped["severities"]
    assert stopped["score"] < 100


def test_classify_score_and_bulletin_groups() -> None:
    from ratatoskr.nifi.policy import classify_health

    health = _health_fixture(
        severities=["INVALID", "BULLETIN_ERROR"],
        bulletins=[
            {
                "fingerprint": "abc123",
                "level": "ERROR",
                "message": "boom",
                "sourceId": "p1",
                "sourceName": "X",
            },
            {
                "fingerprint": "abc123",
                "level": "ERROR",
                "message": "boom",
                "sourceId": "p1",
                "sourceName": "X",
            },
            {
                "fingerprint": "def456",
                "level": "WARNING",
                "message": "slow",
                "sourceId": "p2",
                "sourceName": "Y",
            },
        ],
    )
    c = classify_health(health)
    assert c["level"] == "HIGH"
    assert c["score"] == 100 - 30 - 35
    groups = {g["fingerprint"]: g["count"] for g in c["bulletin_groups"]}
    assert groups["abc123"] == 2
    assert groups["def456"] == 1


def test_diff_health() -> None:
    from ratatoskr.nifi.policy import diff_health

    prev = _health_fixture(
        stopped_processors=[{"id": "a", "name": "A"}],
        invalid_processors=[],
        disabled_controller_services=[],
        queued_connections=[],
    )
    curr = _health_fixture(
        stopped_processors=[{"id": "a", "name": "A"}, {"id": "b", "name": "B"}],
        invalid_processors=[],
        disabled_controller_services=[],
        queued_connections=[],
        severities=["STOPPED"],
    )
    d = diff_health(prev, curr)
    assert d["new"]["stopped_processors"] == ["b"]
    assert d["persistent"]["stopped_processors"] == ["a"]


def test_phase_1a_monitor_no_mutations() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, reset_heal_cooldown, run_monitor_cycle

    _clear_heal_env()
    reset_heal_cooldown()
    client = NiFiClient()
    client.start_processor = MagicMock()  # type: ignore[method-assign]
    client.enable_controller_service = MagicMock()  # type: ignore[method-assign]
    client.terminate_processor = MagicMock()  # type: ignore[method-assign]
    client.empty_connection_queue = MagicMock()  # type: ignore[method-assign]
    client.get_flow_health_status = MagicMock(return_value=_health_fixture())  # type: ignore[method-assign]

    actions = apply_heal_policy(client, _health_fixture(), phase="monitor")
    assert actions == []
    client.start_processor.assert_not_called()

    result = run_monitor_cycle(client, "root", phase="monitor")
    assert result["phase"] == "monitor"
    assert result["heal_actions"] == []
    assert result["heal_plan"] == []
    assert result["mutations"] == []
    assert result["poll_id"]
    assert result["ts"]
    assert result["audit"]["dry_run"] is False
    assert "probe" in result["health"]
    assert result["classification"]["level"] == "MEDIUM"
    assert "score" in result["classification"]


def test_phase_1b_safe_start_and_enable() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    os.environ["NIFI_HEAL_VERIFY"] = "0"
    client = NiFiClient()
    client.start_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.enable_controller_service = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.terminate_processor = MagicMock()  # type: ignore[method-assign]
    client.empty_connection_queue = MagicMock()  # type: ignore[method-assign]
    client.stop_processor = MagicMock()  # type: ignore[method-assign]

    actions = apply_heal_policy(client, _health_fixture(), phase="safe", verify=False)
    ops = [a["op"] for a in actions if a.get("ok")]
    assert "start_processor" in ops
    assert "enable_controller_service" in ops
    # enable before start (rules order)
    assert ops.index("enable_controller_service") < ops.index("start_processor")
    client.start_processor.assert_called_once_with("proc-stopped", 3)
    client.enable_controller_service.assert_called_once_with("svc-1", 2)
    client.terminate_processor.assert_not_called()
    client.empty_connection_queue.assert_not_called()


def test_phase_1c_lab_without_empty_flag() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    client = NiFiClient()
    client.start_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.enable_controller_service = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.terminate_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.fix_processor_config = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.restart_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.empty_connection_queue = MagicMock()  # type: ignore[method-assign]
    client.stop_processor = MagicMock()  # type: ignore[method-assign]

    actions = apply_heal_policy(client, _health_fixture(), phase="lab", verify=False)
    ops = [a["op"] for a in actions if a.get("ok")]
    # Fixture INVALID name is "Broken" → no config template → terminate
    assert "terminate_processor" in ops
    assert "fix_processor_config" not in ops
    assert "empty_connection_queue" not in ops
    client.terminate_processor.assert_called_once_with("proc-invalid", 1)
    client.empty_connection_queue.assert_not_called()


def test_lab_config_fix_for_logattribute_skips_terminate() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, build_heal_plan, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    os.environ["NIFI_HEAL_VERIFY"] = "0"
    health = _health_fixture(
        stopped_processors=[],
        disabled_controller_services=[],
        queued_connections=[],
        invalid_processors=[
            {
                "id": "log-1",
                "name": "LogAttribute",
                "state": "STOPPED",
                "validationStatus": "INVALID",
                "revision": {"version": 4},
            }
        ],
        severities=["INVALID"],
    )
    plan = build_heal_plan(health, phase="lab")
    ops = [p["op"] for p in plan]
    assert "fix_processor_config" in ops
    assert "terminate_processor" not in ops
    fix = next(p for p in plan if p["op"] == "fix_processor_config")
    assert fix["template"] == "auto_terminate_success"
    assert fix["auto_terminated_relationships"] == ["success"]

    client = NiFiClient()
    client.fix_processor_config = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.terminate_processor = MagicMock()  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="lab", verify=False)
    assert any(a.get("op") == "fix_processor_config" and a.get("ok") for a in actions)
    client.fix_processor_config.assert_called_once()
    client.terminate_processor.assert_not_called()


def test_lab_restart_on_repeated_bulletins() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, build_heal_plan, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    os.environ["NIFI_HEAL_VERIFY"] = "0"
    os.environ["NIFI_HEAL_RESTART_MIN_BULLETINS"] = "2"
    health = _health_fixture(
        stopped_processors=[],
        invalid_processors=[],
        disabled_controller_services=[],
        queued_connections=[],
        severities=["BULLETIN_ERROR"],
        bulletins=[
            {
                "fingerprint": "fp1",
                "level": "ERROR",
                "message": "kafka timeout",
                "sourceId": "consume-1",
                "sourceName": "ConsumeKafka",
            },
            {
                "fingerprint": "fp1",
                "level": "ERROR",
                "message": "kafka timeout",
                "sourceId": "consume-1",
                "sourceName": "ConsumeKafka",
            },
        ],
    )
    plan = build_heal_plan(health, phase="lab")
    assert any(p["op"] == "restart_processor" and p["id"] == "consume-1" for p in plan)
    safe_plan = build_heal_plan(health, phase="safe")
    assert not any(p["op"] == "restart_processor" for p in safe_plan)

    client = NiFiClient()
    client.restart_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="lab", verify=False)
    assert any(a.get("op") == "restart_processor" and a.get("ok") for a in actions)
    client.restart_processor.assert_called_once_with("consume-1", None)


def test_config_fix_disabled_falls_back_to_terminate() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    os.environ["NIFI_HEAL_ALLOW_CONFIG_FIX"] = "0"
    os.environ["NIFI_HEAL_VERIFY"] = "0"
    health = _health_fixture(
        stopped_processors=[],
        disabled_controller_services=[],
        queued_connections=[],
        invalid_processors=[
            {
                "id": "log-1",
                "name": "LogAttribute",
                "state": "STOPPED",
                "validationStatus": "INVALID",
                "revision": {"version": 1},
            }
        ],
        severities=["INVALID"],
    )
    client = NiFiClient()
    client.fix_processor_config = MagicMock()  # type: ignore[method-assign]
    client.terminate_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="lab", verify=False)
    assert any(a.get("op") == "terminate_processor" and a.get("ok") for a in actions)
    client.fix_processor_config.assert_not_called()


def test_phase_1c_lab_with_empty_flag() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    os.environ["NIFI_HEAL_ALLOW_EMPTY_QUEUE"] = "1"
    try:
        client = NiFiClient()
        client.start_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        client.enable_controller_service = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        client.terminate_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        client.empty_connection_queue = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        client.stop_processor = MagicMock()  # type: ignore[method-assign]

        actions = apply_heal_policy(client, _health_fixture(), phase="lab", verify=False)
        ops = [a["op"] for a in actions if a.get("ok")]
        assert "empty_connection_queue" in ops
        client.empty_connection_queue.assert_called_once_with("conn-1")
    finally:
        os.environ.pop("NIFI_HEAL_ALLOW_EMPTY_QUEUE", None)


def test_dry_run_proposes_without_mutate() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    client = NiFiClient()
    client.start_processor = MagicMock()  # type: ignore[method-assign]
    client.enable_controller_service = MagicMock()  # type: ignore[method-assign]

    actions = apply_heal_policy(client, _health_fixture(), phase="safe", dry_run=True)
    assert actions
    assert all(a.get("proposed") is True for a in actions)
    assert all(a.get("ok") is None for a in actions)
    client.start_processor.assert_not_called()
    client.enable_controller_service.assert_not_called()


def test_blast_radius_and_cooldown() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    os.environ["NIFI_HEAL_MAX_MUTATIONS"] = "1"
    os.environ["NIFI_HEAL_VERIFY"] = "0"
    client = NiFiClient()
    client.start_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.enable_controller_service = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]

    actions = apply_heal_policy(client, _health_fixture(), phase="safe", verify=False)
    ok = [a for a in actions if a.get("ok")]
    skipped = [a for a in actions if a.get("skipped") == "blast_radius"]
    assert len(ok) == 1
    assert skipped

    # Cooldown blocks immediate re-apply of the same op+id
    reset_heal_cooldown()
    os.environ.pop("NIFI_HEAL_MAX_MUTATIONS", None)
    os.environ["NIFI_HEAL_COOLDOWN_SEC"] = "60"
    client2 = NiFiClient()
    client2.enable_controller_service = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client2.start_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    first = apply_heal_policy(client2, _health_fixture(), phase="safe", verify=False)
    assert any(a.get("ok") for a in first)
    second = apply_heal_policy(client2, _health_fixture(), phase="safe", verify=False)
    assert any(a.get("skipped") == "cooldown" for a in second)


def test_allowlist_filters_actions() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    os.environ["NIFI_HEAL_ALLOW_IDS"] = "svc-1"
    os.environ["NIFI_HEAL_VERIFY"] = "0"
    client = NiFiClient()
    client.start_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.enable_controller_service = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]

    actions = apply_heal_policy(client, _health_fixture(), phase="safe", verify=False)
    ok_ops = [(a["op"], a["id"]) for a in actions if a.get("ok")]
    assert ok_ops == [("enable_controller_service", "svc-1")]
    assert any(a.get("skipped") == "allowlist" for a in actions)
    client.start_processor.assert_not_called()


def test_safer_queue_relief_stops_upstream_before_empty() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, build_heal_plan, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    os.environ["NIFI_HEAL_ALLOW_EMPTY_QUEUE"] = "1"
    os.environ["NIFI_HEAL_VERIFY"] = "0"
    health = _health_fixture(
        stopped_processors=[],
        invalid_processors=[],
        disabled_controller_services=[],
        queued_connections=[
            {
                "id": "conn-1",
                "name": "gen-to-update",
                "flowFilesQueued": 42,
                "sourceId": "proc-upstream",
                "sourceName": "GenerateFlowFile",
                "backpressure_level": "warn",
            }
        ],
        severities=["BACKPRESSURE_WARN", "BACKPRESSURE"],
    )
    plan = build_heal_plan(health, phase="lab")
    ops = [p["op"] for p in plan]
    assert ops.index("stop_processor") < ops.index("empty_connection_queue")

    client = NiFiClient()
    client.stop_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.empty_connection_queue = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    actions = apply_heal_policy(client, health, phase="lab", verify=False)
    client.stop_processor.assert_called_once_with("proc-upstream", None)
    client.empty_connection_queue.assert_called_once_with("conn-1")
    assert actions[0]["op"] == "stop_processor"


def test_verify_after_heal() -> None:
    from ratatoskr.nifi.client import NiFiClient
    from ratatoskr.nifi.policy import apply_heal_policy, reset_heal_cooldown

    _clear_heal_env()
    reset_heal_cooldown()
    client = NiFiClient()
    client.enable_controller_service = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    client.start_processor = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    after = _health_fixture(
        stopped_processors=[],
        disabled_controller_services=[],
        severities=[],
        healthy=True,
    )
    client.get_flow_health_status = MagicMock(return_value=after)  # type: ignore[method-assign]

    health = _health_fixture(
        invalid_processors=[],
        queued_connections=[],
        severities=["STOPPED", "DISABLED_SERVICE"],
    )
    actions = apply_heal_policy(client, health, phase="safe", verify=True)
    assert all(a.get("verified") is True for a in actions if a.get("ok"))


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


def test_cluster_script_registered() -> None:
    from ratatoskr.agents.registry import get_agent_spec

    spec = get_agent_spec("workflow_nifi_monitor")
    assert spec.cluster_script.endswith("run_workflow_nifi_monitor_cluster.py")
    root = Path(__file__).resolve().parents[1]
    assert (root / spec.cluster_script).is_file()


def test_default_nifi_api_base_host() -> None:
    from ratatoskr.nifi.env import default_nifi_api_base

    os.environ.pop("NIFI_API_BASE", None)
    base = default_nifi_api_base()
    assert "nifi-api" in base


def test_env_helpers() -> None:
    from ratatoskr.nifi import env as nifi_env

    _clear_heal_env()
    assert nifi_env.heal_phase() in nifi_env.HEAL_PHASES
    assert nifi_env.backpressure_warn_threshold() == 1
    assert nifi_env.backpressure_crit_threshold() == 100
    os.environ["NIFI_BP_WARN"] = "10"
    os.environ["NIFI_BP_CRIT"] = "50"
    assert nifi_env.backpressure_warn_threshold() == 10
    assert nifi_env.backpressure_crit_threshold() == 50
    os.environ["NIFI_HEAL_DRY_RUN"] = "1"
    assert nifi_env.heal_dry_run() is True


def main() -> int:
    tests = [
        test_classify_health,
        test_classify_score_and_bulletin_groups,
        test_diff_health,
        test_phase_1a_monitor_no_mutations,
        test_phase_1b_safe_start_and_enable,
        test_phase_1c_lab_without_empty_flag,
        test_lab_config_fix_for_logattribute_skips_terminate,
        test_lab_restart_on_repeated_bulletins,
        test_config_fix_disabled_falls_back_to_terminate,
        test_phase_1c_lab_with_empty_flag,
        test_dry_run_proposes_without_mutate,
        test_blast_radius_and_cooldown,
        test_allowlist_filters_actions,
        test_safer_queue_relief_stops_upstream_before_empty,
        test_verify_after_heal,
        test_agent_registered,
        test_compose_nifi_profile_files,
        test_cluster_script_registered,
        test_default_nifi_api_base_host,
        test_env_helpers,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"OK  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
        finally:
            _clear_heal_env()
            try:
                from ratatoskr.nifi.policy import reset_heal_cooldown

                reset_heal_cooldown()
            except Exception:  # noqa: BLE001
                pass
    if failed:
        print(f"FAILED {failed}/{len(tests)}")
        return 1
    print(f"PASS ({len(tests)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
