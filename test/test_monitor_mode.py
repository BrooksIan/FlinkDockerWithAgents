#!/usr/bin/env python3
"""Tests for continuous monitor mode (no live stack required)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_monitor_mode_defaults() -> None:
    from ratatoskr.monitor_mode import (
        is_continuous,
        kafka_monitor_polls,
        monitor_mode,
        nifi_monitor_polls,
    )

    os.environ.pop("MONITOR_MODE", None)
    os.environ.pop("MONITOR_CONTINUOUS", None)
    os.environ.pop("NIFI_MONITOR_POLLS", None)
    os.environ.pop("KAFKA_MONITOR_POLLS", None)
    assert monitor_mode() == "oneshot"
    assert is_continuous() is False
    assert nifi_monitor_polls() == 5
    assert kafka_monitor_polls() == 5


def test_monitor_mode_continuous_env() -> None:
    from ratatoskr.monitor_mode import is_continuous, nifi_monitor_polls

    os.environ["MONITOR_MODE"] = "continuous"
    try:
        assert is_continuous() is True
        assert nifi_monitor_polls() is None
    finally:
        os.environ.pop("MONITOR_MODE", None)


def test_monitor_polls_zero_is_continuous() -> None:
    from ratatoskr.monitor_mode import kafka_monitor_polls, nifi_monitor_polls

    os.environ.pop("MONITOR_MODE", None)
    os.environ["NIFI_MONITOR_POLLS"] = "0"
    os.environ["KAFKA_MONITOR_POLLS"] = "0"
    try:
        assert nifi_monitor_polls() is None
        assert kafka_monitor_polls() is None
    finally:
        os.environ.pop("NIFI_MONITOR_POLLS", None)
        os.environ.pop("KAFKA_MONITOR_POLLS", None)


def test_monitor_state_roundtrip(tmp_path: Path | None = None) -> None:
    from ratatoskr.monitor_runtime import (
        MonitorProc,
        MonitorState,
        clear_state,
        load_state,
        save_state,
    )

    root = Path(tmp_path) if tmp_path else ROOT / ".ratatoskr-test-monitor"
    root.mkdir(parents=True, exist_ok=True)
    try:
        clear_state(root=root)
        state = MonitorState(
            started_at="2026-01-01T00:00:00+00:00",
            interval=10.0,
            phase="monitor",
            processes=[
                MonitorProc(
                    key="nifi",
                    agent="workflow_nifi_monitor",
                    pid=1,
                    log=".ratatoskr/monitor/nifi.log",
                    started_at="2026-01-01T00:00:00+00:00",
                )
            ],
        )
        save_state(state, root=root)
        loaded = load_state(root=root)
        assert loaded is not None
        assert loaded.interval == 10.0
        assert loaded.processes[0].agent == "workflow_nifi_monitor"
        clear_state(root=root)
        assert load_state(root=root) is None
    finally:
        clear_state(root=root)


def test_cli_has_monitor() -> None:
    from ratatoskr.cli import app

    names = {cmd.name for cmd in app.registered_commands} | {
        g.name for g in getattr(app, "registered_groups", []) or []
    }
    # Typer stores groups differently — check help string / add_typer
    from typer.main import get_command

    click_app = get_command(app)
    assert "monitor" in click_app.list_commands(None)


def main() -> int:
    # tmp_path without pytest
    import tempfile

    tests = [
        test_monitor_mode_defaults,
        test_monitor_mode_continuous_env,
        test_monitor_polls_zero_is_continuous,
        test_cli_has_monitor,
    ]
    failed = 0
    for fn in tests:
        try:
            fn()
            print(f"OK  {fn.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"FAIL {fn.__name__}: {exc}")
    try:
        with tempfile.TemporaryDirectory() as td:
            test_monitor_state_roundtrip(Path(td))
        print("OK  test_monitor_state_roundtrip")
    except Exception as exc:  # noqa: BLE001
        failed += 1
        print(f"FAIL test_monitor_state_roundtrip: {exc}")
    if failed:
        print(f"FAILED {failed}")
        return 1
    print(f"PASS ({len(tests) + 1})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
