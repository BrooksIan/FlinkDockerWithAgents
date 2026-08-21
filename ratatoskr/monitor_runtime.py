"""Managed continuous monitor processes (host local runners)."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ratatoskr.monitor_mode import DEFAULT_MONITOR_INTERVAL_SEC, monitor_interval_sec
from ratatoskr.paths import project_root

STATE_DIRNAME = ".ratatoskr/monitor"
STATE_FILE = "state.json"

AGENT_RUNNERS = {
    "nifi": {
        "agent": "workflow_nifi_monitor",
        "runner": "examples/agents/run_workflow_nifi_monitor_local.py",
        "log": "nifi.log",
        "phase_env": "NIFI_HEAL_PHASE",
    },
    "kafka": {
        "agent": "workflow_kafka_monitor",
        "runner": "examples/agents/run_workflow_kafka_monitor_local.py",
        "log": "kafka.log",
        "phase_env": "KAFKA_HEAL_PHASE",
    },
}


@dataclass
class MonitorProc:
    key: str
    agent: str
    pid: int
    log: str
    started_at: str


@dataclass
class MonitorState:
    started_at: str
    interval: float
    phase: str
    processes: list[MonitorProc] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at,
            "interval": self.interval,
            "phase": self.phase,
            "processes": [asdict(p) for p in self.processes],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MonitorState":
        procs = [
            MonitorProc(**p) for p in (data.get("processes") or []) if isinstance(p, dict)
        ]
        return cls(
            started_at=str(data.get("started_at") or ""),
            interval=float(data.get("interval") or DEFAULT_MONITOR_INTERVAL_SEC),
            phase=str(data.get("phase") or "monitor"),
            processes=procs,
        )


def monitor_state_dir(*, root: Path | None = None) -> Path:
    return (root or project_root()) / STATE_DIRNAME


def monitor_state_path(*, root: Path | None = None) -> Path:
    return monitor_state_dir(root=root) / STATE_FILE


def load_state(*, root: Path | None = None) -> MonitorState | None:
    path = monitor_state_path(root=root)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return MonitorState.from_dict(data)


def save_state(state: MonitorState, *, root: Path | None = None) -> Path:
    d = monitor_state_dir(root=root)
    d.mkdir(parents=True, exist_ok=True)
    path = monitor_state_path(root=root)
    path.write_text(json.dumps(state.to_dict(), indent=2) + "\n", encoding="utf-8")
    return path


def clear_state(*, root: Path | None = None) -> None:
    path = monitor_state_path(root=root)
    if path.is_file():
        path.unlink()


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def refresh_state(*, root: Path | None = None) -> MonitorState | None:
    state = load_state(root=root)
    if state is None:
        return None
    alive = [p for p in state.processes if _pid_alive(p.pid)]
    if not alive:
        clear_state(root=root)
        return None
    if len(alive) != len(state.processes):
        state.processes = alive
        save_state(state, root=root)
    return state


def start_monitors(
    *,
    nifi: bool = True,
    kafka: bool = True,
    interval: float | None = None,
    phase: str = "monitor",
    root: Path | None = None,
    foreground: bool = False,
) -> MonitorState:
    """
    Start continuous local runners.

    Background (default): detach processes, write state file.
    Foreground: run a single combined loop in-process (nifi then kafka each tick)
    when both selected; otherwise one agent until Ctrl-C.
    """
    repo = root or project_root()
    existing = refresh_state(root=repo)
    if existing is not None:
        raise RuntimeError(
            "Continuous monitors already running. "
            "Use `ratatoskr monitor status` or `ratatoskr monitor stop` first."
        )

    keys = []
    if nifi:
        keys.append("nifi")
    if kafka:
        keys.append("kafka")
    if not keys:
        raise ValueError("Select at least one of --nifi / --kafka")

    interval_sec = float(interval) if interval is not None else monitor_interval_sec()
    if interval_sec <= 0:
        interval_sec = DEFAULT_MONITOR_INTERVAL_SEC

    if foreground:
        return _run_foreground(keys, interval_sec=interval_sec, phase=phase, root=repo)

    state_dir = monitor_state_dir(root=repo)
    state_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    procs: list[MonitorProc] = []

    env = os.environ.copy()
    env["MONITOR_MODE"] = "continuous"
    env["MONITOR_INTERVAL_SEC"] = str(interval_sec)
    env["NIFI_HEAL_PHASE"] = phase
    env["KAFKA_HEAL_PHASE"] = phase

    for key in keys:
        meta = AGENT_RUNNERS[key]
        runner = repo / meta["runner"]
        if not runner.is_file():
            raise FileNotFoundError(f"Runner not found: {runner}")
        log_path = state_dir / meta["log"]
        log_f = open(log_path, "a", encoding="utf-8")  # noqa: SIM115
        log_f.write(f"\n=== start {now} interval={interval_sec}s phase={phase} ===\n")
        log_f.flush()
        cmd = [
            sys.executable,
            str(runner),
            "--continuous",
            "--interval",
            str(interval_sec),
        ]
        proc = subprocess.Popen(
            cmd,
            cwd=str(repo),
            env=env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        procs.append(
            MonitorProc(
                key=key,
                agent=meta["agent"],
                pid=proc.pid,
                log=str(log_path.relative_to(repo)),
                started_at=now,
            )
        )

    state = MonitorState(
        started_at=now,
        interval=interval_sec,
        phase=phase,
        processes=procs,
    )
    save_state(state, root=repo)
    return state


def _run_foreground(
    keys: list[str],
    *,
    interval_sec: float,
    phase: str,
    root: Path,
) -> MonitorState:
    """Block until Ctrl-C; returns a synthetic finished state."""
    os.environ["MONITOR_MODE"] = "continuous"
    os.environ["MONITOR_INTERVAL_SEC"] = str(interval_sec)
    os.environ["NIFI_HEAL_PHASE"] = phase
    os.environ["KAFKA_HEAL_PHASE"] = phase
    now = datetime.now(timezone.utc).isoformat()
    print(
        f"Continuous monitors (foreground): {', '.join(keys)} "
        f"every {interval_sec}s phase={phase} — Ctrl-C to stop",
        flush=True,
    )
    n = 0
    try:
        while True:
            n += 1
            if "nifi" in keys:
                from examples.agents.run_workflow_nifi_monitor_local import (
                    _one_cycle as nifi_cycle,
                    _print_result as nifi_print,
                )

                nifi_print(
                    nifi_cycle(),
                    label=f"NiFi continuous poll #{n} (interval={interval_sec}s)",
                )
            if "kafka" in keys:
                from examples.agents.run_workflow_kafka_monitor_local import (
                    _one_cycle as kafka_cycle,
                    _print_result as kafka_print,
                )

                kafka_print(
                    kafka_cycle(),
                    label=f"Kafka continuous poll #{n} (interval={interval_sec}s)",
                )
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\nStopped continuous monitors.", flush=True)
    return MonitorState(
        started_at=now,
        interval=interval_sec,
        phase=phase,
        processes=[],
    )


def stop_monitors(*, root: Path | None = None, timeout_sec: float = 5.0) -> list[int]:
    """SIGTERM then SIGKILL remaining PIDs; clear state. Returns stopped pids."""
    repo = root or project_root()
    state = load_state(root=repo)
    if state is None:
        return []
    stopped: list[int] = []
    for proc in state.processes:
        if not _pid_alive(proc.pid):
            continue
        try:
            os.kill(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        stopped.append(proc.pid)
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if not any(_pid_alive(p) for p in stopped):
            break
        time.sleep(0.2)
    for pid in stopped:
        if _pid_alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    clear_state(root=repo)
    return stopped


def status_dict(*, root: Path | None = None) -> dict[str, Any]:
    state = refresh_state(root=root)
    if state is None:
        return {"running": False, "processes": []}
    return {
        "running": True,
        "started_at": state.started_at,
        "interval": state.interval,
        "phase": state.phase,
        "processes": [
            {
                **asdict(p),
                "alive": _pid_alive(p.pid),
            }
            for p in state.processes
        ],
    }
