#!/usr/bin/env python3
"""
Ensure Cowrie Phase 1 / 1.5 / 2 Flink streaming jobs are RUNNING on the compose cluster.

Used by ``flink-pipeline-supervisor`` (and legacy per-phase sidecars) and by
``flink-cowrie ensure-flink-jobs``.

Each sidecar supervises its phase and upstream dependencies, then watches for
missing or failed jobs (e.g. after JobManager restart) and re-submits them.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Callable, Dict, List, Optional, Sequence

from flink_cluster_submit import (
    PHASE1_JOB_NAME,
    PHASE15_JOB_NAME,
    PHASE2_JOB_NAME,
    ensure_running_job_name,
    find_running_jobs,
    job_state_by_name,
    wait_for_flink_cluster,
    wait_for_running_job_name,
)

SubmitFn = Callable[[], str]

PHASE_ORDER = ("1", "1.5", "2")

PHASE_JOB_NAMES: Dict[str, str] = {
    "1": PHASE1_JOB_NAME,
    "1.5": PHASE15_JOB_NAME,
    "2": PHASE2_JOB_NAME,
}

PHASE_DEPENDENCIES: Dict[str, List[str]] = {
    "1": [],
    "1.5": ["1"],
    "2": ["1", "1.5"],
}


def _submit_phase1() -> str:
    import cowrie_kafka_normalize_job as job

    return job.submit_remote_job(wait=True, wait_for_running=True)


def _submit_phase15() -> str:
    import cowrie_actor_classify_job as job

    return job.submit_remote_job(wait=True, wait_for_running=True)


def _submit_phase2() -> str:
    import cowrie_phase2_workflow_job as job

    return job.submit_remote_job(wait=True, wait_for_running=True)


PHASE_SUBMITTERS: Dict[str, SubmitFn] = {
    "1": _submit_phase1,
    "1.5": _submit_phase15,
    "2": _submit_phase2,
}


def _needs_resubmit(job_name: str) -> bool:
    running = find_running_jobs(job_name)
    if running:
        if len(running) > 1:
            print(
                f"Flink job {job_name!r} has {len(running)} RUNNING copies — will dedupe",
                flush=True,
            )
            return True
        return False
    state = job_state_by_name(job_name)
    if state:
        print(f"Flink job {job_name!r} state={state} — will (re)submit", flush=True)
    else:
        print(f"Flink job {job_name!r} missing — will submit", flush=True)
    return True


def ensure_phase(phase: str, *, max_attempts: int = 5) -> str:
    """Ensure one phase and its dependencies are RUNNING."""
    if phase not in PHASE_SUBMITTERS:
        raise ValueError(f"Unknown phase {phase!r}; expected one of {list(PHASE_ORDER)}")

    from cowrie_pipeline import ensure_pipeline_kafka_topics

    ensure_pipeline_kafka_topics()

    wait_for_flink_cluster(
        min_slots=int(os.environ.get("COWRIE_FLINK_MIN_SLOTS", "1")),
        timeout_sec=float(os.environ.get("COWRIE_FLINK_CLUSTER_TIMEOUT_SEC", "300")),
    )

    for dep in PHASE_DEPENDENCIES[phase]:
        dep_name = PHASE_JOB_NAMES[dep]
        if _needs_resubmit(dep_name):
            ensure_running_job_name(
                dep_name,
                PHASE_SUBMITTERS[dep],
                max_attempts=max_attempts,
            )
        else:
            print(f"Flink job {dep_name!r} already RUNNING", flush=True)

    job_name = PHASE_JOB_NAMES[phase]
    if not _needs_resubmit(job_name):
        print(f"Flink job {job_name!r} already RUNNING", flush=True)
        running = wait_for_running_job_name(job_name, timeout_sec=30)
        return running

    return ensure_running_job_name(
        job_name,
        PHASE_SUBMITTERS[phase],
        max_attempts=max_attempts,
    )


def ensure_all_phases(*, max_attempts: int = 5) -> Dict[str, str]:
    """Ensure Phase 1, 1.5, and 2 jobs are RUNNING in order."""
    job_ids: Dict[str, str] = {}
    for phase in PHASE_ORDER:
        job_ids[phase] = ensure_phase(phase, max_attempts=max_attempts)
    return job_ids


def watch_phases(
    phases: Sequence[str],
    *,
    interval_sec: float = 30.0,
    max_attempts: int = 5,
) -> None:
    """Periodically re-ensure selected phases stay RUNNING."""
    if interval_sec <= 0:
        return
    print(
        f"Watching Flink phases {list(phases)} every {interval_sec}s "
        f"(set COWRIE_FLINK_WATCH_INTERVAL=0 to disable)",
        flush=True,
    )
    while True:
        time.sleep(interval_sec)
        for phase in phases:
            job_name = PHASE_JOB_NAMES[phase]
            if _needs_resubmit(job_name):
                try:
                    ensure_phase(phase, max_attempts=max_attempts)
                except Exception as exc:
                    print(f"Watchdog failed for phase {phase}: {exc}", flush=True)


def _parse_phases(raw: Optional[str]) -> List[str]:
    if not raw or raw == "all":
        return list(PHASE_ORDER)
    if raw in PHASE_SUBMITTERS:
        return [raw]
    raise SystemExit(f"Unknown phase {raw!r}; use 1, 1.5, 2, or all")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        default="all",
        help="Pipeline phase to ensure: 1, 1.5, 2, or all (default: all)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Ensure jobs once and exit (no watchdog loop)",
    )
    parser.add_argument(
        "--watch-interval",
        type=float,
        default=float(os.environ.get("COWRIE_FLINK_WATCH_INTERVAL", "30")),
        help="Seconds between health checks (0 disables watchdog)",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=int(os.environ.get("COWRIE_FLINK_SUBMIT_MAX_ATTEMPTS", "5")),
        help="Submit retries per ensure call",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    phases = _parse_phases(args.phase)
    for phase in phases:
        ensure_phase(phase, max_attempts=args.max_attempts)
        print(f"Phase {phase} OK ({PHASE_JOB_NAMES[phase]})", flush=True)

    if args.once or args.watch_interval <= 0:
        return 0

    watch_phases(
        phases,
        interval_sec=args.watch_interval,
        max_attempts=args.max_attempts,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
