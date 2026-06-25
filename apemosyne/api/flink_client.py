"""Flink REST client for the control API."""

from __future__ import annotations

import urllib.error
from typing import Any

from apemosyne.runtime import flink_cluster_submit


class FlinkUnavailableError(RuntimeError):
    """Flink JobManager REST is not reachable."""


def _fetch(path: str) -> dict[str, Any]:
    try:
        return flink_cluster_submit.fetch_json(path)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise FlinkUnavailableError(str(exc)) from exc


def cluster_overview() -> dict[str, Any]:
    return _fetch("/overview")


def list_jobs() -> list[dict[str, Any]]:
    data = _fetch("/jobs/overview")
    jobs = data.get("jobs") or []
    return [
        {
            "id": job.get("jid"),
            "name": job.get("name"),
            "state": job.get("state"),
            "start_time": job.get("start-time"),
            "end_time": job.get("end-time"),
        }
        for job in jobs
    ]


def get_job(job_id: str) -> dict[str, Any]:
    try:
        return _fetch(f"/jobs/{job_id}")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise KeyError(job_id) from exc
        raise


def taskmanager_summary() -> dict[str, Any]:
    data = _fetch("/taskmanagers")
    managers = data.get("taskmanagers") or []
    total_slots = sum(int(tm.get("slotsNumber") or 0) for tm in managers)
    free_slots = sum(int(tm.get("freeSlots") or 0) for tm in managers)
    return {
        "count": len(managers),
        "slots_total": total_slots,
        "slots_free": free_slots,
        "taskmanagers": managers,
    }


def cancel_job(job_id: str) -> None:
    flink_cluster_submit.cancel_job(job_id)
