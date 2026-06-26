"""Flink REST client for the control API."""

from __future__ import annotations

import urllib.error
from typing import Any

from ratatoskr.runtime import flink_cluster_submit


class FlinkUnavailableError(RuntimeError):
    """Flink JobManager REST is not reachable."""


def _rest_ports_to_try(explicit: int | None = None) -> list[int]:
    """Studio minimal stack first, then env/profile default (e.g. honeypot)."""
    if explicit is not None:
        return [explicit]
    from ratatoskr.flink_rest import default_flink_rest_port, studio_flink_rest_port

    ports: list[int] = []
    for port in (studio_flink_rest_port(), default_flink_rest_port()):
        if port not in ports:
            ports.append(port)
    return ports


def _fetch_one(path: str, *, rest_port: int) -> dict[str, Any]:
    try:
        return flink_cluster_submit.fetch_json(path, rest_port=rest_port)
    except urllib.error.HTTPError:
        raise
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise FlinkUnavailableError(str(exc)) from exc


def _fetch(path: str, *, rest_port: int | None = None) -> dict[str, Any]:
    last_error: FlinkUnavailableError | None = None
    for port in _rest_ports_to_try(rest_port):
        try:
            return _fetch_one(path, rest_port=port)
        except FlinkUnavailableError as exc:
            last_error = exc
    assert last_error is not None
    raise last_error


def cluster_overview(*, rest_port: int | None = None) -> dict[str, Any]:
    return _fetch("/overview", rest_port=rest_port)


def list_jobs(*, rest_port: int | None = None) -> list[dict[str, Any]]:
    if rest_port is not None:
        ports = [rest_port]
    else:
        ports = _rest_ports_to_try()
    last_error: FlinkUnavailableError | None = None
    for port in ports:
        try:
            data = _fetch_one("/jobs/overview", rest_port=port)
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
        except FlinkUnavailableError as exc:
            last_error = exc
    assert last_error is not None
    raise last_error


def get_job(job_id: str, *, rest_port: int | None = None) -> dict[str, Any]:
    last_error: FlinkUnavailableError | None = None
    last_missing: KeyError | None = None
    for port in _rest_ports_to_try(rest_port):
        try:
            return _fetch_one(f"/jobs/{job_id}", rest_port=port)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                last_missing = KeyError(job_id)
                continue
            raise FlinkUnavailableError(str(exc)) from exc
        except FlinkUnavailableError as exc:
            last_error = exc
    if last_missing is not None:
        raise last_missing
    if last_error is not None:
        raise last_error
    raise KeyError(job_id)


def taskmanager_summary(*, rest_port: int | None = None) -> dict[str, Any]:
    data = _fetch("/taskmanagers", rest_port=rest_port)
    managers = data.get("taskmanagers") or []
    total_slots = sum(int(tm.get("slotsNumber") or 0) for tm in managers)
    free_slots = sum(int(tm.get("freeSlots") or 0) for tm in managers)
    return {
        "count": len(managers),
        "slots_total": total_slots,
        "slots_free": free_slots,
        "taskmanagers": managers,
    }


def cancel_job(job_id: str, *, rest_port: int | None = None) -> None:
    last_error: Exception | None = None
    for port in _rest_ports_to_try(rest_port):
        try:
            flink_cluster_submit.cancel_job(job_id, rest_port=port)
            return
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise FlinkUnavailableError(str(last_error)) from last_error
    raise FlinkUnavailableError(f"Could not cancel job {job_id}")
