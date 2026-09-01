"""Read-only Cloudera Manager REST client for monitoring agents."""

from __future__ import annotations

import time
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import requests
from urllib3.exceptions import InsecureRequestWarning

from ratatoskr.cm.env import (
    cm_api_base,
    cm_api_version,
    cm_cluster,
    cm_knox_proxied,
    cm_password,
    cm_probe_slow_ms,
    cm_request_timeout_sec,
    cm_user,
    cm_verify_ssl,
    knox_token,
)

_BAD_HEALTH = frozenset({"BAD", "CONCERNING"})
_STOPPED_STATES = frozenset({"STOPPED", "STOPPING", "DOWN"})
_STALE_CONFIG = frozenset({"STALE", "OUTDATED"})


def _items(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        raw = data.get("items")
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
    return []


def _health_bad(summary: Any) -> bool:
    return str(summary or "").upper() in _BAD_HEALTH


@dataclass
class CMClient:
    """Thin read-only wrapper around the Cloudera Manager API."""

    base_url: str = ""
    api_version: str = ""
    cluster: str = ""
    username: str = ""
    password: str = ""
    verify_ssl: bool = True
    timeout_sec: float = 30.0
    mutations: list[dict[str, Any]] = field(default_factory=list, repr=False)
    _session: requests.Session | None = field(default=None, repr=False)
    _last_probe_ms: float = field(default=0.0, repr=False)

    def __post_init__(self) -> None:
        if not self.base_url:
            self.base_url = cm_api_base()
        if not self.api_version:
            self.api_version = cm_api_version()
        if not self.cluster:
            self.cluster = cm_cluster()
        if not self.username:
            self.username = cm_user()
        if not self.password:
            self.password = cm_password()
        if self.timeout_sec <= 0:
            self.timeout_sec = cm_request_timeout_sec()

    def _session_get(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            token = knox_token()
            if token:
                self._session.headers["Authorization"] = f"Bearer {token}"
            elif self.username:
                self._session.auth = (self.username, self.password)
            self._session.headers.setdefault("Content-Type", "application/json")
            if not self.verify_ssl:
                warnings.filterwarnings("ignore", category=InsecureRequestWarning)
        return self._session

    def _api_root(self) -> str:
        version = self.api_version
        if not version or version.lower() == "auto":
            version = self.detect_api_version()
        if not version.startswith("v"):
            version = f"v{version}"
        if cm_knox_proxied(self.base_url):
            return urljoin(f"{self.base_url.rstrip('/')}/", f"{version}/")
        return urljoin(f"{self.base_url.rstrip('/')}/", f"api/{version}/")

    def detect_api_version(self) -> str:
        """Resolve highest supported API version (falls back to v49)."""
        session = self._session_get()
        if cm_knox_proxied(self.base_url):
            url = urljoin(f"{self.base_url.rstrip('/')}/", "version")
        else:
            url = urljoin(f"{self.base_url.rstrip('/')}/", "api/version")
        try:
            resp = session.get(url, timeout=self.timeout_sec, verify=self.verify_ssl)
            if resp.ok:
                text = (resp.text or "").strip().strip('"')
                if text:
                    self.api_version = text if text.startswith("v") else f"v{text}"
                    return self.api_version
        except requests.RequestException:
            pass
        self.api_version = "v49"
        return self.api_version

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = urljoin(self._api_root(), path.lstrip("/"))
        session = self._session_get()
        try:
            resp = session.get(
                url,
                params=params,
                timeout=self.timeout_sec,
                verify=self.verify_ssl,
            )
            ms = getattr(resp, "elapsed", None)
            if ms is not None:
                self._last_probe_ms = ms.total_seconds() * 1000.0
            if not resp.ok:
                return {
                    "ok": False,
                    "status_code": resp.status_code,
                    "url": url,
                    "error": f"HTTP {resp.status_code}",
                    "data": _safe_json(resp),
                }
            return {
                "ok": True,
                "status_code": resp.status_code,
                "url": url,
                "error": None,
                "data": resp.json() if resp.content else {},
            }
        except requests.RequestException as exc:
            return {
                "ok": False,
                "status_code": 0,
                "url": url,
                "error": str(exc),
                "data": {},
            }

    def probe(self) -> dict[str, Any]:
        """Connectivity round-trip via ``/tools/echo``."""
        t0 = time.perf_counter()
        result = self._get("tools/echo", params={"message": "ratatoskr"})
        ms = (time.perf_counter() - t0) * 1000.0
        self._last_probe_ms = ms
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        return {
            "ok": bool(result.get("ok")),
            "base_url": self.base_url,
            "api_version": self.api_version or "auto",
            "probe_ms": round(ms, 2),
            "status_code": result.get("status_code"),
            "error": result.get("error"),
            "echo": data.get("message") if isinstance(data, dict) else None,
        }

    def get_clusters(self, *, view: str = "summary") -> list[dict[str, Any]]:
        result = self._get("clusters", params={"view": view})
        if not result.get("ok"):
            return []
        return _items(result.get("data"))

    def get_cluster(self, name: str, *, view: str = "full") -> dict[str, Any]:
        result = self._get(f"clusters/{name}", params={"view": view})
        if not result.get("ok"):
            return {"name": name, "error": result.get("error"), "status_code": result.get("status_code")}
        data = result.get("data")
        return data if isinstance(data, dict) else {"name": name}

    def get_services(self, cluster: str, *, view: str = "full") -> list[dict[str, Any]]:
        result = self._get(f"clusters/{cluster}/services", params={"view": view})
        if not result.get("ok"):
            return []
        return _items(result.get("data"))

    def get_roles(self, cluster: str, service: str, *, view: str = "full") -> list[dict[str, Any]]:
        result = self._get(
            f"clusters/{cluster}/services/{service}/roles",
            params={"view": view},
        )
        if not result.get("ok"):
            return []
        return _items(result.get("data"))

    def get_hosts(self, *, view: str = "summary") -> list[dict[str, Any]]:
        result = self._get("hosts", params={"view": view})
        if not result.get("ok"):
            return []
        return _items(result.get("data"))

    def get_events(
        self,
        *,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        max_results: int = 100,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"maxResults": max_results}
        if from_time is not None:
            params["from"] = _cm_iso(from_time)
        if to_time is not None:
            params["to"] = _cm_iso(to_time)
        result = self._get("events", params=params)
        if not result.get("ok"):
            return []
        return _items(result.get("data"))

    def get_parcels(self, cluster: str) -> list[dict[str, Any]]:
        result = self._get(f"clusters/{cluster}/parcels")
        if not result.get("ok"):
            return []
        return _items(result.get("data"))

    def get_mgmt_service(self, *, view: str = "full") -> dict[str, Any]:
        result = self._get("cm/service", params={"view": view})
        data = result.get("data")
        if not result.get("ok") or not isinstance(data, dict):
            return {"error": result.get("error"), "status_code": result.get("status_code")}
        return data

    def get_active_commands(self, *, max_results: int = 50) -> list[dict[str, Any]]:
        result = self._get("commands/commands", params={"maxResults": max_results})
        if not result.get("ok"):
            return []
        commands = _items(result.get("data"))
        return [
            c
            for c in commands
            if str(c.get("active") or "").lower() == "true"
            or str(c.get("success") or "").lower() == "false"
        ]

    def get_timeseries(
        self,
        query: str,
        *,
        from_time: datetime,
        to_time: datetime,
        desired_rollup: str = "RAW",
    ) -> dict[str, Any]:
        """Fetch CM timeseries for a query expression."""
        params = {
            "query": query,
            "from": _cm_iso(from_time),
            "to": _cm_iso(to_time),
            "desiredRollup": desired_rollup,
        }
        result = self._get("timeseries", params=params)
        if not result.get("ok"):
            return {"error": result.get("error"), "items": []}
        data = result.get("data")
        return data if isinstance(data, dict) else {"items": []}

    def discover_cluster_name(self) -> str:
        """Resolve cluster name from /clusters or host clusterRef (Knox may hide list)."""
        clusters = self.get_clusters(view="summary")
        if clusters:
            return str(clusters[0].get("name") or "")
        for host in self.get_hosts(view="summary"):
            ref = host.get("clusterRef")
            if isinstance(ref, dict) and ref.get("clusterName"):
                return str(ref["clusterName"])
        return ""

    def get_cluster_health_snapshot(self, cluster_name: str | None = None) -> dict[str, Any]:
        """Comprehensive CM health snapshot for monitoring agents."""
        poll_t0 = time.perf_counter()
        cluster = (cluster_name or self.cluster or "").strip()
        probe = self.probe()

        if not probe.get("ok"):
            return _unreachable_snapshot(probe=probe, cluster=cluster)

        if not cluster:
            cluster = self.discover_cluster_name()
            if not cluster:
                return _unreachable_snapshot(
                    probe=probe,
                    cluster="",
                    error="no clusters found (set CM_CLUSTER)",
                )
            self.cluster = cluster

        cluster_info = self.get_cluster(cluster, view="full")
        if cluster_info.get("error"):
            return _unreachable_snapshot(
                probe=probe,
                cluster=cluster,
                error=str(cluster_info.get("error")),
            )

        services = self.get_services(cluster, view="full")
        hosts = self.get_hosts(view="summary")
        from ratatoskr.cm.env import cm_event_lookback_sec

        lookback = cm_event_lookback_sec()
        now = datetime.now(timezone.utc)
        raw_events = self.get_events(
            from_time=now - timedelta(seconds=lookback), to_time=now
        ) if lookback > 0 else []

        from ratatoskr.cm.events import process_cm_events

        event_result = process_cm_events(raw_events)
        critical_events = event_result["critical_events"]
        event_warnings = event_result["event_warnings"]
        suppressed_events = int(event_result.get("suppressed_events") or 0)
        parcels = self.get_parcels(cluster)
        mgmt = self.get_mgmt_service(view="full")
        commands = self.get_active_commands()

        bad_services: list[dict[str, Any]] = []
        stopped_services: list[dict[str, Any]] = []
        stale_services: list[dict[str, Any]] = []
        failed_health_checks: list[dict[str, Any]] = []
        stopped_roles: list[dict[str, Any]] = []
        bad_hosts: list[dict[str, Any]] = []
        decommissioned_hosts: list[dict[str, Any]] = []

        for svc in services:
            name = str(svc.get("name") or "")
            health_summary = str(svc.get("healthSummary") or "")
            service_state = str(svc.get("serviceState") or svc.get("entityStatus") or "")
            staleness = str(svc.get("configStalenessStatus") or "")
            entry = {
                "cluster": cluster,
                "service": name,
                "type": svc.get("type"),
                "health_summary": health_summary,
                "service_state": service_state,
                "config_staleness": staleness,
            }
            if _health_bad(health_summary):
                bad_services.append(entry)
            if service_state.upper() in _STOPPED_STATES:
                stopped_services.append(entry)
            if staleness.upper() in _STALE_CONFIG:
                stale_services.append(entry)
            for check in svc.get("healthChecks") or []:
                if not isinstance(check, dict):
                    continue
                summary = str(check.get("summary") or check.get("name") or "")
                if summary.upper() in ("BAD", "CONCERNING", "WARNING"):
                    failed_health_checks.append(
                        {
                            "cluster": cluster,
                            "service": name,
                            "name": check.get("name"),
                            "summary": summary,
                            "explanation": check.get("explanation"),
                        }
                    )

            for role in self.get_roles(cluster, name, view="full"):
                role_name = str(role.get("name") or "")
                role_state = str(role.get("roleState") or role.get("entityStatus") or "")
                role_health = str(role.get("healthSummary") or "")
                host_ref = role.get("hostRef") if isinstance(role.get("hostRef"), dict) else {}
                host_id = host_ref.get("hostId") if isinstance(host_ref, dict) else None
                role_entry = {
                    "cluster": cluster,
                    "service": name,
                    "role": role_name,
                    "type": role.get("type"),
                    "role_state": role_state,
                    "health_summary": role_health,
                    "host_id": host_id,
                }
                if role_state.upper() in _STOPPED_STATES:
                    stopped_roles.append(role_entry)
                if _health_bad(role_health):
                    failed_health_checks.append(
                        {
                            "cluster": cluster,
                            "service": name,
                            "role": role_name,
                            "name": f"role:{role_name}",
                            "summary": role_health,
                            "explanation": f"Role {role_name} health is {role_health}",
                        }
                    )

        cluster_hosts = self._get(f"clusters/{cluster}/hosts")
        cluster_host_ids = {
            str(h.get("hostId"))
            for h in _items(cluster_hosts.get("data") if cluster_hosts.get("ok") else [])
            if h.get("hostId") is not None
        }
        for host in hosts:
            host_id = str(host.get("hostId") or "")
            if cluster_host_ids and host_id and host_id not in cluster_host_ids:
                continue
            health_summary = str(host.get("healthSummary") or "")
            commission = str(host.get("commissionState") or "")
            host_entry = {
                "host_id": host_id,
                "hostname": host.get("hostname") or host.get("ipAddress"),
                "health_summary": health_summary,
                "commission_state": commission,
            }
            if _health_bad(health_summary):
                bad_hosts.append(host_entry)
            if commission.upper() in ("DECOMMISSIONED", "DECOMMISSIONING"):
                decommissioned_hosts.append(host_entry)

        parcel_errors: list[dict[str, Any]] = []
        for parcel in parcels:
            state = parcel.get("state") if isinstance(parcel.get("state"), dict) else {}
            errors = state.get("errors") if isinstance(state, dict) else None
            stage = str(state.get("stage") or "") if isinstance(state, dict) else ""
            if errors or stage.upper() in ("DOWNLOADING", "DISTRIBUTING", "ACTIVATING"):
                status = str(state.get("status") or "")
                if errors or status.upper() == "ERROR":
                    parcel_errors.append(
                        {
                            "product": parcel.get("product"),
                            "version": parcel.get("version"),
                            "stage": stage,
                            "status": status,
                            "errors": errors,
                        }
                    )

        from ratatoskr.cm.metrics import evaluate_metric_checks, metric_checks_for_cluster

        metric_checks = metric_checks_for_cluster(cluster=cluster, services=services)

        def _fetch_timeseries(query: str, start: datetime, end: datetime) -> dict[str, Any]:
            return self.get_timeseries(query, from_time=start, to_time=end)

        metric_result = evaluate_metric_checks(
            metric_checks,
            fetch_timeseries=_fetch_timeseries,
        )
        metric_breaches = list(metric_result.get("breaches") or [])
        metric_samples = list(metric_result.get("samples") or [])
        metric_severities = list(metric_result.get("severities") or [])

        failed_commands: list[dict[str, Any]] = []
        for cmd in commands:
            success = str(cmd.get("success") or "").lower()
            active = str(cmd.get("active") or "").lower()
            if success == "false" and active != "true":
                failed_commands.append(
                    {
                        "id": cmd.get("id"),
                        "name": cmd.get("name"),
                        "result_message": cmd.get("resultMessage"),
                        "active": cmd.get("active"),
                    }
                )

        poll_ms = (time.perf_counter() - poll_t0) * 1000.0
        probe = {**probe, "poll_ms": round(poll_ms, 2)}
        if poll_ms > cm_probe_slow_ms():
            probe["slow"] = True

        severities = _derive_severities(
            cluster_info=cluster_info,
            bad_services=bad_services,
            stopped_services=stopped_services,
            stopped_roles=stopped_roles,
            failed_health_checks=failed_health_checks,
            bad_hosts=bad_hosts,
            stale_services=stale_services,
            decommissioned_hosts=decommissioned_hosts,
            critical_events=critical_events,
            event_warnings=event_warnings,
            parcel_errors=parcel_errors,
            failed_commands=failed_commands,
            mgmt=mgmt,
            probe=probe,
            metric_severities=metric_severities,
        )

        healthy = not severities
        return {
            "cluster": cluster,
            "healthy": healthy,
            "severities": severities,
            "probe": probe,
            "cluster_info": {
                "name": cluster_info.get("name"),
                "display_name": cluster_info.get("displayName"),
                "health_summary": cluster_info.get("healthSummary"),
                "version": cluster_info.get("version"),
            },
            "bad_services": bad_services,
            "stopped_services": stopped_services,
            "stale_services": stale_services,
            "stopped_roles": stopped_roles,
            "failed_health_checks": failed_health_checks,
            "bad_hosts": bad_hosts,
            "decommissioned_hosts": decommissioned_hosts,
            "critical_events": critical_events,
            "event_warnings": event_warnings,
            "suppressed_events": suppressed_events,
            "parcel_errors": parcel_errors,
            "failed_commands": failed_commands,
            "metric_breaches": metric_breaches,
            "metrics": {
                "checked_at": metric_result.get("checked_at"),
                "samples": metric_samples,
                "breaches": metric_breaches,
            },
            "mgmt": {
                "health_summary": mgmt.get("healthSummary"),
                "service_state": mgmt.get("serviceState"),
            },
            "counts": {
                "services": len(services),
                "bad_services": len(bad_services),
                "stopped_roles": len(stopped_roles),
                "bad_hosts": len(bad_hosts),
                "critical_events": len(critical_events),
                "event_warnings": len(event_warnings),
                "suppressed_events": suppressed_events,
                "failed_commands": len(failed_commands),
                "metric_breaches": len(metric_breaches),
            },
        }


def _safe_json(resp: requests.Response) -> dict[str, Any]:
    try:
        data = resp.json()
        return data if isinstance(data, dict) else {"body": data}
    except ValueError:
        return {"body": (resp.text or "")[:2000]}


def _cm_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _unreachable_snapshot(
    *,
    probe: dict[str, Any],
    cluster: str,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "cluster": cluster,
        "healthy": False,
        "severities": ["CM_UNREACHABLE"],
        "probe": {**probe, "error": error or probe.get("error")},
        "cluster_info": {},
        "bad_services": [],
        "stopped_services": [],
        "stale_services": [],
        "stopped_roles": [],
        "failed_health_checks": [],
        "bad_hosts": [],
        "decommissioned_hosts": [],
        "critical_events": [],
        "event_warnings": [],
        "suppressed_events": 0,
        "parcel_errors": [],
        "failed_commands": [],
        "metric_breaches": [],
        "metrics": {},
        "mgmt": {},
        "counts": {},
    }


def _derive_severities(
    *,
    cluster_info: dict[str, Any],
    bad_services: list[dict[str, Any]],
    stopped_services: list[dict[str, Any]],
    stopped_roles: list[dict[str, Any]],
    failed_health_checks: list[dict[str, Any]],
    bad_hosts: list[dict[str, Any]],
    stale_services: list[dict[str, Any]],
    decommissioned_hosts: list[dict[str, Any]],
    critical_events: list[dict[str, Any]],
    event_warnings: list[dict[str, Any]],
    parcel_errors: list[dict[str, Any]],
    failed_commands: list[dict[str, Any]],
    mgmt: dict[str, Any],
    probe: dict[str, Any],
    metric_severities: list[str] | None = None,
) -> list[str]:
    severities: list[str] = []
    if _health_bad(cluster_info.get("healthSummary")):
        severities.append("CLUSTER_BAD")
    if bad_services:
        severities.append("SERVICE_BAD")
    if stopped_services:
        severities.append("SERVICE_DOWN")
    if stopped_roles:
        severities.append("ROLE_DOWN")
    if failed_health_checks:
        severities.append("HEALTH_CHECK_FAIL")
    if bad_hosts:
        severities.append("HOST_BAD")
    if stale_services:
        severities.append("CONFIG_STALE")
    if decommissioned_hosts:
        severities.append("HOST_DECOMMISSIONED")
    if critical_events:
        severities.append("EVENT_CRITICAL")
    if event_warnings:
        severities.append("EVENT_WARN")
    if parcel_errors:
        severities.append("PARCEL_ERROR")
    if failed_commands:
        severities.append("COMMAND_FAILED")
    if _health_bad(mgmt.get("healthSummary")):
        severities.append("MGMT_UNHEALTHY")
    if probe.get("slow"):
        severities.append("CM_SLOW")
    for sev in metric_severities or []:
        if sev and sev not in severities:
            severities.append(sev)
    return severities
