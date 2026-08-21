"""NiFi REST client — tool names aligned with Cloudera NiFi-MCP-Server."""

from __future__ import annotations

import hashlib
import os
import socket
import time
import warnings
from dataclasses import dataclass, field
from typing import Any, Optional, Pattern
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.exceptions import InsecureRequestWarning
from urllib3.poolmanager import PoolManager

from ratatoskr.nifi.env import (
    HEAL_PHASES,
    allow_empty_queue,
    backpressure_crit_threshold,
    backpressure_warn_threshold,
    default_nifi_api_base,
    heal_phase,
    probe_slow_ms,
    watch_id_regex,
    watch_name_regex,
)

DEFAULT_API_BASE = "https://localhost:8443/nifi-api"

# Re-export for callers that import gates from client
__all__ = [
    "DEFAULT_API_BASE",
    "HEAL_PHASES",
    "NiFiClient",
    "allow_empty_queue",
    "heal_phase",
]


class _SNIHTTPSConnection(HTTPSConnection):
    """TCP to ``connect_host`` while ``host`` (localhost) is used for SNI."""

    connect_host: str | None = None

    def _new_conn(self):  # type: ignore[no-untyped-def]
        from urllib3.util import connection as urllib3_connection

        extra: dict[str, Any] = {}
        if self.source_address:
            extra["source_address"] = self.source_address
        if self.socket_options:
            extra["socket_options"] = self.socket_options
        return urllib3_connection.create_connection(
            (self.connect_host or self.host, self.port),
            self.timeout,
            **extra,
        )


class _SNIHTTPSConnectionPool(HTTPSConnectionPool):
    ConnectionCls = _SNIHTTPSConnection

    def __init__(
        self,
        *args: Any,
        connect_host: str | None = None,
        **kwargs: Any,
    ):
        self._connect_host = connect_host
        super().__init__(*args, **kwargs)

    def _new_conn(self) -> HTTPSConnection:
        conn = super()._new_conn()
        conn.connect_host = self._connect_host
        return conn


class _SNIPoolManager(PoolManager):
    def __init__(
        self,
        *args: Any,
        connect_host: str | None = None,
        **kwargs: Any,
    ):
        self._connect_host = connect_host
        super().__init__(*args, **kwargs)

    def connection_from_host(
        self,
        host: str,
        port: int | None = None,
        scheme: str = "http",
        pool_kwargs: Any = None,
    ):
        if scheme == "https" and self._connect_host:
            port = port or 443
            pool_key = ("https", host, port, self._connect_host)
            with self.pools.lock:
                pool = self.pools.get(pool_key)
                if pool is None:
                    pool = _SNIHTTPSConnectionPool(
                        host,
                        port,
                        connect_host=self._connect_host,
                        **self.connection_pool_kw,
                    )
                    self.pools[pool_key] = pool
                return pool
        return super().connection_from_host(
            host, port=port, scheme=scheme, pool_kwargs=pool_kwargs
        )


class _SNIAdapter(HTTPAdapter):
    """Connect to Docker DNS IP while presenting localhost SNI (lab NiFi cert)."""

    def __init__(self, connect_host: str, **kwargs: Any) -> None:
        self._connect_host = connect_host
        super().__init__(**kwargs)

    def init_poolmanager(
        self, connections: int, maxsize: int, block: bool = False, **pool_kwargs: Any
    ):
        self.poolmanager = _SNIPoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            connect_host=self._connect_host,
            **pool_kwargs,
        )


def _bulletin_fingerprint(
    *,
    source_id: Any,
    level: str,
    message: Any,
) -> str:
    raw = f"{source_id or ''}|{level}|{message or ''}"
    return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:16]


def _watch_keep(
    item: dict[str, Any],
    *,
    name_re: Pattern[str] | None,
    id_re: Pattern[str] | None,
) -> bool:
    """If any watch regex is set, keep items matching name OR id."""
    if name_re is None and id_re is None:
        return True
    name = str(item.get("name") or "")
    eid = str(item.get("id") or "")
    if name_re is not None and name_re.search(name):
        return True
    if id_re is not None and id_re.search(eid):
        return True
    return False


@dataclass
class NiFiClient:
    """Thin HTTP client for local Docker NiFi (single-user + bearer token)."""

    api_base: str = ""
    username: str = field(default_factory=lambda: os.environ.get("NIFI_USERNAME", "admin"))
    password: str = field(
        default_factory=lambda: os.environ.get("NIFI_PASSWORD", "RatatoskrNiFi1!")
    )
    verify_ssl: bool = field(
        default_factory=lambda: os.environ.get("NIFI_VERIFY_SSL", "false").lower()
        in ("1", "true", "yes")
    )
    timeout: float = 30.0
    session: requests.Session = field(default_factory=requests.Session, repr=False)
    # Mutation call log for tests / agent OutputEvents
    mutations: list[dict[str, Any]] = field(default_factory=list, repr=False)
    _token: str = field(default="", repr=False)
    _last_login_ms: float = field(default=0.0, repr=False)
    _last_request_ms: float = field(default=0.0, repr=False)

    def __post_init__(self) -> None:
        base = (self.api_base or default_nifi_api_base()).rstrip(
            "/"
        )
        if base.endswith("/nifi"):
            base = base[: -len("/nifi")] + "/nifi-api"
        elif not base.endswith("nifi-api"):
            base = base + "/nifi-api"
        self.api_base = base
        self.session.verify = self.verify_ssl
        self.session.headers.update({"Accept": "application/json"})
        # Local NiFi uses a self-signed cert; default NIFI_VERIFY_SSL=false.
        if not self.verify_ssl:
            warnings.filterwarnings("ignore", category=InsecureRequestWarning)

        # Flink containers use https://nifi:8443 but the lab cert only has localhost.
        # Rewrite to https://localhost + TCP to the nifi service IP (curl --resolve).
        tls_name = (os.environ.get("NIFI_TLS_SERVER_NAME") or "").strip()
        host = (urlparse(self.api_base).hostname or "").lower()
        port = urlparse(self.api_base).port or 8443
        if not tls_name and host in ("nifi", "host.docker.internal"):
            tls_name = "localhost"
        if tls_name and host and host != tls_name:
            try:
                connect_ip = socket.gethostbyname(host)
            except OSError:
                connect_ip = host
            path = urlparse(self.api_base).path or "/nifi-api"
            self.api_base = f"https://{tls_name}:{port}{path}".rstrip("/")
            self.session.headers["Host"] = f"{tls_name}:{port}"
            self.session.mount("https://", _SNIAdapter(connect_ip))

    def login(self) -> str:
        """Obtain a bearer token via POST /access/token (required for NiFi 2.x)."""
        # Token endpoint returns text/plain — do not send Accept: application/json.
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*",
        }
        # Preserve Host override from session if present
        if "Host" in self.session.headers:
            headers["Host"] = self.session.headers["Host"]
        t0 = time.perf_counter()
        try:
            resp = self.session.post(
                self._url("/access/token"),
                data={"username": self.username, "password": self.password},
                headers=headers,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            token = (resp.text or "").strip()
            if not token:
                raise RuntimeError("NiFi /access/token returned an empty token")
            self._token = token
            self.session.headers["Authorization"] = f"Bearer {token}"
            return token
        finally:
            self._last_login_ms = (time.perf_counter() - t0) * 1000.0

    def _ensure_auth(self) -> None:
        if not self._token:
            self.login()

    def _url(self, path: str) -> str:
        return urljoin(self.api_base.rstrip("/") + "/", path.lstrip("/"))

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        params: Optional[dict[str, Any]] = None,
        record_mutation: bool = False,
        mutation_name: str = "",
    ) -> Any:
        self._ensure_auth()
        t0 = time.perf_counter()
        try:
            resp = self.session.request(
                method,
                self._url(path),
                json=json_body,
                params=params,
                timeout=self.timeout,
            )
            # Token expired — refresh once
            if resp.status_code == 401 and path.rstrip("/") != "/access/token":
                self._token = ""
                self.session.headers.pop("Authorization", None)
                self.login()
                resp = self.session.request(
                    method,
                    self._url(path),
                    json=json_body,
                    params=params,
                    timeout=self.timeout,
                )
            if record_mutation:
                self.mutations.append(
                    {
                        "op": mutation_name or method,
                        "path": path,
                        "status": resp.status_code,
                    }
                )
            resp.raise_for_status()
            if resp.status_code == 204 or not resp.content:
                return None
            ctype = resp.headers.get("Content-Type", "")
            if "json" in ctype:
                return resp.json()
            return resp.text
        finally:
            self._last_request_ms = (time.perf_counter() - t0) * 1000.0

    # --- Read (MCP-aligned) ---

    def get_nifi_version(self) -> dict[str, Any]:
        data = self._request("GET", "/flow/about")
        return data.get("about", data) if isinstance(data, dict) else {"raw": data}

    def get_root_process_group(self) -> dict[str, Any]:
        data = self._request("GET", "/flow/process-groups/root")
        return data if isinstance(data, dict) else {"raw": data}

    def list_processors(self, process_group_id: str) -> list[dict[str, Any]]:
        data = self._request("GET", f"/process-groups/{process_group_id}/processors")
        if isinstance(data, dict):
            return list(data.get("processors") or [])
        return []

    def list_connections(self, process_group_id: str) -> list[dict[str, Any]]:
        data = self._request("GET", f"/process-groups/{process_group_id}/connections")
        if isinstance(data, dict):
            return list(data.get("connections") or [])
        return []

    def get_bulletins(self, after_ms: Optional[int] = None) -> list[dict[str, Any]]:
        params = {}
        if after_ms is not None:
            params["after"] = after_ms
        data = self._request("GET", "/flow/bulletin-board", params=params or None)
        if isinstance(data, dict):
            board = data.get("bulletinBoard") or data
            return list(board.get("bulletins") or [])
        return []

    def get_processor_details(self, processor_id: str) -> dict[str, Any]:
        data = self._request("GET", f"/processors/{processor_id}")
        return data if isinstance(data, dict) else {"raw": data}

    def get_processor_state(self, processor_id: str) -> dict[str, Any]:
        details = self.get_processor_details(processor_id)
        entity = details.get("component") or details.get("processor") or details
        status = details.get("status") or {}
        return {
            "id": entity.get("id") if isinstance(entity, dict) else processor_id,
            "name": entity.get("name") if isinstance(entity, dict) else None,
            "state": entity.get("state") if isinstance(entity, dict) else None,
            "validationStatus": entity.get("validationStatus")
            if isinstance(entity, dict)
            else None,
            "runStatus": status.get("runStatus") if isinstance(status, dict) else None,
            "revision": details.get("revision"),
        }

    def check_connection_queue(self, connection_id: str) -> dict[str, Any]:
        data = self._request("GET", f"/connections/{connection_id}")
        status = (data or {}).get("status") or {}
        agg = status.get("aggregateSnapshot") or status
        return {
            "id": connection_id,
            "flowFilesQueued": int(agg.get("flowFilesQueued") or 0),
            "bytesQueued": int(agg.get("bytesQueued") or 0),
            "queued": agg.get("queued"),
            "revision": (data or {}).get("revision"),
        }

    def get_controller_services(
        self, process_group_id: Optional[str] = None
    ) -> list[dict[str, Any]]:
        if process_group_id:
            data = self._request(
                "GET", f"/flow/process-groups/{process_group_id}/controller-services"
            )
        else:
            data = self._request("GET", "/flow/controller/controller-services")
        if isinstance(data, dict):
            return list(
                data.get("controllerServices")
                or data.get("controllerServiceEntities")
                or []
            )
        return []

    def get_controller_service_details(self, service_id: str) -> dict[str, Any]:
        data = self._request("GET", f"/controller-services/{service_id}")
        return data if isinstance(data, dict) else {"raw": data}

    def list_process_groups(self, process_group_id: str) -> list[dict[str, Any]]:
        data = self._request("GET", f"/process-groups/{process_group_id}/process-groups")
        if isinstance(data, dict):
            return list(data.get("processGroups") or [])
        return []

    def get_flow_summary(self, process_group_id: str) -> dict[str, Any]:
        data = self._request("GET", f"/flow/process-groups/{process_group_id}")
        return data if isinstance(data, dict) else {"raw": data}

    def get_flow_health_status(
        self, process_group_id: str = "root", *, recursive: bool = True
    ) -> dict[str, Any]:
        """Comprehensive health snapshot for monitoring agents.

        When ``recursive`` is true (default), also inspects child process groups
        so sample flows under nested PGs are visible from ``root``.

        Adds probe timings, DISABLED_SERVICE, graded backpressure, and optional
        watchlist filtering (``NIFI_WATCH_NAME_REGEX`` / ``NIFI_WATCH_ID_REGEX``).
        """
        poll_t0 = time.perf_counter()
        name_re = watch_name_regex()
        id_re = watch_id_regex()
        bp_warn = backpressure_warn_threshold()
        bp_crit = backpressure_crit_threshold()

        if process_group_id == "root":
            root = self.get_root_process_group()
            flow = root.get("processGroupFlow") or {}
            process_group_id = flow.get("id") or "root"

        pg_ids = [process_group_id]
        if recursive and process_group_id:
            try:
                for ent in self.list_process_groups(process_group_id):
                    child_id = (ent.get("component") or {}).get("id") or ent.get("id")
                    if child_id:
                        pg_ids.append(child_id)
            except Exception:  # noqa: BLE001 — keep root-only on list failure
                pass

        processors: list[dict[str, Any]] = []
        connections: list[dict[str, Any]] = []
        services: list[dict[str, Any]] = []
        for pg_id in pg_ids:
            processors.extend(self.list_processors(pg_id))
            connections.extend(self.list_connections(pg_id))
            try:
                services.extend(self.get_controller_services(pg_id))
            except Exception:  # noqa: BLE001
                pass
        bulletins = self.get_bulletins()

        stopped: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []
        for ent in processors:
            comp = ent.get("component") or {}
            state = comp.get("state") or ""
            validation = comp.get("validationStatus") or ""
            item = {
                "id": comp.get("id") or ent.get("id"),
                "name": comp.get("name"),
                "state": state,
                "validationStatus": validation,
                "revision": ent.get("revision"),
            }
            if not _watch_keep(item, name_re=name_re, id_re=id_re):
                continue
            if state == "STOPPED":
                stopped.append(item)
            if validation and validation.upper() == "INVALID":
                invalid.append(item)

        disabled_services: list[dict[str, Any]] = []
        for ent in services:
            comp = ent.get("component") or {}
            state = comp.get("state") or ""
            if state != "DISABLED":
                continue
            item = {
                "id": comp.get("id") or ent.get("id"),
                "name": comp.get("name"),
                "state": state,
                "revision": ent.get("revision"),
            }
            if not _watch_keep(item, name_re=name_re, id_re=id_re):
                continue
            disabled_services.append(item)

        backpressured: list[dict[str, Any]] = []
        for ent in connections:
            status = ent.get("status") or {}
            agg = status.get("aggregateSnapshot") or status
            queued = int(agg.get("flowFilesQueued") or 0)
            if queued < bp_warn:
                continue
            comp = ent.get("component") or {}
            source = comp.get("source") or {}
            level = "crit" if queued >= bp_crit else "warn"
            item = {
                "id": comp.get("id") or ent.get("id"),
                "name": comp.get("name"),
                "flowFilesQueued": queued,
                "bytesQueued": int(agg.get("bytesQueued") or 0),
                "backpressure_level": level,
                "sourceId": source.get("id"),
                "sourceName": source.get("name"),
                "revision": ent.get("revision"),
            }
            if not _watch_keep(item, name_re=name_re, id_re=id_re):
                continue
            backpressured.append(item)

        # Index current processor problems so stale bulletins don't keep the flow "unhealthy".
        problem_ids = {
            *(p["id"] for p in stopped if p.get("id")),
            *(p["id"] for p in invalid if p.get("id")),
        }
        proc_by_id = {}
        for ent in processors:
            comp = ent.get("component") or {}
            pid = comp.get("id") or ent.get("id")
            if pid:
                proc_by_id[pid] = {
                    "state": comp.get("state"),
                    "validationStatus": comp.get("validationStatus"),
                }

        active_error_bulletins = []
        stale_bulletins = []
        for b in bulletins:
            bulletin = b.get("bulletin") or b
            level = (bulletin.get("level") or "").upper()
            if level not in ("ERROR", "WARNING"):
                continue
            source_id = bulletin.get("sourceId")
            message = bulletin.get("message")
            entry = {
                "level": level,
                "message": message,
                "sourceName": bulletin.get("sourceName"),
                "sourceId": source_id,
                "fingerprint": _bulletin_fingerprint(
                    source_id=source_id, level=level, message=message
                ),
            }
            if name_re is not None or id_re is not None:
                if not _watch_keep(
                    {"id": source_id, "name": bulletin.get("sourceName")},
                    name_re=name_re,
                    id_re=id_re,
                ):
                    continue
            if source_id and source_id in problem_ids:
                active_error_bulletins.append(entry)
            elif source_id and source_id in proc_by_id:
                stale_bulletins.append(entry)
            elif level == "ERROR" and not source_id:
                active_error_bulletins.append(entry)
            else:
                stale_bulletins.append(entry)

        poll_ms = (time.perf_counter() - poll_t0) * 1000.0
        probe = {
            "ok": True,
            "login_ms": round(self._last_login_ms, 2),
            "poll_ms": round(poll_ms, 2),
            "last_request_ms": round(self._last_request_ms, 2),
        }

        severities: list[str] = []
        if stopped:
            severities.append("STOPPED")
        if invalid:
            severities.append("INVALID")
        if disabled_services:
            severities.append("DISABLED_SERVICE")
        warn_queues = [c for c in backpressured if c.get("backpressure_level") == "warn"]
        crit_queues = [c for c in backpressured if c.get("backpressure_level") == "crit"]
        if warn_queues:
            severities.append("BACKPRESSURE_WARN")
        if crit_queues:
            severities.append("BACKPRESSURE_CRIT")
        if backpressured:
            severities.append("BACKPRESSURE")
        if active_error_bulletins:
            severities.append("BULLETIN_ERROR")
        if poll_ms >= probe_slow_ms():
            severities.append("NIFI_SLOW")

        return {
            "process_group_id": process_group_id,
            "healthy": not severities,
            "severities": severities,
            "stopped_processors": stopped,
            "invalid_processors": invalid,
            "disabled_controller_services": disabled_services,
            "queued_connections": backpressured,
            "bulletins": active_error_bulletins,
            "stale_bulletins": stale_bulletins,
            "probe": probe,
            "counts": {
                "processors": len(processors),
                "connections": len(connections),
                "controller_services": len(services),
            },
        }

    # --- Write (MCP-aligned; revision-aware) ---

    def start_processor(self, processor_id: str, version: Optional[int] = None) -> dict[str, Any]:
        details = self.get_processor_details(processor_id)
        revision = details.get("revision") or {}
        if version is not None:
            revision = {**revision, "version": version}
        body = {
            "revision": revision,
            "state": "RUNNING",
            "disconnectedNodeAcknowledged": True,
        }
        return self._request(
            "PUT",
            f"/processors/{processor_id}/run-status",
            json_body=body,
            record_mutation=True,
            mutation_name="start_processor",
        ) or {"ok": True, "id": processor_id, "state": "RUNNING"}

    def stop_processor(self, processor_id: str, version: Optional[int] = None) -> dict[str, Any]:
        details = self.get_processor_details(processor_id)
        revision = details.get("revision") or {}
        if version is not None:
            revision = {**revision, "version": version}
        body = {
            "revision": revision,
            "state": "STOPPED",
            "disconnectedNodeAcknowledged": True,
        }
        return self._request(
            "PUT",
            f"/processors/{processor_id}/run-status",
            json_body=body,
            record_mutation=True,
            mutation_name="stop_processor",
        ) or {"ok": True, "id": processor_id, "state": "STOPPED"}

    def enable_controller_service(
        self, service_id: str, version: Optional[int] = None
    ) -> dict[str, Any]:
        details = self.get_controller_service_details(service_id)
        revision = details.get("revision") or {}
        if version is not None:
            revision = {**revision, "version": version}
        body = {
            "revision": revision,
            "state": "ENABLED",
            "disconnectedNodeAcknowledged": True,
        }
        return self._request(
            "PUT",
            f"/controller-services/{service_id}/run-status",
            json_body=body,
            record_mutation=True,
            mutation_name="enable_controller_service",
        ) or {"ok": True, "id": service_id, "state": "ENABLED"}

    def disable_controller_service(
        self, service_id: str, version: Optional[int] = None
    ) -> dict[str, Any]:
        details = self.get_controller_service_details(service_id)
        revision = details.get("revision") or {}
        if version is not None:
            revision = {**revision, "version": version}
        body = {
            "revision": revision,
            "state": "DISABLED",
            "disconnectedNodeAcknowledged": True,
        }
        return self._request(
            "PUT",
            f"/controller-services/{service_id}/run-status",
            json_body=body,
            record_mutation=True,
            mutation_name="disable_controller_service",
        ) or {"ok": True, "id": service_id, "state": "DISABLED"}

    def create_controller_service(
        self,
        process_group_id: str,
        service_type: str,
        name: str,
        *,
        properties: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        body = {
            "revision": {"version": 0},
            "component": {
                "type": service_type,
                "name": name,
                "properties": properties or {},
            },
        }
        return self._request(
            "POST",
            f"/process-groups/{process_group_id}/controller-services",
            json_body=body,
            record_mutation=True,
            mutation_name="create_controller_service",
        ) or {}

    def update_controller_service_properties(
        self,
        service_id: str,
        properties: dict[str, Any],
    ) -> dict[str, Any]:
        """Update CS properties (service should be DISABLED)."""
        details = self.get_controller_service_details(service_id)
        revision = details.get("revision") or {"version": 0}
        component = dict(details.get("component") or {})
        merged = dict(component.get("properties") or {})
        merged.update(properties)
        body = {
            "revision": revision,
            "component": {
                "id": service_id,
                "name": component.get("name"),
                "properties": merged,
            },
            "disconnectedNodeAcknowledged": True,
        }
        return self._request(
            "PUT",
            f"/controller-services/{service_id}",
            json_body=body,
            record_mutation=True,
            mutation_name="update_controller_service",
        ) or {}

    def terminate_processor(
        self, processor_id: str, version: Optional[int] = None
    ) -> dict[str, Any]:
        details = self.get_processor_details(processor_id)
        revision = details.get("revision") or {}
        if version is not None:
            revision = {**revision, "version": version}
        # NiFi terminate threads endpoint
        return self._request(
            "DELETE",
            f"/processors/{processor_id}/threads",
            record_mutation=True,
            mutation_name="terminate_processor",
        ) or {"ok": True, "id": processor_id, "terminated": True}

    def restart_processor(
        self, processor_id: str, version: Optional[int] = None
    ) -> dict[str, Any]:
        """Stop then start a processor (revision-aware, retries 409 races)."""
        details = self.get_processor_details(processor_id)
        state = (details.get("component") or {}).get("state") or ""
        if state != "STOPPED":
            try:
                self.stop_processor(processor_id, version)
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.5)
        last_err: Exception | None = None
        for _ in range(6):
            try:
                self.start_processor(processor_id)
                return {"ok": True, "id": processor_id, "state": "RUNNING", "restarted": True}
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                time.sleep(0.5)
        raise RuntimeError(f"restart_processor failed for {processor_id}: {last_err}")

    def fix_processor_config(
        self,
        processor_id: str,
        *,
        auto_terminated_relationships: Optional[list[str]] = None,
        properties: Optional[dict[str, str]] = None,
        then_start: bool = True,
    ) -> dict[str, Any]:
        """Apply a narrow config patch (lab templates) then optionally start."""
        details = self.get_processor_details(processor_id)
        state = (details.get("component") or {}).get("state") or ""
        if state not in ("STOPPED", "DISABLED"):
            try:
                self.stop_processor(processor_id)
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.4)
        self.update_processor_config(
            processor_id,
            properties=properties,
            auto_terminated_relationships=auto_terminated_relationships,
        )
        time.sleep(0.4)
        started = False
        if then_start:
            last_err: Exception | None = None
            for _ in range(6):
                try:
                    self.start_processor(processor_id)
                    started = True
                    last_err = None
                    break
                except Exception as exc:  # noqa: BLE001
                    last_err = exc
                    time.sleep(0.5)
            if not started and last_err is not None:
                raise RuntimeError(
                    f"fix_processor_config start failed for {processor_id}: {last_err}"
                ) from last_err
        return {
            "ok": True,
            "id": processor_id,
            "started": started,
            "auto_terminated_relationships": auto_terminated_relationships,
            "properties": properties or {},
        }

    def empty_connection_queue(self, connection_id: str) -> dict[str, Any]:
        """Drop all flowfiles on a connection. Destructive — lab only."""
        return self._request(
            "POST",
            f"/flowfile-queues/{connection_id}/drop-requests",
            record_mutation=True,
            mutation_name="empty_connection_queue",
        ) or {"ok": True, "id": connection_id, "emptied": True}

    # --- Flow bootstrap helpers ---

    def create_process_group(
        self, parent_id: str, name: str, x: float = 0.0, y: float = 0.0
    ) -> dict[str, Any]:
        parent = self._request("GET", f"/process-groups/{parent_id}")
        revision = (parent or {}).get("revision") or {"version": 0}
        body = {
            "revision": revision,
            "component": {
                "name": name,
                "position": {"x": x, "y": y},
            },
        }
        return self._request(
            "POST",
            f"/process-groups/{parent_id}/process-groups",
            json_body=body,
            record_mutation=True,
            mutation_name="create_process_group",
        ) or {}

    def create_processor(
        self,
        process_group_id: str,
        processor_type: str,
        name: str,
        x: float = 0.0,
        y: float = 0.0,
        properties: Optional[dict[str, str]] = None,
        auto_terminated_relationships: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        config: dict[str, Any] = {"properties": properties or {}}
        if auto_terminated_relationships:
            config["autoTerminatedRelationships"] = list(auto_terminated_relationships)
        body = {
            "revision": {"version": 0},
            "component": {
                "type": processor_type,
                "name": name,
                "position": {"x": x, "y": y},
                "config": config,
            },
        }
        return self._request(
            "POST",
            f"/process-groups/{process_group_id}/processors",
            json_body=body,
            record_mutation=True,
            mutation_name="create_processor",
        ) or {}

    def update_processor_config(
        self,
        processor_id: str,
        *,
        properties: Optional[dict[str, str]] = None,
        auto_terminated_relationships: Optional[list[str]] = None,
        scheduling_period: Optional[str] = None,
    ) -> dict[str, Any]:
        """Update processor config (revision-aware). Processor should be STOPPED."""
        details = self.get_processor_details(processor_id)
        revision = details.get("revision") or {"version": 0}
        component = dict(details.get("component") or {})
        config = dict(component.get("config") or {})
        if properties is not None:
            merged = dict(config.get("properties") or {})
            merged.update(properties)
            config["properties"] = merged
        if auto_terminated_relationships is not None:
            config["autoTerminatedRelationships"] = list(auto_terminated_relationships)
        if scheduling_period is not None:
            config["schedulingPeriod"] = scheduling_period
        component["config"] = config
        body = {
            "revision": revision,
            "component": component,
            "disconnectedNodeAcknowledged": True,
        }
        return self._request(
            "PUT",
            f"/processors/{processor_id}",
            json_body=body,
            record_mutation=True,
            mutation_name="update_processor_config",
        ) or {}

    def create_connection(
        self,
        process_group_id: str,
        source_id: str,
        source_type: str,
        destination_id: str,
        destination_type: str,
        relationships: list[str],
        name: str = "",
    ) -> dict[str, Any]:
        body = {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "source": {
                    "id": source_id,
                    "type": source_type,
                    "groupId": process_group_id,
                },
                "destination": {
                    "id": destination_id,
                    "type": destination_type,
                    "groupId": process_group_id,
                },
                "selectedRelationships": relationships,
            },
        }
        return self._request(
            "POST",
            f"/process-groups/{process_group_id}/connections",
            json_body=body,
            record_mutation=True,
            mutation_name="create_connection",
        ) or {}
