"""NiFi REST client — tool names aligned with Cloudera NiFi-MCP-Server."""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from urllib3.exceptions import InsecureRequestWarning


DEFAULT_API_BASE = "https://localhost:8443/nifi-api"
HEAL_PHASES = frozenset({"monitor", "safe", "lab"})


def heal_phase() -> str:
    raw = (os.environ.get("NIFI_HEAL_PHASE") or "monitor").strip().lower()
    return raw if raw in HEAL_PHASES else "monitor"


def allow_empty_queue() -> bool:
    return os.environ.get("NIFI_HEAL_ALLOW_EMPTY_QUEUE", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


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

    def __post_init__(self) -> None:
        base = (self.api_base or os.environ.get("NIFI_API_BASE") or DEFAULT_API_BASE).rstrip(
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

    def login(self) -> str:
        """Obtain a bearer token via POST /access/token (required for NiFi 2.x)."""
        # Token endpoint returns text/plain — do not send Accept: application/json.
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*",
        }
        resp = requests.post(
            self._url("/access/token"),
            data={"username": self.username, "password": self.password},
            headers=headers,
            verify=self.verify_ssl,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        token = (resp.text or "").strip()
        if not token:
            raise RuntimeError("NiFi /access/token returned an empty token")
        self._token = token
        self.session.headers["Authorization"] = f"Bearer {token}"
        return token

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
        """
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
            if state == "STOPPED":
                stopped.append(item)
            if validation and validation.upper() == "INVALID":
                invalid.append(item)

        disabled_services: list[dict[str, Any]] = []
        for ent in services:
            comp = ent.get("component") or {}
            state = comp.get("state") or ""
            if state == "DISABLED":
                disabled_services.append(
                    {
                        "id": comp.get("id") or ent.get("id"),
                        "name": comp.get("name"),
                        "state": state,
                        "revision": ent.get("revision"),
                    }
                )

        backpressured: list[dict[str, Any]] = []
        for ent in connections:
            status = ent.get("status") or {}
            agg = status.get("aggregateSnapshot") or status
            queued = int(agg.get("flowFilesQueued") or 0)
            if queued > 0:
                backpressured.append(
                    {
                        "id": (ent.get("component") or {}).get("id") or ent.get("id"),
                        "name": (ent.get("component") or {}).get("name"),
                        "flowFilesQueued": queued,
                        "bytesQueued": int(agg.get("bytesQueued") or 0),
                        "revision": ent.get("revision"),
                    }
                )

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
            entry = {
                "level": level,
                "message": bulletin.get("message"),
                "sourceName": bulletin.get("sourceName"),
                "sourceId": bulletin.get("sourceId"),
            }
            source_id = bulletin.get("sourceId")
            if source_id and source_id in problem_ids:
                active_error_bulletins.append(entry)
            elif source_id and source_id in proc_by_id:
                # Source exists but is healthy now — bulletin is residual on the board.
                stale_bulletins.append(entry)
            elif level == "ERROR" and not source_id:
                active_error_bulletins.append(entry)
            else:
                stale_bulletins.append(entry)

        severities: list[str] = []
        if stopped:
            severities.append("STOPPED")
        if invalid:
            severities.append("INVALID")
        if backpressured:
            severities.append("BACKPRESSURE")
        if active_error_bulletins:
            severities.append("BULLETIN_ERROR")

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
