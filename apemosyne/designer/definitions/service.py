"""High-level agent definition CRUD."""

from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apemosyne.designer.definitions.models import (
    AgentDefinition,
    AgentDefinitionEdge,
    AgentDefinitionNode,
)
from apemosyne.designer.definitions.seed import DOUBLE_VALUE_ID, double_value_definition_payload
from apemosyne.designer.definitions.store import (
    AgentDefinitionStore,
    agent_definitions_store,
)
from apemosyne.designer.definitions.validate import validate_agent_definition

_default_service: "AgentDefinitionService | None" = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _definition_to_dict(definition: AgentDefinition) -> dict[str, Any]:
    return {
        "id": definition.id,
        "name": definition.name,
        "type": definition.type,
        "version": definition.version,
        "description": definition.description,
        "status": definition.status,
        "nodes": [asdict(n) for n in definition.nodes],
        "edges": [asdict(e) for e in definition.edges],
        "layout": definition.layout,
        "input_schema": definition.input_schema,
        "output_schema": definition.output_schema,
        "manifest_name": definition.manifest_name,
        "catalog_category_id": definition.catalog_category_id,
        "catalog_subcategory_id": definition.catalog_subcategory_id,
        "catalog_tags": list(definition.catalog_tags),
        "created_at": definition.created_at,
        "updated_at": definition.updated_at,
    }


def _parse_nodes(raw: list[dict[str, Any]]) -> list[AgentDefinitionNode]:
    return [
        AgentDefinitionNode(
            id=n["id"],
            kind=n["kind"],
            name=str(n.get("name") or ""),
            config=n.get("config") or {},
        )
        for n in raw
    ]


def _parse_edges(raw: list[dict[str, Any]]) -> list[AgentDefinitionEdge]:
    return [
        AgentDefinitionEdge(
            id=e["id"],
            source=e["source"],
            target=e["target"],
            kind=e["kind"],
        )
        for e in raw
    ]


class AgentDefinitionService:
    def __init__(self, store: AgentDefinitionStore) -> None:
        self._store = store

    def store_count(self) -> int:
        return self._store.count()

    def create(
        self,
        name: str,
        *,
        agent_type: str = "workflow",
        description: str = "",
        nodes: list[dict[str, Any]] | None = None,
        edges: list[dict[str, Any]] | None = None,
        layout: dict[str, dict[str, float]] | None = None,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        manifest_name: str | None = None,
        catalog_category_id: str | None = None,
        catalog_subcategory_id: str | None = None,
        catalog_tags: list[str] | None = None,
    ) -> dict[str, Any]:
        definition_id = f"def_{uuid.uuid4().hex[:12]}"
        now = _utc_now()
        self._store.insert(
            definition_id=definition_id,
            name=name,
            agent_type=agent_type,
            version=1,
            description=description,
            status="draft",
            nodes=nodes or [],
            edges=edges or [],
            layout=layout or {},
            input_schema=input_schema or {},
            output_schema=output_schema or {},
            manifest_name=manifest_name,
            catalog={
                "category_id": catalog_category_id,
                "subcategory_id": catalog_subcategory_id,
                "tags": catalog_tags or [],
            },
            created_at=now,
            updated_at=now,
        )
        return self.get(definition_id)

    def create_from_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        definition_id = str(payload.get("id") or f"def_{uuid.uuid4().hex[:12]}")
        now = _utc_now()
        self._store.insert(
            definition_id=definition_id,
            name=str(payload["name"]),
            agent_type=str(payload.get("type") or "workflow"),
            version=int(payload.get("version") or 1),
            description=str(payload.get("description") or ""),
            status=str(payload.get("status") or "draft"),
            nodes=payload.get("nodes") or [],
            edges=payload.get("edges") or [],
            layout=payload.get("layout") or {},
            input_schema=payload.get("input_schema") or {},
            output_schema=payload.get("output_schema") or {},
            manifest_name=payload.get("manifest_name"),
            catalog={
                "category_id": payload.get("catalog_category_id"),
                "subcategory_id": payload.get("catalog_subcategory_id"),
                "tags": list(payload.get("catalog_tags") or []),
            },
            created_at=now,
            updated_at=now,
        )
        return self.get(definition_id)

    def get(self, definition_id: str) -> dict[str, Any]:
        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)
        return _definition_to_dict(definition)

    def list_definitions(self, *, limit: int = 100) -> list[dict[str, Any]]:
        self._ensure_seeds()
        return [
            _definition_to_dict(d) for d in self._store.list_definitions(limit=limit)
        ]

    def _ensure_seeds(self) -> None:
        if self._store.count() == 0:
            self.create_from_payload(double_value_definition_payload())

    def seed_double_value(self) -> dict[str, Any]:
        try:
            return self.get(DOUBLE_VALUE_ID)
        except KeyError:
            return self.create_from_payload(double_value_definition_payload())

    def update(self, definition_id: str, body: dict[str, Any]) -> dict[str, Any]:
        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)

        nodes = body.get("nodes")
        edges = body.get("edges")
        catalog = {
            "category_id": definition.catalog_category_id,
            "subcategory_id": definition.catalog_subcategory_id,
            "tags": list(definition.catalog_tags),
        }
        if "catalog_category_id" in body:
            catalog["category_id"] = body["catalog_category_id"]
        if "catalog_subcategory_id" in body:
            catalog["subcategory_id"] = body["catalog_subcategory_id"]
        if "catalog_tags" in body:
            catalog["tags"] = list(body["catalog_tags"] or [])

        updated = self._store.update(
            definition_id,
            name=body.get("name"),
            agent_type=body.get("type"),
            version=body.get("version"),
            description=body.get("description"),
            status=body.get("status"),
            nodes=nodes,
            edges=edges,
            layout=body.get("layout"),
            input_schema=body.get("input_schema"),
            output_schema=body.get("output_schema"),
            manifest_name=body.get("manifest_name"),
            catalog=catalog,
            updated_at=_utc_now(),
        )
        if not updated:
            raise KeyError(definition_id)
        return self.get(definition_id)

    def delete(self, definition_id: str) -> None:
        if not self._store.delete(definition_id):
            raise KeyError(definition_id)

    def validate(self, definition_id: str) -> dict[str, Any]:
        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)
        return validate_agent_definition(definition)

    def validate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        from apemosyne.designer.definitions.models import agent_definition_from_dict

        definition = agent_definition_from_dict(payload)
        return validate_agent_definition(definition)

    def compile(self, definition_id: str, *, root: Path | None = None) -> dict[str, Any]:
        from apemosyne.designer.definitions.compile import (
            CompileError,
            compile_agent_definition,
            compile_result_to_dict,
        )

        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)
        try:
            result = compile_agent_definition(definition, root=root, write_files=True)
        except CompileError as exc:
            raise ValueError(str(exc)) from exc

        self._store.update(
            definition_id,
            status="compiled",
            updated_at=_utc_now(),
        )
        payload = compile_result_to_dict(result)
        payload["definition"] = self.get(definition_id)
        return payload

    def publish(self, definition_id: str, *, root: Path | None = None) -> dict[str, Any]:
        from apemosyne.designer.definitions.publish import (
            PublishError,
            publish_agent_definition,
            publish_result_to_dict,
        )

        definition = self._store.get(definition_id)
        if definition is None:
            raise KeyError(definition_id)
        try:
            result = publish_agent_definition(definition, root=root, compile_first=True)
        except PublishError as exc:
            raise ValueError(str(exc)) from exc

        self._store.update(
            definition_id,
            status="published",
            manifest_name=result.manifest_name,
            updated_at=_utc_now(),
        )
        payload = publish_result_to_dict(result)
        payload["definition"] = self.get(definition_id)
        return payload


def reset_agent_definition_service_for_tests() -> None:
    global _default_service
    _default_service = None


def default_agent_definition_service(root: Path | None = None) -> AgentDefinitionService:
    global _default_service
    if _default_service is None or root is not None:
        _default_service = AgentDefinitionService(agent_definitions_store(root))
    return _default_service
