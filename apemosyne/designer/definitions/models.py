"""Agent definition graph data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

AgentDefinitionType = Literal["workflow", "react"]
AgentDefinitionStatus = Literal["draft", "compiled", "published"]
AgentNodeKind = Literal["input_event", "action", "tool", "output_event", "prompt", "llm_call"]
AgentEdgeKind = Literal["listens_to", "calls", "emits"]


@dataclass
class AgentDefinitionNode:
    id: str
    kind: AgentNodeKind
    name: str = ""
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentDefinitionEdge:
    id: str
    source: str
    target: str
    kind: AgentEdgeKind


@dataclass
class AgentDefinition:
    id: str
    name: str
    type: AgentDefinitionType
    version: int
    description: str
    status: AgentDefinitionStatus
    nodes: list[AgentDefinitionNode] = field(default_factory=list)
    edges: list[AgentDefinitionEdge] = field(default_factory=list)
    layout: dict[str, dict[str, float]] = field(default_factory=dict)
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] = field(default_factory=dict)
    manifest_name: str | None = None
    catalog_category_id: str | None = None
    catalog_subcategory_id: str | None = None
    catalog_tags: list[str] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""


def agent_definition_from_dict(data: dict[str, Any]) -> AgentDefinition:
    return AgentDefinition(
        id=data["id"],
        name=data["name"],
        type=data["type"],
        version=int(data.get("version") or 1),
        description=str(data.get("description") or ""),
        status=data.get("status") or "draft",
        nodes=[
            AgentDefinitionNode(
                id=n["id"],
                kind=n["kind"],
                name=str(n.get("name") or ""),
                config=n.get("config") or {},
            )
            for n in data.get("nodes") or []
        ],
        edges=[
            AgentDefinitionEdge(
                id=e["id"],
                source=e["source"],
                target=e["target"],
                kind=e["kind"],
            )
            for e in data.get("edges") or []
        ],
        layout=data.get("layout") or {},
        input_schema=data.get("input_schema") or {},
        output_schema=data.get("output_schema") or {},
        manifest_name=data.get("manifest_name"),
        catalog_category_id=data.get("catalog_category_id"),
        catalog_subcategory_id=data.get("catalog_subcategory_id"),
        catalog_tags=list(data.get("catalog_tags") or []),
        created_at=data.get("created_at") or "",
        updated_at=data.get("updated_at") or "",
    )
