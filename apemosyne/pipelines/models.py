"""Pipeline graph data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

NodeKind = Literal["source", "agent", "sink"]


@dataclass
class PipelineNode:
    id: str
    kind: NodeKind
    agent: str | None = None
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineEdge:
    id: str
    source: str
    target: str
    mapping: dict[str, str] = field(default_factory=dict)


@dataclass
class Pipeline:
    id: str
    name: str
    nodes: list[PipelineNode] = field(default_factory=list)
    edges: list[PipelineEdge] = field(default_factory=list)
    layout: dict[str, dict[str, float]] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


@dataclass
class AgentStepResult:
    agent: str
    duration_ms: int
    input_data: Any
    output_data: Any


def pipeline_from_dict(data: dict[str, Any]) -> Pipeline:
    return Pipeline(
        id=data["id"],
        name=data["name"],
        nodes=[
            PipelineNode(
                id=n["id"],
                kind=n["kind"],
                agent=n.get("agent"),
                config=n.get("config") or {},
            )
            for n in data.get("nodes") or []
        ],
        edges=[
            PipelineEdge(
                id=e["id"],
                source=e["source"],
                target=e["target"],
                mapping=e.get("mapping") or {},
            )
            for e in data.get("edges") or []
        ],
        layout=data.get("layout") or {},
        created_at=data.get("created_at") or "",
        updated_at=data.get("updated_at") or "",
    )
