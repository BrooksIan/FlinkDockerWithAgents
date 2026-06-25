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
