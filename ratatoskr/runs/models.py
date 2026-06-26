"""Run and span data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

RunKind = Literal["local", "cluster"]
RunStatus = Literal["starting", "running", "finished", "failed", "canceled"]
SpanKind = Literal["action", "tool", "output", "agent", "sink"]
SpanStatus = Literal["ok", "error"]


@dataclass
class Span:
    id: str
    run_id: str
    kind: SpanKind
    name: str
    status: SpanStatus
    started_at: str
    parent_id: str | None = None
    finished_at: str | None = None
    duration_ms: int | None = None
    input: Any | None = None
    output: Any | None = None


@dataclass
class Run:
    id: str
    agent: str
    kind: RunKind
    status: RunStatus
    started_at: str
    finished_at: str | None = None
    flink_job_id: str | None = None
    error: str | None = None
    record_count: int = 0
    spans: list[Span] = field(default_factory=list)
    plan: list[dict[str, Any]] = field(default_factory=list)
