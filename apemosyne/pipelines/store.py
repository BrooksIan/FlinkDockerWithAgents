"""SQLite persistence for composed pipelines."""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from apemosyne.paths import project_root
from apemosyne.pipelines.models import Pipeline, PipelineEdge, PipelineNode

_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    graph_json TEXT NOT NULL,
    layout_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pipelines_updated ON pipelines(updated_at DESC);
"""


def pipelines_db_path(root: Path | None = None) -> Path:
    env = os.environ.get("APEMOSYNE_PIPELINES_DB", "").strip()
    if env:
        return Path(env)
    repo = root or project_root()
    directory = repo / ".apemosyne"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / "pipelines.db"


class PipelineStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def insert(
        self,
        *,
        pipeline_id: str,
        name: str,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        layout: dict[str, dict[str, float]],
        created_at: str,
        updated_at: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO pipelines (id, name, graph_json, layout_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    pipeline_id,
                    name,
                    json.dumps({"nodes": nodes, "edges": edges}),
                    json.dumps(layout),
                    created_at,
                    updated_at,
                ),
            )

    def update(
        self,
        pipeline_id: str,
        *,
        name: str | None = None,
        nodes: list[dict[str, Any]] | None = None,
        edges: list[dict[str, Any]] | None = None,
        layout: dict[str, dict[str, float]] | None = None,
        updated_at: str,
    ) -> bool:
        row = self._fetch_row(pipeline_id)
        if row is None:
            return False
        current = json.loads(row["graph_json"])
        current_layout = json.loads(row["layout_json"])
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE pipelines
                SET name = ?, graph_json = ?, layout_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    name if name is not None else row["name"],
                    json.dumps(
                        {
                            "nodes": nodes if nodes is not None else current["nodes"],
                            "edges": edges if edges is not None else current["edges"],
                        }
                    ),
                    json.dumps(layout if layout is not None else current_layout),
                    updated_at,
                    pipeline_id,
                ),
            )
        return True

    def delete(self, pipeline_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))
            return cur.rowcount > 0

    def get(self, pipeline_id: str) -> Pipeline | None:
        row = self._fetch_row(pipeline_id)
        if row is None:
            return None
        return self._row_to_pipeline(row)

    def list_pipelines(self, *, limit: int = 100) -> list[Pipeline]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM pipelines ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._row_to_pipeline(row) for row in rows]

    def _fetch_row(self, pipeline_id: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM pipelines WHERE id = ?", (pipeline_id,)
            ).fetchone()

    @staticmethod
    def _row_to_pipeline(row: sqlite3.Row) -> Pipeline:
        graph = json.loads(row["graph_json"])
        layout = json.loads(row["layout_json"])
        nodes = [
            PipelineNode(
                id=n["id"],
                kind=n["kind"],
                agent=n.get("agent"),
                config=n.get("config") or {},
            )
            for n in graph.get("nodes") or []
        ]
        edges = [
            PipelineEdge(
                id=e["id"],
                source=e["source"],
                target=e["target"],
                mapping=e.get("mapping") or {},
            )
            for e in graph.get("edges") or []
        ]
        return Pipeline(
            id=row["id"],
            name=row["name"],
            nodes=nodes,
            edges=edges,
            layout=layout,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
