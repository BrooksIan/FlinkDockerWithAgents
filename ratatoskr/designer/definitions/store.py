"""SQLite persistence for agent definitions (designer.db)."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from ratatoskr.designer.definitions.models import (
    AgentDefinition,
    AgentDefinitionEdge,
    AgentDefinitionNode,
)
from ratatoskr.designer.store import designer_db_path

_SCHEMA = """
CREATE TABLE IF NOT EXISTS agent_definitions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'draft',
    graph_json TEXT NOT NULL,
    layout_json TEXT NOT NULL,
    io_json TEXT NOT NULL,
    manifest_name TEXT,
    catalog_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_definitions_updated ON agent_definitions(updated_at DESC);
"""


class AgentDefinitionStore:
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
        definition_id: str,
        name: str,
        agent_type: str,
        version: int,
        description: str,
        status: str,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        layout: dict[str, dict[str, float]],
        input_schema: dict[str, Any],
        output_schema: dict[str, Any],
        manifest_name: str | None,
        catalog: dict[str, Any],
        created_at: str,
        updated_at: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_definitions (
                    id, name, type, version, description, status,
                    graph_json, layout_json, io_json, manifest_name, catalog_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    definition_id,
                    name,
                    agent_type,
                    version,
                    description,
                    status,
                    json.dumps({"nodes": nodes, "edges": edges}),
                    json.dumps(layout),
                    json.dumps(
                        {"input_schema": input_schema, "output_schema": output_schema}
                    ),
                    manifest_name,
                    json.dumps(catalog),
                    created_at,
                    updated_at,
                ),
            )

    def update(
        self,
        definition_id: str,
        *,
        name: str | None = None,
        agent_type: str | None = None,
        version: int | None = None,
        description: str | None = None,
        status: str | None = None,
        nodes: list[dict[str, Any]] | None = None,
        edges: list[dict[str, Any]] | None = None,
        layout: dict[str, dict[str, float]] | None = None,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
        manifest_name: str | None = None,
        catalog: dict[str, Any] | None = None,
        updated_at: str,
    ) -> bool:
        row = self._fetch_row(definition_id)
        if row is None:
            return False

        graph = json.loads(row["graph_json"])
        layout_current = json.loads(row["layout_json"])
        io = json.loads(row["io_json"])
        catalog_current = json.loads(row["catalog_json"])

        with self._connect() as conn:
            conn.execute(
                """
                UPDATE agent_definitions
                SET name = ?, type = ?, version = ?, description = ?, status = ?,
                    graph_json = ?, layout_json = ?, io_json = ?, manifest_name = ?,
                    catalog_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    name if name is not None else row["name"],
                    agent_type if agent_type is not None else row["type"],
                    version if version is not None else row["version"],
                    description if description is not None else row["description"],
                    status if status is not None else row["status"],
                    json.dumps(
                        {
                            "nodes": nodes if nodes is not None else graph["nodes"],
                            "edges": edges if edges is not None else graph["edges"],
                        }
                    ),
                    json.dumps(layout if layout is not None else layout_current),
                    json.dumps(
                        {
                            "input_schema": (
                                input_schema
                                if input_schema is not None
                                else io.get("input_schema") or {}
                            ),
                            "output_schema": (
                                output_schema
                                if output_schema is not None
                                else io.get("output_schema") or {}
                            ),
                        }
                    ),
                    manifest_name if manifest_name is not None else row["manifest_name"],
                    json.dumps(catalog if catalog is not None else catalog_current),
                    updated_at,
                    definition_id,
                ),
            )
        return True

    def delete(self, definition_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM agent_definitions WHERE id = ?", (definition_id,)
            )
            return cur.rowcount > 0

    def get(self, definition_id: str) -> AgentDefinition | None:
        row = self._fetch_row(definition_id)
        if row is None:
            return None
        return self._row_to_definition(row)

    def list_definitions(self, *, limit: int = 100) -> list[AgentDefinition]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM agent_definitions ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._row_to_definition(row) for row in rows]

    def count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS n FROM agent_definitions").fetchone()
            return int(row["n"]) if row else 0

    def _fetch_row(self, definition_id: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM agent_definitions WHERE id = ?", (definition_id,)
            ).fetchone()

    @staticmethod
    def _row_to_definition(row: sqlite3.Row) -> AgentDefinition:
        graph = json.loads(row["graph_json"])
        layout = json.loads(row["layout_json"])
        io = json.loads(row["io_json"])
        catalog = json.loads(row["catalog_json"])
        nodes = [
            AgentDefinitionNode(
                id=n["id"],
                kind=n["kind"],
                name=str(n.get("name") or ""),
                config=n.get("config") or {},
            )
            for n in graph.get("nodes") or []
        ]
        edges = [
            AgentDefinitionEdge(
                id=e["id"],
                source=e["source"],
                target=e["target"],
                kind=e["kind"],
            )
            for e in graph.get("edges") or []
        ]
        return AgentDefinition(
            id=row["id"],
            name=row["name"],
            type=row["type"],
            version=int(row["version"]),
            description=row["description"],
            status=row["status"],
            nodes=nodes,
            edges=edges,
            layout=layout,
            input_schema=io.get("input_schema") or {},
            output_schema=io.get("output_schema") or {},
            manifest_name=row["manifest_name"],
            catalog_category_id=catalog.get("category_id"),
            catalog_subcategory_id=catalog.get("subcategory_id"),
            catalog_tags=list(catalog.get("tags") or []),
            mcp_servers=list(catalog.get("mcp_servers") or []),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


def agent_definitions_store(root: Path | None = None) -> AgentDefinitionStore:
    return AgentDefinitionStore(designer_db_path(root))
