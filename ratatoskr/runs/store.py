"""SQLite persistence for agent runs and spans."""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from ratatoskr.paths import project_root
from ratatoskr.runs.models import Run, RunKind, RunStatus, Span, SpanKind, SpanStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    agent TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    flink_job_id TEXT,
    error TEXT,
    record_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS spans (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    parent_id TEXT,
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    duration_ms INTEGER,
    input_json TEXT,
    output_json TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS idx_runs_agent ON runs(agent);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_spans_run ON spans(run_id);
"""


def runs_db_path(root: Path | None = None) -> Path:
    env = os.environ.get("RATATOSKR_RUNS_DB", "").strip()
    if env:
        return Path(env)
    repo = root or project_root()
    directory = repo / ".ratatoskr"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / "runs.db"


class RunStore:
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

    def insert_run(
        self,
        *,
        run_id: str,
        agent: str,
        kind: RunKind,
        status: RunStatus,
        started_at: str,
        flink_job_id: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (id, agent, kind, status, started_at, flink_job_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, agent, kind, status, started_at, flink_job_id),
            )

    def update_run(
        self,
        run_id: str,
        *,
        status: RunStatus | None = None,
        finished_at: str | None = None,
        flink_job_id: str | None = None,
        error: str | None = None,
        record_count: int | None = None,
    ) -> None:
        fields: list[str] = []
        values: list[Any] = []
        if status is not None:
            fields.append("status = ?")
            values.append(status)
        if finished_at is not None:
            fields.append("finished_at = ?")
            values.append(finished_at)
        if flink_job_id is not None:
            fields.append("flink_job_id = ?")
            values.append(flink_job_id)
        if error is not None:
            fields.append("error = ?")
            values.append(error)
        if record_count is not None:
            fields.append("record_count = ?")
            values.append(record_count)
        if not fields:
            return
        values.append(run_id)
        with self._connect() as conn:
            conn.execute(f"UPDATE runs SET {', '.join(fields)} WHERE id = ?", values)

    def insert_span(
        self,
        *,
        span_id: str,
        run_id: str,
        kind: SpanKind,
        name: str,
        status: SpanStatus,
        started_at: str,
        parent_id: str | None = None,
        finished_at: str | None = None,
        duration_ms: int | None = None,
        input_data: Any | None = None,
        output_data: Any | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO spans (
                    id, run_id, parent_id, kind, name, status,
                    started_at, finished_at, duration_ms, input_json, output_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    span_id,
                    run_id,
                    parent_id,
                    kind,
                    name,
                    status,
                    started_at,
                    finished_at,
                    duration_ms,
                    json.dumps(input_data) if input_data is not None else None,
                    json.dumps(output_data) if output_data is not None else None,
                ),
            )

    def get_run(self, run_id: str) -> Run | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            spans = self._load_spans(conn, run_id)
        return self._row_to_run(row, spans)

    def list_runs(
        self,
        *,
        agent: str | None = None,
        limit: int = 50,
    ) -> list[Run]:
        query = "SELECT * FROM runs"
        params: list[Any] = []
        if agent:
            query += " WHERE agent = ?"
            params.append(agent)
        query += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_run(row, []) for row in rows]

    def _load_spans(self, conn: sqlite3.Connection, run_id: str) -> list[Span]:
        rows = conn.execute(
            "SELECT * FROM spans WHERE run_id = ? ORDER BY started_at ASC",
            (run_id,),
        ).fetchall()
        return [self._row_to_span(row) for row in rows]

    @staticmethod
    def _row_to_run(row: sqlite3.Row, spans: list[Span]) -> Run:
        return Run(
            id=row["id"],
            agent=row["agent"],
            kind=row["kind"],
            status=row["status"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            flink_job_id=row["flink_job_id"],
            error=row["error"],
            record_count=int(row["record_count"] or 0),
            spans=spans,
        )

    @staticmethod
    def _row_to_span(row: sqlite3.Row) -> Span:
        input_data = json.loads(row["input_json"]) if row["input_json"] else None
        output_data = json.loads(row["output_json"]) if row["output_json"] else None
        return Span(
            id=row["id"],
            run_id=row["run_id"],
            parent_id=row["parent_id"],
            kind=row["kind"],
            name=row["name"],
            status=row["status"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            duration_ms=row["duration_ms"],
            input=input_data,
            output=output_data,
        )
