"""SQLite persistence for agent designer platform settings."""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from ratatoskr.paths import project_root

_SCHEMA = """
CREATE TABLE IF NOT EXISTS platform_settings (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

REACT_LLM_KEY = "react_llm_defaults"


def designer_db_path(root: Path | None = None) -> Path:
    env = os.environ.get("RATATOSKR_DESIGNER_DB", "").strip()
    if env:
        return Path(env)
    repo = root or project_root()
    directory = repo / ".ratatoskr"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / "designer.db"


class DesignerStore:
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

    def get_json(self, key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value_json FROM platform_settings WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        data = json.loads(row["value_json"])
        return data if isinstance(data, dict) else None

    def set_json(self, key: str, value: dict[str, Any], *, updated_at: str) -> None:
        payload = json.dumps(value)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO platform_settings (key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (key, payload, updated_at),
            )
