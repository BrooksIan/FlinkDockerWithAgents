"""Console script entry — loads Typer ``app`` from bytecode."""

from __future__ import annotations

from ratatoskr._bootstrap import get_app

app = get_app()
