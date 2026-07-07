"""Shared HTTP helpers for Ratatoskr agents and platform tools.

Named ``httpio`` (not ``http``) to avoid shadowing the Python standard library
``http`` package when ``ratatoskr`` is on ``sys.path``.
"""

from ratatoskr.httpio.fetch import append_query, http_fetch_json

__all__ = ["append_query", "http_fetch_json"]
