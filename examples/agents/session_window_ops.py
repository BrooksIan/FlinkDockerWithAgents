"""PyFlink window operators for session aggregation (cluster runner).

Cowrie/session_detect helpers remain in ``examples/agents`` for optional demos.
"""

from __future__ import annotations

from ratatoskr.pipelines.window_ops import (
    FixedGapExtractor,
    GenericSessionSummaryFunction,
    PolicyGapExtractor,
)

# Backward-compatible names used by older runners and docs.
CowrieActivityGapExtractor = PolicyGapExtractor
SessionSummaryFunction = GenericSessionSummaryFunction

__all__ = [
    "CowrieActivityGapExtractor",
    "FixedGapExtractor",
    "GenericSessionSummaryFunction",
    "PolicyGapExtractor",
    "SessionSummaryFunction",
]
