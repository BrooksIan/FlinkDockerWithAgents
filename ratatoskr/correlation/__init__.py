"""NiFi ↔ Kafka cross-signal correlation (observe-only)."""

from ratatoskr.correlation.engine import correlate_signals, run_correlate_cycle
from ratatoskr.correlation.rules import CORRELATION_RULES

__all__ = [
    "CORRELATION_RULES",
    "correlate_signals",
    "run_correlate_cycle",
]
