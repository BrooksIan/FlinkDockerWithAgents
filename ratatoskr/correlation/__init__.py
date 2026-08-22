"""NiFi ↔ Kafka cross-signal correlation and optional coordinated heals."""

from ratatoskr.correlation.engine import (
    correlate_signals,
    run_correlate_cycle,
    run_cross_stack_cycle,
)
from ratatoskr.correlation.heal import CROSS_HEAL_PLAYBOOKS, plan_cross_heals
from ratatoskr.correlation.rules import CORRELATION_RULES, DATAPLANE_CORRELATION_RULES
from ratatoskr.correlation.runbook import (
    allowed_cross_remediation,
    fallback_cross_runbook,
    slim_correlation,
)

__all__ = [
    "CORRELATION_RULES",
    "CROSS_HEAL_PLAYBOOKS",
    "DATAPLANE_CORRELATION_RULES",
    "allowed_cross_remediation",
    "correlate_signals",
    "fallback_cross_runbook",
    "plan_cross_heals",
    "run_correlate_cycle",
    "run_cross_stack_cycle",
    "slim_correlation",
]
