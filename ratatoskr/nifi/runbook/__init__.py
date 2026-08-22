"""NiFi debugging runbook helpers (schema, fixtures, fallback, Phase 2 context, Phase 3 demo)."""

from ratatoskr.nifi.runbook.context import (
    allowed_remediation,
    constrain_remediation,
    enrich_monitor_context,
    order_refs,
    proposed_heal_plan,
    severity_guidance,
)
from ratatoskr.nifi.runbook.demo import (
    RUNBOOK_BRIEF_TOPIC,
    SCENARIOS,
    list_scenarios,
    operator_talking_points,
    publish_runbook_brief,
    run_offline_scenario,
    summarize_monitor,
    summarize_runbook,
)
from ratatoskr.nifi.runbook.fallback import fallback_runbook
from ratatoskr.nifi.runbook.fixtures import (
    FIXTURE_PACKS,
    list_fixture_ids,
    load_fixture,
)
from ratatoskr.nifi.runbook.schema import (
    RUNBOOK_SCHEMA_VERSION,
    empty_runbook,
    is_valid_runbook,
    is_valid_runbook_event,
    validate_runbook,
    validate_runbook_event,
    wrap_runbook_event,
)

__all__ = [
    "FIXTURE_PACKS",
    "RUNBOOK_BRIEF_TOPIC",
    "RUNBOOK_SCHEMA_VERSION",
    "SCENARIOS",
    "allowed_remediation",
    "constrain_remediation",
    "empty_runbook",
    "enrich_monitor_context",
    "fallback_runbook",
    "is_valid_runbook",
    "is_valid_runbook_event",
    "list_fixture_ids",
    "list_scenarios",
    "load_fixture",
    "operator_talking_points",
    "order_refs",
    "proposed_heal_plan",
    "publish_runbook_brief",
    "run_offline_scenario",
    "severity_guidance",
    "summarize_monitor",
    "summarize_runbook",
    "validate_runbook",
    "validate_runbook_event",
    "wrap_runbook_event",
]
