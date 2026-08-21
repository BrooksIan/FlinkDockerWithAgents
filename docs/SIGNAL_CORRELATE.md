# Cross-signal correlation & incident scribe

Observe-only pairing of [NiFi](NIFI_MONITOR.md) and [Kafka](KAFKA_MONITOR.md) monitor OutputEvents, plus an optional ReAct brief.

## Agents

| Agent | Type | Role |
|-------|------|------|
| `workflow_signal_correlate` | workflow | Match rules across NiFi + Kafka severities → `incidents[]` |
| `react_incident_scribe` | react | Explain incidents (Designer LLM or deterministic fallback). **Never mutates.** |

## Rules (examples)

| Rule id | NiFi | Kafka | Level |
|---------|------|-------|-------|
| `pipeline_backpressure_lag` | BACKPRESSURE* | LAG_* / CONSUMER_STALLED | HIGH |
| `dual_unreachable` | NIFI_UNREACHABLE | BROKER_UNREACHABLE | HIGH |
| `nifi_stopped_kafka_lag` | STOPPED / DISABLED_SERVICE | LAG_* / stalled | HIGH |
| `nifi_invalid_kafka_missing` | INVALID / BULLETIN_ERROR | TOPIC_MISSING | MEDIUM |
| `stack_degraded` | any degradation | any degradation | MEDIUM (fallback) |

## Run

```bash
# Demo fixtures (no live NiFi/Kafka)
python examples/agents/run_workflow_signal_correlate_local.py --demo
python examples/agents/run_react_incident_scribe_local.py

# Live poll both monitors (phase=monitor) then correlate
python examples/agents/run_workflow_signal_correlate_local.py --live
```

Optional Kafka topics: `signals.correlate.output`, `signals.incident.brief`.
