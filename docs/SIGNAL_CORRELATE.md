# Cross-signal correlation & cross-stack heal

Pairing of [NiFi](NIFI_MONITOR.md) and [Kafka](KAFKA_MONITOR.md) monitor OutputEvents into incidents, optional ReAct brief, and gated coordinated heals on the shared Kafka→NiFi demo flow.

## Agents

| Agent | Type | Role |
|-------|------|------|
| `workflow_signal_correlate` | workflow | Match rules across NiFi + Kafka severities → `incidents[]` (observe-only) |
| `workflow_cross_stack_heal` | workflow | Correlate, then run playbooks when `CROSS_HEAL_PHASE=lab` |
| `react_incident_scribe` | react | Explain incidents (Designer LLM or deterministic fallback). **Never mutates.** |

## Rules

| Rule id | NiFi | Kafka | Level | Cross heal (lab) |
|---------|------|-------|-------|------------------|
| `pipeline_backpressure_lag` | BACKPRESSURE* | LAG_* / CONSUMER_STALLED | HIGH | NiFi queue relief (`stop` / `empty_connection_queue` / `start`) |
| `dual_unreachable` | NIFI_UNREACHABLE | BROKER_UNREACHABLE | HIGH | — (infra) |
| `nifi_stopped_kafka_lag` | STOPPED / DISABLED_SERVICE | LAG_* / stalled | HIGH | Start ConsumeKafka path (safe) |
| `nifi_invalid_kafka_missing` | INVALID / BULLETIN_ERROR | TOPIC_MISSING | MEDIUM | `create_topic` + NiFi lab fix/start |
| `kafka_topic_nifi_consumer` | STOPPED / INVALID / … | TOPIC_MISSING | HIGH | `create_topic` → `start_processor` ConsumeKafka |
| `stack_degraded` | any degradation | any degradation | MEDIUM (fallback) | — |

Playbooks live in `ratatoskr/correlation/heal.py` (`CROSS_HEAL_PLAYBOOKS`). Each step narrows health and reuses side `apply_heal_policy`.

## Env

| Variable | Default | Meaning |
|----------|---------|---------|
| `CROSS_HEAL_PHASE` | `monitor` | `monitor` = plan only; `lab` = execute playbooks |
| `CROSS_HEAL_DRY_RUN` | off | Propose without mutating |
| `CROSS_HEAL_ALLOW_EMPTY_QUEUE` | off | Allow NiFi `empty_connection_queue` in lag playbook |
| `CROSS_HEAL_DEMO_TOPIC` | `nifi.kafka.demo` | Preferred topic for create_topic steps |
| `CROSS_HEAL_CONSUME_NAMES` | `ConsumeKafka` | Preferred processor names for start steps |

Side gates (`NIFI_HEAL_*`, `KAFKA_HEAL_*`) still apply inside each playbook step.

## Offline fixtures

```bash
# Correlate BACKPRESSURE + LAG_CRIT (no brokers)
python examples/agents/run_workflow_signal_correlate_local.py --demo

# Plan cross heals for topic-missing / backpressure-lag fixtures
python examples/agents/run_workflow_cross_stack_heal_local.py --demo topic-missing
python examples/agents/run_workflow_cross_stack_heal_local.py --demo backpressure-lag

# Incident brief (never mutates)
python examples/agents/run_react_incident_scribe_local.py
```

## Live heal examples

Prereqs: `ratatoskr kafka up`, `ratatoskr up --profile nifi`, `./scripts/nifi_load_kafka_flow.sh`.

```bash
# Correlate only (live poll both monitors)
python examples/agents/run_workflow_signal_correlate_local.py --live

# Cross-stack agent — plan (default) or heal
python examples/agents/run_workflow_cross_stack_heal_local.py --live --phase monitor
CROSS_HEAL_PHASE=lab CROSS_HEAL_ALLOW_EMPTY_QUEUE=1 \
  python examples/agents/run_workflow_cross_stack_heal_local.py --live --phase lab
```

### Catalog demos (`demo_nifi_kafka_heal.py`)

| Scenario | Inject | Matched rule(s) | Heal ops |
|----------|--------|-----------------|----------|
| `cross-topic` | Stop ConsumeKafka + delete `nifi.kafka.demo` | `kafka_topic_nifi_consumer` | `create_topic` → `start_processor` |
| `cross-lag` | Stop LogAttribute + publish backlog + lag fault group | `pipeline_backpressure_lag`, `nifi_stopped_kafka_lag` | queue relief + start stopped |

```bash
python3 scripts/demo_nifi_kafka_heal.py --scenario cross-topic
python3 scripts/demo_nifi_kafka_heal.py --scenario cross-lag
python3 scripts/demo_nifi_kafka_heal.py --dry-run --scenario cross-topic
```

Single-stack heal examples on the same flow: [NIFI_MONITOR.md](NIFI_MONITOR.md#orchestrated-heal-examples) · [KAFKA_MONITOR.md](KAFKA_MONITOR.md#orchestrated-heal-examples-shared-base).

Optional Kafka topics: `signals.correlate.output`, `signals.cross_heal.output`, `signals.incident.brief`.
