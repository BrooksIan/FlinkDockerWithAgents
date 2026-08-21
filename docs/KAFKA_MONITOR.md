# Kafka Cluster Monitoring with Flink Agents

Deterministic **workflow agent** for Apache Kafka health monitoring and phased auto-healing in Ratatoskr (same pattern as [NiFi](NIFI_MONITOR.md)).

## Components

| Piece | Role |
|-------|------|
| `ratatoskr kafka up` | Studio Kafka (`deploy/docker-compose.kafka.yml`) |
| `ratatoskr.kafka.KafkaClient` | Admin client (probe, describe, lag, create topic) |
| `ratatoskr.kafka.policy` | Classify severities; apply heal by phase |
| `workflow_kafka_monitor` | Flink Agents workflow agent |

## Severities

| Severity | Meaning |
|----------|---------|
| `BROKER_UNREACHABLE` / `BROKER_SLOW` | Bootstrap / metadata probe |
| `TOPIC_MISSING` | Canonical catalog topic absent on broker |
| `TOPIC_UNEXPECTED` | Extra live topic (opt-in via `KAFKA_FLAG_UNEXPECTED=1`) |
| `UNDER_REPLICATED` / `OFFLINE_PARTITION` | ISR / leader issues |
| `LAG_WARN` / `LAG_CRIT` | Consumer group lag vs `KAFKA_LAG_*` |
| `CONSUMER_STALLED` / `GROUP_EMPTY` | Members=0 while lag > 0 |

## Heal matrix

| Phase | Env | Mutations |
|-------|-----|-----------|
| monitor | `KAFKA_HEAL_PHASE=monitor` | none |
| safe | `KAFKA_HEAL_PHASE=safe` | `create_topic` for missing catalog topics |
| lab | `KAFKA_HEAL_PHASE=lab` | safe + `reset_offsets` / `delete_group` **only** if listed in `KAFKA_HEAL_ALLOW_GROUPS` |

Gates: `KAFKA_HEAL_DRY_RUN`, `KAFKA_HEAL_MAX_MUTATIONS`, `KAFKA_HEAL_COOLDOWN_SEC`, `KAFKA_HEAL_ALLOW_TOPICS`, `KAFKA_HEAL_VERIFY`.

## Run locally

```bash
ratatoskr kafka up
export KAFKA_HEAL_PHASE=monitor
# Default KAFKA_CATALOG=studio — expects Studio topics only (not cowrie.*)
python examples/agents/run_workflow_kafka_monitor_local.py
# or: ratatoskr agent run workflow_kafka_monitor --local

export KAFKA_HEAL_PHASE=safe KAFKA_HEAL_DRY_RUN=1
python examples/agents/run_workflow_kafka_monitor_local.py
```

Honeypot / full-stack catalog: `export KAFKA_CATALOG=full`

Continuous: `python examples/agents/run_workflow_kafka_monitor_local.py --interval 10 --count 6`

Topics: `kafka.monitor.poll` / `kafka.monitor.output` (Studio kafka-init + `kafka_sources`).