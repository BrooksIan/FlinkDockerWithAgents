# NiFi Flow Monitoring with Flink Agents

This guide describes the **workflow agent** pattern for Apache NiFi health monitoring and phased auto-healing in Ratatoskr. It does not cover the honeypot stack.

## Why a workflow agent

NiFi heal actions should be **deterministic and auditable**: same health snapshot → same classification → same allowed mutations. That matches the [workflow agent](FLINK_AGENTS.md#workflow-agents) model (code-defined `@action` / `@tool` graph, no LLM in the loop).

![Workflow agent control flow](../assets/images/WorkflowAgentsDiagram.png)

## Components

| Piece | Role |
|-------|------|
| `ratatoskr up --profile nifi` | Flink (`deploy/`) + NiFi (`nifi/`) |
| `ratatoskr.nifi.NiFiClient` | Local REST client (MCP-aligned tool names) |
| `ratatoskr.nifi.policy` | Classify severities; apply heal by phase |
| `workflow_nifi_monitor` | Flink Agents workflow agent |
| [NiFi-MCP-Server](https://github.com/cloudera/NiFi-MCP-Server) | CDP dual path via Knox (Designer / Claude) |

## Severities

| Severity | Meaning |
|----------|---------|
| `STOPPED` | Processor in STOPPED state |
| `INVALID` | Processor validationStatus INVALID |
| `DISABLED_SERVICE` | Controller service DISABLED |
| `BACKPRESSURE` / `_WARN` / `_CRIT` | Queued flowfiles (graded by `NIFI_BP_WARN` / `NIFI_BP_CRIT`) |
| `BULLETIN_ERROR` | ERROR/WARNING bulletins on the board |
| `NIFI_SLOW` / `NIFI_UNREACHABLE` | API probe latency or connect failure |

## Heal matrix

| Phase | Env | Mutations |
|-------|-----|-----------|
| monitor (1A) | `NIFI_HEAL_PHASE=monitor` | none |
| safe (1B) | `NIFI_HEAL_PHASE=safe` | `enable_controller_service`, `start_processor` (ordered) |
| lab (1C) | `NIFI_HEAL_PHASE=lab` | safe + `stop_processor` (upstream of backpressure) + `terminate_processor`; `empty_connection_queue` if `NIFI_HEAL_ALLOW_EMPTY_QUEUE=1` |

Gates: `NIFI_HEAL_DRY_RUN`, `NIFI_HEAL_MAX_MUTATIONS`, `NIFI_HEAL_COOLDOWN_SEC`, `NIFI_HEAL_ALLOW_IDS` / `NIFI_HEAL_ALLOW_NAME_REGEX`, `NIFI_HEAL_VERIFY`.

## Run locally

```bash
ratatoskr up --profile nifi
./scripts/nifi_load_sample_flow.sh
export NIFI_HEAL_PHASE=monitor
ratatoskr agent run workflow_nifi_monitor --local
```

Inject a stopped processor, then heal:

```bash
python3 scripts/nifi_fault_inject.py --stop-generate
export NIFI_HEAL_PHASE=safe
python3 examples/agents/run_workflow_nifi_monitor_local.py
```

## Kafka→NiFi demo flow (shared base)

For combined NiFi + Kafka monitoring demos, load a ConsumeKafka pipeline (topic `nifi.kafka.demo`, group `ratatoskr-nifi-kafka-demo`):

```bash
ratatoskr kafka up
ratatoskr up --profile nifi
./scripts/nifi_load_kafka_flow.sh
```

Publish a test message from the host (`localhost:9094`):

```bash
python3 -c "from kafka import KafkaProducer; p=KafkaProducer(bootstrap_servers='localhost:9094'); p.send('nifi.kafka.demo', b'{\"hello\":\"nifi\"}'); p.flush()"
```

Future hooks: stop `ConsumeKafka` (NiFi STOPPED), delete `nifi.kafka.demo` (Kafka TOPIC_MISSING), stop `LogAttribute` (queue backlog), inspect group lag on `ratatoskr-nifi-kafka-demo`.

### Orchestrated heal demo (Kafka→NiFi)

Break `ConsumeKafka`, show monitor-only detection, then safe-phase heal:

```bash
python3 scripts/demo_nifi_kafka_heal.py
# or: python3 scripts/demo_nifi_kafka_heal.py --scenario disable-cs
```

Manual steps:

```bash
python3 scripts/nifi_fault_inject.py --target kafka --stop-consume
export NIFI_HEAL_PHASE=monitor
python3 examples/agents/run_workflow_nifi_monitor_local.py --count 1
# Expect STOPPED ConsumeKafka, heal_actions: []

export NIFI_HEAL_PHASE=safe
python3 examples/agents/run_workflow_nifi_monitor_local.py --count 1
# Expect start_processor on ConsumeKafka
```

## Heal demo script (1B / 1C)

Prereqs: `ratatoskr up --profile nifi`, `./scripts/nifi_load_sample_flow.sh`, and `source .venv/bin/activate`.
Use `--restore` between scenarios to reset the sample flow.

**1B — safe start (STOPPED GenerateFlowFile)**

```bash
python3 scripts/nifi_fault_inject.py --restore
python3 scripts/nifi_fault_inject.py --stop-generate
export NIFI_HEAL_PHASE=safe
python3 examples/agents/run_workflow_nifi_monitor_local.py --count 1
# Expect heal_actions: start_processor on GenerateFlowFile
```

**1C — lab terminate (INVALID LogAttribute)**

```bash
python3 scripts/nifi_fault_inject.py --restore
python3 scripts/nifi_fault_inject.py --invalid-log
export NIFI_HEAL_PHASE=lab
python3 examples/agents/run_workflow_nifi_monitor_local.py --count 1
# Expect heal_actions: terminate_processor on LogAttribute
# (INVALID processors are not started)
```

**1C — lab empty queue (BACKPRESSURE)**

```bash
python3 scripts/nifi_fault_inject.py --restore
python3 scripts/nifi_fault_inject.py --queue-backlog --settle-sec 5
export NIFI_HEAL_PHASE=lab
export NIFI_HEAL_ALLOW_EMPTY_QUEUE=1
python3 examples/agents/run_workflow_nifi_monitor_local.py --count 1
# Expect queued update-to-log, then empty_connection_queue (+ start LogAttribute)
```

`--queue-backlog` temporarily sets GenerateFlowFile to `1 sec` so queues build in a few seconds (NiFi’s default is often `1 min`).

Cluster path (after `ratatoskr build`):

```bash
export NIFI_HEAL_PHASE=monitor NIFI_MONITOR_POLLS=5
ratatoskr agent run workflow_nifi_monitor --cluster --profile nifi
```

## CDP / MCP dual path

Local Docker NiFi has no Knox. For CDP Flow Management:

1. Install / configure [NiFi-MCP-Server](https://github.com/cloudera/NiFi-MCP-Server) with `NIFI_API_BASE` and `KNOX_TOKEN`
2. Enable the **Apache NiFi (MCP)** entry under Dashboard Settings → MCP (catalog in `examples/mcp/mcp-server-catalog.yaml`)
3. Keep heal policies written against the same tool names as `ratatoskr.nifi.client`

## Continuous and cluster

- Host interval: `python examples/agents/run_workflow_nifi_monitor_local.py --interval 10 --count 6`
- Kafka ticks: `scripts/nifi_publish_poll_ticks.py` + runner `--kafka-topic nifi.monitor.poll`
- Cluster: see heal demo script above (`--cluster --profile nifi`)

Also: `--lab-demo` combines INVALID + backlog in one inject.

See [nifi/README.md](../nifi/README.md) for ports, sample flow, and layout.
