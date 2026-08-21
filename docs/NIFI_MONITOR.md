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
| `BACKPRESSURE` | Connection with queued flowfiles |
| `BULLETIN_ERROR` | ERROR/WARNING bulletins on the board |

## Heal matrix

| Phase | Env | Mutations |
|-------|-----|-----------|
| monitor (1A) | `NIFI_HEAL_PHASE=monitor` | none |
| safe (1B) | `NIFI_HEAL_PHASE=safe` | `start_processor`, `enable_controller_service` |
| lab (1C) | `NIFI_HEAL_PHASE=lab` | safe + `terminate_processor`; `empty_connection_queue` if `NIFI_HEAL_ALLOW_EMPTY_QUEUE=1` |

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

## CDP / MCP dual path

Local Docker NiFi has no Knox. For CDP Flow Management:

1. Install / configure [NiFi-MCP-Server](https://github.com/cloudera/NiFi-MCP-Server) with `NIFI_API_BASE` and `KNOX_TOKEN`
2. Enable the **Apache NiFi (MCP)** entry under Dashboard Settings → MCP (catalog in `examples/mcp/mcp-server-catalog.yaml`)
3. Keep heal policies written against the same tool names as `ratatoskr.nifi.client`

See [nifi/README.md](../nifi/README.md) for ports, sample flow, and layout.
