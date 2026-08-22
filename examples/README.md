# Flink Agents Examples

<p align="center">
  <img src="../assets/branding/Ratatoskr_title_image.png" alt="Ratatoskr — wood-textured wordmark and squirrel mascot" width="360" />
</p>

Generic demos and **registered agents** for the minimal Flink stack — no honeypot dependencies.

Part of the [Ratatoskr](../README.md) workspace. Platform guide: [docs/PLATFORM.md](../docs/PLATFORM.md).

## Prerequisites

```bash
pip install -e .
ratatoskr build
ratatoskr up              # default: minimal JobManager + TaskManager
```

## Registered agents

Manifest: [`agents/agent-manifest.yaml`](agents/agent-manifest.yaml)  
Catalog: [`agents/agent-catalog.yaml`](agents/agent-catalog.yaml) — categories, display names, I/O schemas for the dashboard.

| Agent | Type | Local runner | Description |
|-------|------|--------------|-------------|
| `workflow_counter` | workflow | `run_workflow_local.py` | `@action` + `@tool` — doubles integers |
| `workflow_api_fetch` | workflow | `run_workflow_api_fetch_local.py` | HTTP GET/POST to configured API (Settings) |
| `workflow_nifi_monitor` | workflow | `run_workflow_nifi_monitor_local.py` | NiFi health monitor / heal (`NIFI_HEAL_PHASE`) |
| `workflow_kafka_monitor` | workflow | `run_workflow_kafka_monitor_local.py` | Kafka health monitor / heal (`KAFKA_HEAL_PHASE`) |
| `workflow_signal_correlate` | workflow | `run_workflow_signal_correlate_local.py` | NiFi↔Kafka correlation (observe-only) |
| `workflow_cross_stack_heal` | workflow | `run_workflow_cross_stack_heal_local.py` | Correlate + coordinated heals (`CROSS_HEAL_PHASE`) |
| `react_echo` | react | `run_react_local.py` | Tool-chaining lab (no LLM) |
| `react_incident_scribe` | react | `run_react_incident_scribe_local.py` | Explain correlated incidents (never mutates) |
| `react_cross_runbook` | react | `run_react_cross_runbook_local.py` | Cross-signal NiFi↔Kafka runbook (never mutates; HITL via `demo_cross_runbook.py`) |
| `react_nifi_runbook` | react | `run_react_nifi_runbook_local.py` | Structured NiFi debug runbook (never mutates) |
| `react_double_value` | react | `run_react_double_local.py` | LLM doubles numeric input (Designer settings) |
| `react_skills_demo` | react | `run_react_skills_demo_local.py` | Native `@chat_model_setup` + math-calculator skill |
| `session_detect` | workflow | `run_session_window_local.py` | Session severity from dynamic window batches |

```bash
ratatoskr agent list
ratatoskr agent describe workflow_counter
ratatoskr agent run workflow_counter --local
ratatoskr agent run workflow_api_fetch --local   # configure URL in Settings first
ratatoskr agent submit workflow_counter   # needs Flink cluster up
ratatoskr agent status
```

Heal demos (NiFi / Kafka / cross-stack): [docs/NIFI_MONITOR.md](../docs/NIFI_MONITOR.md#orchestrated-heal-examples) · [docs/KAFKA_MONITOR.md](../docs/KAFKA_MONITOR.md#orchestrated-heal-examples-shared-base) · [docs/SIGNAL_CORRELATE.md](../docs/SIGNAL_CORRELATE.md) · `python3 scripts/demo_nifi_kafka_heal.py --list`

### `workflow_api_fetch`

Fetches JSON from an HTTP endpoint on each input event. Configure in dashboard **Settings → API fetch (workflow agent)** or via environment:

```bash
export RATATOSKR_API_FETCH_ENDPOINT_URL=https://jsonplaceholder.typicode.com/
ratatoskr agent run workflow_api_fetch --local
```

Optional input fields: `path` / `path_suffix`, `query` (GET), `body` (POST). Output includes `url`, `status_code`, `ok`, and `data`.

### Source layout

```text
examples/agents/
├── agent-manifest.yaml
├── agent-catalog.yaml
├── workflow_counter.py / workflow_api_fetch.py
├── workflow_nifi_monitor.py / workflow_kafka_monitor.py
├── workflow_signal_correlate.py / workflow_cross_stack_heal.py
├── react_echo.py / react_incident_scribe.py / react_nifi_runbook.py / react_double_value.py
├── run_workflow_*_local.py / run_react_*_local.py
└── … cluster runners where registered
```

Guides: [docs/NIFI_MONITOR.md](../docs/NIFI_MONITOR.md) · [docs/KAFKA_MONITOR.md](../docs/KAFKA_MONITOR.md) · [docs/SIGNAL_CORRELATE.md](../docs/SIGNAL_CORRELATE.md).

NiFi runbook POC: `python3 scripts/demo_nifi_runbook.py --list` · `--heal --approve`.
Cross-signal runbook + HITL: `python3 scripts/demo_cross_runbook.py --scenario topic-missing --heal --approve` · live: `--live --inject --heal --approve`.

### Add a new agent

1. Create `examples/agents/my_agent.py` — subclass `flink_agents` `Agent` with `@action` / `@tool`.
2. Add `run_my_agent_local.py` and optional `run_my_agent_cluster.py`.
3. Register in `agent-manifest.yaml`:

```yaml
agents:
  my_agent:
    type: workflow
    description: One-line summary
    entry: examples.agents.my_agent:MyAgent
    runner: examples/agents/run_my_agent_local.py
    cluster_script: examples/agents/run_my_agent_cluster.py
```

4. Optionally add a demo in `ratatoskr/manifests/demo-files.yaml`.

## Demos (TaskManager)

| Demo | Command | Description |
|------|---------|-------------|
| Datastream | `ratatoskr demo datastream` | PyFlink DataStream smoke |
| Table | `ratatoskr demo table` | Table API smoke |
| Workflow | `ratatoskr demo workflow` | `workflow_counter` local runner |
| ReAct lab | `ratatoskr demo react` | `react_echo` local runner |

Legacy PyFlink-only scripts in this directory:

- `demo_datastream.py`, `demo_table.py`, `demo_datastream_local.py`

## Control API

With `ratatoskr api start` running:

```bash
curl http://127.0.0.1:8090/v1/agents
curl http://127.0.0.1:8090/v1/agents/workflow_counter
curl -X POST http://127.0.0.1:8090/v1/agents/workflow_counter/submit
```

Local dev: no `RATATOSKR_API_KEY` required.

## Cowrie / security demos (optional)

Honeypot demos live under [`honeypot/demo/`](../honeypot/demo/):

```bash
ratatoskr up --profile full
ratatoskr demo cowrie
```

See [honeypot/README.md](../honeypot/README.md).
