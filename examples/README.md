# Flink Agents Examples

<p align="center">
  <img src="../docs/branding/Ratatoskr_title_image.png" alt="Ratatoskr — wood-textured wordmark and squirrel mascot" width="360" />
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
| `react_echo` | react | `run_react_local.py` | Tool-chaining lab (no LLM) |
| `react_double_value` | react | `run_react_double_local.py` | LLM doubles numeric input (Designer settings) |
| `react_skills_demo` | react | `run_react_skills_demo_local.py` | Native `@chat_model_setup` + math-calculator skill |

```bash
ratatoskr agent list
ratatoskr agent describe workflow_counter
ratatoskr agent run workflow_counter --local
ratatoskr agent submit workflow_counter   # needs Flink cluster up
ratatoskr agent status
```

### Source layout

```text
examples/agents/
├── agent-manifest.yaml       # Registry (name, entry, runners)
├── workflow_counter.py       # CounterAgent
├── react_echo.py             # ReactEchoAgent
├── run_workflow_local.py     # AgentsExecutionEnvironment local
├── run_workflow_cluster.py   # PyFlink + Agents operator
├── run_react_local.py
└── run_react_cluster.py
```

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
