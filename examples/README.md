# Flink Agents Examples

Generic demos and **registered agents** for the minimal Flink stack — no honeypot dependencies.

Part of the [Apemosyne](../README.md) workspace. Platform guide: [docs/PLATFORM.md](../docs/PLATFORM.md).

## Prerequisites

```bash
pip install -e .
apemosyne build
apemosyne up              # default: minimal JobManager + TaskManager
```

## Registered agents

Manifest: [`agents/agent-manifest.yaml`](agents/agent-manifest.yaml)  
Catalog: [`agents/agent-catalog.yaml`](agents/agent-catalog.yaml) — categories, display names, I/O schemas for the dashboard.

| Agent | Type | Local runner | Description |
|-------|------|--------------|-------------|
| `workflow_counter` | workflow | `run_workflow_local.py` | `@action` + `@tool` — doubles integers |
| `react_echo` | react | `run_react_local.py` | Tool-chaining lab (no LLM) |

```bash
apemosyne agent list
apemosyne agent describe workflow_counter
apemosyne agent run workflow_counter --local
apemosyne agent submit workflow_counter   # needs Flink cluster up
apemosyne agent status
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

4. Optionally add a demo in `apemosyne/manifests/demo-files.yaml`.

## Demos (TaskManager)

| Demo | Command | Description |
|------|---------|-------------|
| Datastream | `apemosyne demo datastream` | PyFlink DataStream smoke |
| Table | `apemosyne demo table` | Table API smoke |
| Workflow | `apemosyne demo workflow` | `workflow_counter` local runner |
| ReAct lab | `apemosyne demo react` | `react_echo` local runner |

Legacy PyFlink-only scripts in this directory:

- `demo_datastream.py`, `demo_table.py`, `demo_datastream_local.py`

## Control API

With `apemosyne api start` running:

```bash
curl http://127.0.0.1:8090/v1/agents
curl http://127.0.0.1:8090/v1/agents/workflow_counter
curl -X POST http://127.0.0.1:8090/v1/agents/workflow_counter/submit
```

Local dev: no `APEMOSYNE_API_KEY` required.

## Cowrie / security demos (optional)

Honeypot demos live under [`honeypot/demo/`](../honeypot/demo/):

```bash
apemosyne up --profile full
apemosyne demo cowrie
```

See [honeypot/README.md](../honeypot/README.md).
