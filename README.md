# Cloudera Blueprint: Ratatoskr — Apache Flink Agents on Docker

<p align="center">
  <img src="assets/branding/Ratatoskr_title_image.png" alt="Ratatoskr — wood-textured wordmark and squirrel mascot" width="480" />
</p>

Named for the Norse squirrel that carries messages up and down **Yggdrasil** — a fit for event pipelines, Kafka, and Flink streams. See [assets/branding/RATATOSKR.md](assets/branding/RATATOSKR.md). Catalog metadata: [`METADATA.yaml`](METADATA.yaml).

## Overview

Ratatoskr is a developer workspace for building, running, and verifying [Apache Flink Agents](https://github.com/apache/flink-agents) on Docker. It ships a Typer CLI, FastAPI Control API, React dashboard (Agent Designer + Agentic Studio), and registered workflow/ReAct agents — with a single-command path from clone to working Flink jobs.

**Primary use cases**

| Use case | What it demonstrates | Start here |
|----------|----------------------|------------|
| [Honeypot](#1-honeypot--cybersecurity) | Cowrie → Kafka → Flink Agents triage and enrichment | [honeypot/README.md](honeypot/README.md) |
| [NiFi monitoring](#2-nifi-flow-monitoring) | Flow health, phased heal, runbook HITL | [nifi/README.md](nifi/README.md) · [docs/NIFI_MONITOR.md](docs/NIFI_MONITOR.md) · [docs/NIFI_RUNBOOK.md](docs/NIFI_RUNBOOK.md) |
| [Kafka monitoring](#3-kafka-cluster-monitoring) | Broker/topic/lag probes and phased healing | [docs/KAFKA_MONITOR.md](docs/KAFKA_MONITOR.md) |
| [Cross-signal](#4-cross-signal-correlation) | NiFi↔Kafka incidents, scribe, coordinated heals | [docs/SIGNAL_CORRELATE.md](docs/SIGNAL_CORRELATE.md) |

Registered agents (manifest + dashboard catalog): [`examples/agents/agent-catalog.yaml`](examples/agents/agent-catalog.yaml) · [`examples/agents/agent-manifest.yaml`](examples/agents/agent-manifest.yaml). Browse them in the dashboard at `/agents`, or via `ratatoskr agent list`.

## Demo

![Ratatoskr Overview — live health, Flink status, and recent jobs](assets/images/UIScreenshots/Overview.png)

| | |
|---|---|
| Agent catalog | ![Agent catalog](assets/images/UIScreenshots/AgentCatalog.png) |
| Agent Designer | ![Agent Designer](assets/images/UIScreenshots/AgentDesigner.png) |
| Agentic Studio | ![Agentic Studio](assets/images/UIScreenshots/PipelineStudio.png) |
| Studio canvas | ![Studio canvas](assets/images/UIScreenshots/JobsCanvas.png) |
| Runs | ![Runs](assets/images/UIScreenshots/JobsView.png) |
| Settings | ![Settings](assets/images/UIScreenshots/Settings_LLMs.png) |

## Use Cases

### 1. Honeypot / cybersecurity

Live SSH/Telnet honeypot (Cowrie) streams attack events through Kafka into Flink Agents: a hot-path workflow for triage (`cowrie.alerts`) and optional ReAct enrichment via Cloudera AI Inference (`cowrie.react_alerts`).

```bash
ratatoskr up --profile full
ratatoskr dashboard
```

- Dashboard: http://localhost:8501 · Flink UI: http://localhost:8081
- Details: [honeypot/README.md](honeypot/README.md)

### 2. NiFi flow monitoring

A deterministic workflow agent (`workflow_nifi_monitor`) polls NiFi health — stopped/invalid processors, queues, bulletins — and can auto-heal under gated phases (`monitor` → `safe` → `lab`). Optional ReAct agent `react_nifi_runbook` turns those facts into a structured debug runbook (Cloudera Inference or fallback; never mutates). Local labs use NiFi REST; CDP uses the same tool semantics via [NiFi-MCP-Server](https://github.com/cloudera/NiFi-MCP-Server).

```bash
ratatoskr up --profile nifi
./scripts/nifi_load_sample_flow.sh
export NIFI_HEAL_PHASE=monitor   # or safe / lab
ratatoskr agent run workflow_nifi_monitor --local
python examples/agents/run_react_nifi_runbook_local.py   # fixture; add --live after a poll
```

- NiFi UI: https://localhost:8443/nifi — login `admin` / `RatatoskrNiFi1!`
- Runbook POC: `python3 scripts/demo_nifi_runbook.py --heal --approve` · HITL before mutate · [docs/NIFI_RUNBOOK.md](docs/NIFI_RUNBOOK.md)
- Heal demos: [docs/NIFI_MONITOR.md](docs/NIFI_MONITOR.md#orchestrated-heal-examples) · `python3 scripts/demo_nifi_kafka_heal.py --list`
- Continuous: `ratatoskr monitor start` · [docs/NIFI_MONITOR.md](docs/NIFI_MONITOR.md#continuous-and-cluster)
- Details: [nifi/README.md](nifi/README.md) · [docs/NIFI_MONITOR.md](docs/NIFI_MONITOR.md)

### 3. Kafka cluster monitoring

The same phased pattern for Apache Kafka (`workflow_kafka_monitor`): probe brokers, catalog topics, partitions, and consumer lag; heal only under explicit gates (`KAFKA_HEAL_PHASE=monitor|safe|lab`).

```bash
ratatoskr kafka up
export KAFKA_HEAL_PHASE=monitor
ratatoskr agent run workflow_kafka_monitor --local
```

- Heal demos: [docs/KAFKA_MONITOR.md](docs/KAFKA_MONITOR.md#heal-demo-script-safe--lab) · continuous/cluster [docs/KAFKA_MONITOR.md](docs/KAFKA_MONITOR.md#how-kafka-monitor--heal-gets-deployed)
- Cross-stack: [docs/SIGNAL_CORRELATE.md](docs/SIGNAL_CORRELATE.md)
- Details: [docs/KAFKA_MONITOR.md](docs/KAFKA_MONITOR.md)

### 4. Cross-signal correlation

Deterministic correlation of NiFi + Kafka monitor OutputEvents (`workflow_signal_correlate`), optional ReAct brief (`react_incident_scribe`) or structured cross runbook (`react_cross_runbook`, never mutates), and gated coordinated heals (`workflow_cross_stack_heal`).

```bash
python examples/agents/run_workflow_signal_correlate_local.py --demo
python examples/agents/run_react_incident_scribe_local.py
python3 scripts/demo_cross_runbook.py
python3 scripts/demo_nifi_kafka_heal.py --scenario cross-topic
```

- Details: [docs/SIGNAL_CORRELATE.md](docs/SIGNAL_CORRELATE.md)

## Agent catalog

All runnable agents are registered in:

| File | Role |
|------|------|
| [`examples/agents/agent-catalog.yaml`](examples/agents/agent-catalog.yaml) | Dashboard catalog — categories, display names, I/O schemas |
| [`examples/agents/agent-manifest.yaml`](examples/agents/agent-manifest.yaml) | Runtime registry — entry points, local/cluster runners |

```bash
ratatoskr agent list
ratatoskr agent describe workflow_counter
ratatoskr agent run workflow_counter --local
```

How to add agents and example runners: [examples/README.md](examples/README.md).

## Agent Designer & Agentic Studio

The React dashboard includes two visual tools for authoring agents and composing them into pipelines. Start the Control API and UI (`ratatoskr api start` + `./scripts/dev-start.sh`), then open http://localhost:3000.

### Agent Designer (`/designer`)

Build **workflow** and **ReAct** agents on a canvas without hand-editing Python:

1. **Create agent** — Workflow or ReAct template
2. Edit nodes and edges — actions, tools, LLM / MCP nodes; auto-save and validate
3. **Compile** — emits Python, Flink YAML, manifest snippet, and a local runner under `.ratatoskr/agents/{id}/`

ReAct agents need an LLM under **Settings**. Details: [dashboard/README.md](dashboard/README.md) · [docs/AGENT_DESIGNER_PLAN.md](docs/AGENT_DESIGNER_PLAN.md).

### Agentic Studio (`/studio`)

Compose **linear multi-agent pipelines** (Source → optional window → Agent(s) → Sink) from catalog and Designer agents:

- **Canvas** — Source (records or Kafka), dynamic session window, agents from the [catalog](examples/agents/agent-catalog.yaml), Capture or Kafka sink
- **Build with assistant** — guided form → pipeline draft; optional LLM refine; can suggest missing agents before create
- **Run locally** or **Run on Flink cluster** — history under `/runs`; Kafka sink default topic `workflow.test.output`

Kafka sources require a dynamic session window (enforced for cluster submit). Cluster runs need Studio Kafka (`ratatoskr kafka up`) and the minimal Flink stack. Templates include Counter → Echo and the Yggdrasil event pipeline.

Pipelines persist in `.ratatoskr/pipelines.db`. Full page guide: [dashboard/README.md](dashboard/README.md#agentic-studio).

## Quickstart

```bash
git clone https://github.com/BrooksIan/FlinkDockerWithAgents.git
cd FlinkDockerWithAgents
pip install -e .
cp .env.example .env   # optional Studio defaults

ratatoskr build
ratatoskr up           # JobManager + TaskManager
ratatoskr kafka up     # Studio Kafka on :9094
ratatoskr api start

curl http://127.0.0.1:8090/v1/health
ratatoskr agent list
ratatoskr doctor
```

Dashboard (optional):

```bash
./scripts/dev-start.sh   # http://localhost:3000
# Stop: ./scripts/dev-stop.sh
```

After code or image updates: `./scripts/restart-studio-cluster.sh` (`--dev`, `--build --dev`, or `--sync-only`).

| URL | Service |
|-----|---------|
| http://localhost:3000 | Dashboard (dev) |
| http://localhost:8082 | Flink Web UI (minimal / Studio) |
| http://localhost:8081 | Flink Web UI (honeypot / full) |
| http://localhost:9094 | Studio Kafka bootstrap |
| http://127.0.0.1:8090/docs | Control API (Swagger) |

## Architecture

```mermaid
flowchart TB
  subgraph User["Developer / Operator"]
    CLI["ratatoskr CLI"]
    UI["Dashboard :3000"]
  end

  subgraph Plane["Control Plane"]
    API["FastAPI Control API :8090"]
    REG["Agent Registry"]
    GEN["Agent Designer codegen"]
  end

  subgraph Runtime["Flink Runtime (Docker)"]
    JM["JobManager"]
    TM["TaskManager"]
    K["Studio Kafka :9094"]
  end

  subgraph Agents["Flink Agents"]
    WF["Workflow Agents"]
    RA["ReAct Agents"]
  end

  CLI --> API
  UI --> API
  API --> REG & GEN
  GEN --> Agents
  Agents -->|deploy| JM
  JM <--> TM
  TM <--> K
```

| Component | Role |
|-----------|------|
| Apache Flink + Flink Agents | Streaming runtime and agent SDK |
| Docker Compose (`deploy/`) | JobManager, TaskManager, Studio Kafka |
| `ratatoskr` CLI / Control API | Build, stack, agents, health |
| React dashboard | Overview, catalog, Designer, Studio, Runs |
| Optional Cowrie (`honeypot/`) | Cybersecurity reference pipeline |
| Optional Apache NiFi (`nifi/`) | Flow monitoring / healing lab |
| Cloudera AI Inference (optional) | LLM enrichment for honeypot / ReAct |

Platform details: [docs/PLATFORM.md](docs/PLATFORM.md).

## Repository structure

| Path | Description |
|------|-------------|
| `ratatoskr/` | CLI + Control API |
| `examples/agents/` | Agent catalog, manifest, and runners |
| `dashboard/` | React UI |
| `deploy/` | Docker Compose and image build |
| `honeypot/` | Cowrie honeypot demo |
| `nifi/` | NiFi monitoring lab |
| `docs/` | Platform and use-case guides |
| `scripts/` | Dev start/stop, cluster restart, fault injectors |
| `test/` | CLI and platform tests |

## Prerequisites

- Docker and Docker Compose v2
- Python 3.10+
- Git (image build clones `apache/flink-agents`)
- Optional: Node.js for dashboard development
- Optional honeypot / LLM: `CLOUDERA_AI_BASE_URL`, `CLOUDERA_JWT_TOKEN` in `.env`
- Optional NiFi lab: `NIFI_*` vars (see `.env.example`)

Local dev: leave `RATATOSKR_API_KEY` unset.

## Hardware requirements

| Deployment | Minimum |
|------------|---------|
| Minimal + Studio Kafka | 4 CPU, 8 GB RAM, 20 GB disk |
| NiFi profile | 6 CPU, 12 GB RAM, 30 GB disk |
| Full profile (honeypot) | 8 CPU, 16 GB RAM, 40 GB disk |

## Documentation

- [docs/PLATFORM.md](docs/PLATFORM.md) — Control API, agents, Studio, dashboard
- [docs/FLINK_AGENTS.md](docs/FLINK_AGENTS.md) — Workflow vs ReAct
- [docs/AGENT_DESIGNER_PLAN.md](docs/AGENT_DESIGNER_PLAN.md) — Agent Designer authoring and codegen
- [docs/NIFI_MONITOR.md](docs/NIFI_MONITOR.md) — NiFi monitoring / healing demos
- [docs/NIFI_RUNBOOK.md](docs/NIFI_RUNBOOK.md) — ReAct runbooks + HITL approve before heal
- [docs/CUSTOMER_POC.md](docs/CUSTOMER_POC.md) — Data-plane customer demo (propose → ack → apply)
- [docs/KAFKA_MONITOR.md](docs/KAFKA_MONITOR.md) — Kafka monitoring / healing demos
- [docs/SIGNAL_CORRELATE.md](docs/SIGNAL_CORRELATE.md) — Cross-signal correlation, scribe, cross-stack heals
- [docs/README.md](docs/README.md) — Documentation index (incl. Mermaid diagram map)
- [examples/README.md](examples/README.md) — Example agents
- [examples/agents/agent-catalog.yaml](examples/agents/agent-catalog.yaml) — Agent catalog
- [ratatoskr/README.md](ratatoskr/README.md) — CLI reference
- [dashboard/README.md](dashboard/README.md) — Dashboard, Designer, and Studio
- [honeypot/README.md](honeypot/README.md) — Honeypot demo
- [nifi/README.md](nifi/README.md) — NiFi lab
- [deploy/README.md](deploy/README.md) — Compose / image layout
- [Apache Flink Agents docs](https://nightlies.apache.org/flink/flink-agents-docs-release-0.3/)

## License

Apache License 2.0
