# Ratatoskr

<p align="center">
  <img src="dashboard/public/ratatoskr-icon.svg" alt="Ratatoskr — squirrel messenger on Yggdrasil" width="96" height="96" />
</p>

**Build, run, and verify [Apache Flink Agents](https://github.com/apache/flink-agents) on Docker — with a CLI, registered agents, and a Control API for dashboards.**

Named for the Norse squirrel that carries messages up and down **Yggdrasil** — a fit for event pipelines, Kafka, and Flink streams. See [docs/branding/RATATOSKR.md](docs/branding/RATATOSKR.md).

This repository is a **multi-project workspace** centered on the **Ratatoskr** CLI:

| Path | Description |
|------|-------------|
| [`ratatoskr/`](ratatoskr/README.md) | CLI + Control API (`ratatoskr`) |
| [`examples/`](examples/README.md) | Generic Flink Agents demos and agent registry |
| [`docs/`](docs/README.md) | Guides (workflow vs ReAct, platform API) |
| [`honeypot/`](honeypot/README.md) | Optional Cowrie honeypot reference pipeline |
| [`dashboard/`](dashboard/README.md) | Web UI — Overview, Agents, Designer, Studio, Runs, Jobs |
| [`test/`](test/README.md) | CLI and platform tests |

## Quick start (Flink Agents platform)

```bash
pip install -e .
ratatoskr build
ratatoskr up                    # minimal: JobManager + TaskManager (default)
ratatoskr kafka up              # Studio Kafka (pipeline sources/sinks on :9094)
```

Copy [`.env.example`](.env.example) to `.env` for Studio defaults:

```bash
RATATOSKR_PROFILE=minimal
FLINK_REST_PORT=8082
KAFKA_BOOTSTRAP_SERVERS=localhost:9094
RATATOSKR_API_PORT=8090
```

**After code or image updates**, restart the Studio cluster (Flink + Kafka + sync runtime into containers):

```bash
./scripts/restart-studio-cluster.sh
./scripts/restart-studio-cluster.sh --dev          # + API + dashboard
./scripts/restart-studio-cluster.sh --build --dev  # rebuild image + dev stack
./scripts/restart-studio-cluster.sh --sync-only     # hot-sync code, no container restart
```

**Terminal 2 — Control API** (for dashboard development; no API key in local dev):

```bash
ratatoskr api start
curl http://127.0.0.1:8090/v1/health
curl http://127.0.0.1:8090/v1/agents
```

**Agents:**

```bash
ratatoskr agent list
ratatoskr agent run workflow_counter --local
ratatoskr agent submit workflow_counter
ratatoskr doctor
ratatoskr verify --tier quick
```

## Dashboard (web UI)

```bash
./scripts/dev-start.sh
```

Stop: `./scripts/dev-stop.sh`

Dashboard: http://localhost:3000 — Overview, agent catalog, **Agent Designer**, **Agentic Studio**, runs, jobs, and LLM settings.

See [dashboard/README.md](dashboard/README.md) and [docs/PLATFORM.md](docs/PLATFORM.md).

| URL | Service |
|-----|---------|
| http://localhost:3000 | Dashboard (dev) |
| http://localhost:8082 | Flink Web UI (minimal / Studio stack) |
| http://localhost:8081 | Flink Web UI (honeypot / full profile) |
| http://localhost:9094 | Studio Kafka bootstrap (host) |
| http://127.0.0.1:8090/docs | Control API (Swagger) |
| http://127.0.0.1:8090/v1/health | Pipeline health |

## Quick start (optional honeypot)

```bash
ratatoskr up --profile full    # Cowrie + Kafka + dashboard
ratatoskr dashboard
```

See [honeypot/README.md](honeypot/README.md).

## Repository layout

```text
.
├── ratatoskr/                 # CLI package + api/ + agents/ + runtime/
├── examples/
│   └── agents/                # workflow_counter, react_echo + manifest
├── docs/                      # FLINK_AGENTS.md, PLATFORM.md
├── honeypot/                  # Optional Cowrie subproject
├── test/                      # Platform + CLI tests
├── docker-compose.yml         # Minimal Flink stack (default)
├── docker-compose.kafka.yml   # Studio Kafka (independent of honeypot)
├── Dockerfile                 # agent_flink_image build
├── scripts/
│   ├── dev-start.sh           # API + dashboard dev
│   ├── dev-stop.sh
│   └── restart-studio-cluster.sh  # Flink + Kafka + code sync after updates
└── README.md
```

## CLI overview

```bash
# Stack
ratatoskr build [git-ref]
ratatoskr up [--profile minimal|full] [--mode flink|platform|honeypot]
ratatoskr down
ratatoskr kafka up|down|status   # Studio Kafka (docker-compose.kafka.yml)
ratatoskr doctor

# Studio cluster (after pulling or editing runtime code)
./scripts/restart-studio-cluster.sh [--build] [--smoke] [--dev|--api|--dashboard] [--sync-only]

# Agents
ratatoskr agent list|describe|run|submit|status|cancel

# Control API
ratatoskr api start|url|openapi|check

# Demos & verify
ratatoskr demo workflow|react|datastream|table
ratatoskr verify --tier quick|standard|full|nightly
ratatoskr test launch [--cluster]
ratatoskr test validate
```

Honeypot-only tests (`phase1`–`phase3`, `production`) run when `honeypot/` is present and `--profile full` is used.

Full command reference: [ratatoskr/README.md](ratatoskr/README.md)

## Prerequisites

- Docker and Docker Compose v2
- Python 3.10+
- Git (image build clones `apache/flink-agents`)

Optional: copy [`.env.example`](.env.example) to `.env` for API/Flink overrides. **Local dev:** leave `RATATOSKR_API_KEY` unset.

## Documentation

| Doc | Description |
|-----|-------------|
| [docs/PLATFORM.md](docs/PLATFORM.md) | Control API, agents, observability, dashboard integration |
| [dashboard/README.md](dashboard/README.md) | Dashboard pages, dev setup, project structure |
| [docs/AGENT_DESIGNER_PLAN.md](docs/AGENT_DESIGNER_PLAN.md) | Visual agent authoring and codegen roadmap |
| [docs/FLINK_AGENTS.md](docs/FLINK_AGENTS.md) | Workflow vs ReAct agents |
| [ratatoskr/README.md](ratatoskr/README.md) | CLI package layout |
| [examples/README.md](examples/README.md) | Example agents and demos |
| [honeypot/README.md](honeypot/README.md) | Cowrie cybersecurity demo (optional) |

## License

Apache License 2.0
