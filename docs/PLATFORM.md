# Ratatoskr platform — Flink Agents control plane

This document describes the **generic Flink Agents platform** in this workspace: CLI lifecycle, registered agents, the **Control API**, and the [dashboard](../dashboard/README.md). It does not cover the optional [honeypot](../honeypot/README.md) subproject.

## Architecture

```text
Developer / Dashboard
        │
        ▼
  Ratatoskr CLI ──────────────┐
  (ratatoskr agent …)         │
        │                     ▼
        │              Control API :8090
        │              (FastAPI, optional)
        ▼                     │
  Docker Compose              │ reads
  JobManager + TaskManager    ▼
        │              Flink REST :8082 (minimal) / :8081 (honeypot)
        ▼
  Flink Agents jobs
  (workflow / ReAct examples)
```

| Layer | Role |
|-------|------|
| **CLI** | Build image, start stack, run/submit agents, verify, doctor |
| **Control API** | HTTP surface for dashboards (`/v1/agents`, `/v1/jobs`, …) |
| **Flink cluster** | JobManager + TaskManager (`docker-compose.yml`) |
| **Agent registry** | `examples/agents/agent-manifest.yaml` |

## Quick start (local dev)

No API key required for local development.

```bash
pip install -e .
ratatoskr build
ratatoskr up                    # default: minimal Flink stack
ratatoskr kafka up              # Studio Kafka for pipeline sources/sinks
```

Recommended `.env` for Studio (copy from [`.env.example`](../.env.example)):

```bash
RATATOSKR_PROFILE=minimal
FLINK_REST_PORT=8082
KAFKA_BOOTSTRAP_SERVERS=localhost:9094
```

After editing runtime code or the Dockerfile:

```bash
./scripts/restart-studio-cluster.sh [--build] [--api] [--smoke]
```

**Terminal 2 — Control API:**

```bash
ratatoskr api start
```

**Try it:**

```bash
curl http://127.0.0.1:8090/v1/health
curl http://127.0.0.1:8090/v1/agents
open http://127.0.0.1:8090/docs    # Swagger UI
```

**Run agents:**

```bash
ratatoskr agent list
ratatoskr agent run workflow_counter --local
ratatoskr agent submit workflow_counter
ratatoskr agent status
```

Flink Web UI (minimal): http://localhost:8082

### Startup modes

Presets in `ratatoskr/manifests/startup-modes.yaml`:

| Mode | Command | Stack |
|------|---------|-------|
| `flink` (default) | `ratatoskr up` | Minimal JM + TM |
| `platform` | `ratatoskr up --mode platform` | Same + documents API URL |
| `honeypot` | `ratatoskr up --mode honeypot` | Full Cowrie pipeline (optional) |

## Control API

### Endpoints

| Method | Path | Auth when key set | Description |
|--------|------|-------------------|-------------|
| `GET` | `/v1/health` | No | Pipeline health (API + Flink + agent count) |
| `GET` | `/v1/pipeline/health` | No | Same as `/v1/health` |
| `GET` | `/v1/cluster/overview` | Yes | Flink cluster overview |
| `GET` | `/v1/jobs` | Yes | List Flink jobs |
| `GET` | `/v1/jobs/{id}` | Yes | Job detail |
| `DELETE` | `/v1/jobs/{id}` | Yes | Cancel job |
| `GET` | `/v1/agents` | Yes | List registered agents |
| `GET` | `/v1/agents/{name}` | Yes | Agent metadata |
| `GET` | `/v1/agents/{name}/definition` | Yes | Catalog + Flink YAML content |
| `GET` | `/v1/agents/{name}/graph` | Yes | Internal action/tool graph for Studio drill-down |
| `GET` | `/v1/agents/catalog` | Yes | Agent catalog (categories, I/O schemas) |
| `POST` | `/v1/agents/{name}/submit` | Yes | Submit agent to cluster |
| `GET` | `/v1/designer/llm-settings` | Yes | ReAct LLM settings (key masked) |
| `PUT` | `/v1/designer/llm-settings` | Yes | Update LLM endpoint, model, API key |
| `POST` | `/v1/designer/llm-settings/test` | Yes | Test LLM with double-value prompt |
| `GET` | `/v1/agent-definitions` | Yes | List designer agent definitions |
| `POST` | `/v1/agent-definitions` | Yes | Create definition |
| `GET` | `/v1/agent-definitions/{id}` | Yes | Get designer definition |
| `PUT` | `/v1/agent-definitions/{id}` | Yes | Update definition graph |
| `DELETE` | `/v1/agent-definitions/{id}` | Yes | Delete definition |
| `POST` | `/v1/agent-definitions/{id}/validate` | Yes | Validate definition graph |
| `POST` | `/v1/agent-definitions/{id}/compile` | Yes | Generate Python + YAML artifacts |
| `GET` | `/v1/runs` | Yes | List agent/pipeline runs |
| `GET` | `/v1/runs/{id}` | Yes | Run detail + spans |
| `GET` | `/v1/pipelines` | Yes | List composed pipelines |
| `POST` | `/v1/pipelines` | Yes | Create pipeline |
| `GET` | `/v1/pipelines/{id}` | Yes | Pipeline graph + layout |
| `PUT` | `/v1/pipelines/{id}` | Yes | Save canvas state |
| `DELETE` | `/v1/pipelines/{id}` | Yes | Delete pipeline |
| `POST` | `/v1/pipelines/{id}/validate` | Yes | Validate linear pipeline (local + cluster) |
| `POST` | `/v1/pipelines/{id}/run` | Yes | Run pipeline locally |
| `POST` | `/v1/pipelines/{id}/submit` | Yes | Submit batch pipeline to minimal Flink cluster |
| `GET` | `/v1/kafka/topics` | Yes | List known / discoverable Kafka topics |
| `GET` | `/v1/cluster/status` | Yes | Studio Flink + Kafka readiness checks |
| `POST` | `/v1/cluster/validate` | Yes | Re-run cluster readiness checks |
| `GET` | `/v1/events` | No | SSE health + job snapshots |
| `GET` | `/metrics` | No | Prometheus metrics |
| `GET` | `/openapi.json` | No | OpenAPI schema (codegen for dashboard) |
| `GET` | `/docs` | No | Swagger UI |

### CLI

```bash
ratatoskr api start              # uvicorn on :8090
ratatoskr api url
ratatoskr api openapi -o openapi.json
ratatoskr api check              # probe /v1/health
```

### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `RATATOSKR_API_HOST` | `127.0.0.1` | API bind address |
| `RATATOSKR_API_PORT` | `8090` | API port |
| `RATATOSKR_API_KEY` | *(unset)* | Shared secret; when set, protected routes need `X-API-Key` header |
| `FLINK_REST_ADDRESS` | `localhost` | Flink JobManager host for API/CLI |
| `FLINK_REST_PORT` | `8082` (minimal) / `8081` (full) | Host Flink REST / Web UI port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | Studio Kafka (`ratatoskr kafka up`) |
| `RATATOSKR_PROFILE` | `minimal` | Compose profile for agent/pipeline cluster submit |
| `RATATOSKR_LOG_JSON` | `0` | `1` = structured JSON logs from API |

**Local dev:** leave `RATATOSKR_API_KEY` unset. All routes are open.

**Exposed / shared API:** set a long random value and pass it on every protected request:

```bash
export RATATOSKR_API_KEY="your-secret"
ratatoskr api start

curl -H "X-API-Key: your-secret" http://127.0.0.1:8090/v1/agents
```

Wrong or missing key → `401 Invalid or missing API key`.

## Agent registry

Agents are declared in [`examples/agents/agent-manifest.yaml`](../examples/agents/agent-manifest.yaml):

| Agent | Type | Description |
|-------|------|-------------|
| `workflow_counter` | workflow | Deterministic `@action` + `@tool` — doubles integers |
| `react_echo` | react | Tool-chaining lab agent (no LLM) |
| `react_double_value` | react | ReAct agent that doubles values via LLM (requires Settings LLM) |
| `react_skills_demo` | react | Native Flink chat model + math-calculator skill (requires Settings LLM) |

```bash
ratatoskr agent list
ratatoskr agent describe workflow_counter
ratatoskr agent run workflow_counter --local
ratatoskr agent run workflow_counter --cluster
ratatoskr agent submit workflow_counter
ratatoskr agent cancel <job-id>
```

To add an agent:

1. Add `examples/agents/my_agent.py` (Flink Agents `Agent` subclass).
2. Add local/cluster runner scripts.
3. Register in `agent-manifest.yaml` (optional `flink_yaml:` for Flink Agents YAML definition).
4. Optionally add a demo entry in `ratatoskr/manifests/demo-files.yaml`.

## Observability

| Signal | How |
|--------|-----|
| Health | `GET /v1/health` or `ratatoskr doctor` |
| Prometheus | `GET /metrics` (`ratatoskr_flink_reachable`, request counters, …) |
| Flink UI (minimal) | http://localhost:8082 |
| Flink UI (honeypot) | http://localhost:8081 |
| Verify | `ratatoskr verify --tier quick\|standard\|full` |

`ratatoskr doctor` checks agent manifest, Docker (warn), Flink REST, and API settings. Warnings for missing image/containers/API key are normal in local dev.

## Compose profiles

| Profile | File | Services |
|---------|------|----------|
| `minimal` (default) | `docker-compose.yml` | JobManager + TaskManager |
| `kafka` | `docker-compose.kafka.yml` | Studio Zookeeper + Kafka (`ratatoskr kafka up`) |
| `full` | `honeypot/docker-compose.yml` | Cowrie + Kafka + pipeline + dashboard |

```bash
ratatoskr up                  # minimal (default)
ratatoskr kafka up            # Studio Kafka (independent of honeypot)
ratatoskr up --profile full   # honeypot (optional)
ratatoskr down
ratatoskr status
```

### Studio cluster restart

Use after pulling changes or editing `ratatoskr/` pipeline/runtime code:

```bash
./scripts/restart-studio-cluster.sh              # Flink + Kafka + sync code + bootstrap JARs
./scripts/restart-studio-cluster.sh --build      # rebuild agent_flink_image
./scripts/restart-studio-cluster.sh --sync-only  # hot-sync without restarting containers
./scripts/restart-studio-cluster.sh --smoke      # + cluster launch smoke job
```

Implementation: [`ratatoskr/runtime/studio_cluster_sync.py`](../ratatoskr/runtime/studio_cluster_sync.py) copies runtime modules into JobManager and TaskManager, then runs `bootstrap_cluster_containers()` (Flink Agents thin JAR layout for Pemja).

## Verification tiers

Defined in `ratatoskr/manifests/verify-tiers.yaml`:

| Tier | Includes |
|------|----------|
| `quick` | Workspace smoke + agent registry (no Docker) |
| `standard` | + Control API tests + platform doctor |
| `full` | + Docker image present |
| `nightly` | Extended repeat checks |

```bash
ratatoskr verify --tier quick
ratatoskr test validate        # file layout only
ratatoskr test launch          # Flink Agents import smoke
ratatoskr test launch --cluster
```

Honeypot tests (`phase1`, `phase2`, `production`, …) require `honeypot/` and `--profile full`.

## Dashboard

Web UI in [`dashboard/`](../dashboard/README.md) — React + Vite, talks to Control API only.

```bash
ratatoskr up
./scripts/dev-start.sh          # API :8090 + dashboard :3000
```

Or manually: `ratatoskr api start` then `cd dashboard && npm run dev`.

| Route | Description |
|-------|-------------|
| `/` | Overview (live via `GET /v1/events` SSE) |
| `/agents` | Registered agent catalog |
| `/agents/:name` | Detail, Flink YAML, submit |
| `/designer` | Agent Designer — user definitions + catalog preview |
| `/designer/:id` | Visual editor — validate, compile to Python/YAML |
| `/settings` | LLM connection for ReAct agents |
| `/runs` | Agent and pipeline run history |
| `/runs/:id` | Run detail, execution plan, spans |
| `/studio` | **Agentic Studio** — pipeline list |
| `/studio/:id` | Drag-and-drop canvas, validate, run locally, submit to cluster |
| `/jobs` | Flink jobs, cancel |

Full page reference: [dashboard/README.md](../dashboard/README.md).

### Agent Designer

Visual editor for **workflow** and **ReAct** agents. Definitions persist in `.ratatoskr/designer.db`; compiled artifacts go to `.ratatoskr/agents/{definition_id}/`.

- **Runtime vs designer:** `GET /v1/agents/{name}/definition` returns manifest Flink YAML for registered agents. `GET /v1/agent-definitions/{id}` returns the designer graph — different stores, different IDs.
- **Compile:** `POST /v1/agent-definitions/{id}/compile` generates Python modules, Flink YAML, manifest snippet, and a local runner.
- **LLM:** ReAct agents use platform settings from `/v1/designer/llm-settings` (configured in dashboard **Settings**).

Roadmap: [AGENT_DESIGNER_PLAN.md](AGENT_DESIGNER_PLAN.md).

### Agentic Studio

Compose **linear multi-agent pipelines** visually (Source → Agent → … → Sink):

1. Open **Studio** in the dashboard sidebar.
2. Create a pipeline (demo template: `workflow_counter` → `react_echo` with edge mapping).
3. Connect nodes left-to-right; configure source records, Kafka topics, and edge field mapping in the inspector.
4. **Validate**, then **Run locally** or **Run on Flink cluster** — creates a run with per-agent spans on `/runs/:id`.
5. Double-click an agent node to view its internal action/tool graph (read-only).

Pipelines persist in `.ratatoskr/pipelines.db`. Cluster submit targets the **minimal** Flink stack on host port **8082** (not honeypot `:8081`).

| Capability | Local run | Cluster submit |
|------------|-----------|----------------|
| Static source records | Yes | Yes (batch) |
| Kafka source | Yes (sample) | Not yet |
| Capture sink | Yes | Yes (`print`) |
| Kafka sink | Yes | Yes (Flink Agents sink agent; default topic `workflow.test.output`) |
| Published ReAct agents | Yes | Warn-only (Pemja unreliable on cluster) |

**Prerequisites for cluster submit:** `ratatoskr up`, `ratatoskr kafka up`, `./scripts/restart-studio-cluster.sh` after code updates. Check **Settings → Cluster readiness** in the dashboard.

Codegen writes `.ratatoskr/pipelines/{id}/run_cluster.py`; submit copies artifacts to JobManager and runs `flink run`.

**OpenAPI client codegen:**

```bash
./scripts/generate_api_client.sh
```

**Flink YAML pilot:** `workflow_counter` includes `flink_yaml: examples/agents/workflow_counter/agent.yaml` ([upstream YAML API](https://nightlies.apache.org/flink/flink-agents-docs-main/docs/development/yaml/)). Cluster submit via `load_yaml` requires Flink Agents 0.3+; catalog + definition API work today.

## Dashboard integration (API consumers)

1. Run `ratatoskr api start` (and `ratatoskr up` for Flink).
2. Export OpenAPI: `ratatoskr api openapi -o openapi.json`.
3. Generate a client (TypeScript, etc.) from `/openapi.json`.
4. Poll `GET /v1/health` for status; use `GET /v1/agents` and `POST /v1/agents/{name}/submit` for operations.
5. Add `X-API-Key` when you deploy beyond localhost.

## See also

- [../dashboard/README.md](../dashboard/README.md) — dashboard pages, dev setup, project layout
- [AGENT_DESIGNER_PLAN.md](AGENT_DESIGNER_PLAN.md) — Agent Designer phases and API
- [FLINK_AGENTS.md](FLINK_AGENTS.md) — workflow vs ReAct concepts
- [../examples/README.md](../examples/README.md) — example agents and demos
- [../ratatoskr/README.md](../ratatoskr/README.md) — CLI package layout
- [../honeypot/README.md](../honeypot/README.md) — optional Cowrie reference pipeline
