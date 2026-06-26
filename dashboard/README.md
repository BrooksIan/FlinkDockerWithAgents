# Ratatoskr Dashboard

Web UI for the **Ratatoskr Control API** — monitor Flink Agents, compose pipelines, design agents visually, and inspect runs. No honeypot dependencies.

## Prerequisites

From the repo root:

```bash
pip install -e .
ratatoskr build
ratatoskr up                    # JobManager + TaskManager (Flink UI :8082)
ratatoskr kafka up              # Studio Kafka (:9094) — for pipeline Kafka sinks
```

Copy [`.env.example`](../.env.example) to `.env` with `RATATOSKR_PROFILE=minimal`, `FLINK_REST_PORT=8082`, and `KAFKA_BOOTSTRAP_SERVERS=localhost:9094`.

After pulling changes or editing cluster runtime code:

```bash
./scripts/restart-studio-cluster.sh --dev   # Flink + Kafka + API + dashboard
```

Or restart only dev services (cluster already running):

```bash
./scripts/restart-studio-cluster.sh --sync-only --dev
```

API docs: http://127.0.0.1:8090/docs  
Dashboard: http://localhost:3000

Local dev: leave `RATATOSKR_API_KEY` unset so all routes are open.

## Run

**One command** (Flink must already be up; foreground API + dashboard):

```bash
./scripts/dev-start.sh
```

**Background** (after cluster restart):

```bash
./scripts/restart-studio-cluster.sh --dev
```

Stops API + dashboard:

```bash
./scripts/dev-stop.sh
```

**Manual** (two terminals):

```bash
ratatoskr api start
```

```bash
cd dashboard
npm install
npm run dev
```

Open http://localhost:3000

Vite proxies `/v1`, `/metrics`, and `/openapi.json` to the API on `:8090`.

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `VITE_API_BASE_URL` | `""` (use proxy) | API base when not using the Vite dev proxy |
| `VITE_API_KEY` | unset | `X-API-Key` header when API auth is enabled |

LLM settings for ReAct agents are stored server-side (`.ratatoskr/designer.db`), not in dashboard env vars. Configure them under **Settings** in the UI.

## Pages

| Route | Description |
|-------|-------------|
| `/` | Overview — live health and jobs via SSE (`GET /v1/events`) |
| `/agents` | Agent catalog (registered Flink agents) |
| `/agents/:name` | Agent detail, Flink YAML, submit to cluster |
| `/designer` | Agent Designer — list user-defined agents, catalog preview |
| `/designer/:id` | Visual editor — canvas, validate, compile to Python/YAML |
| `/settings` | LLM connection + Flink cluster readiness (Studio stack on :8082) |
| `/runs` | Run history (agents + pipelines) |
| `/runs/:id` | Run detail, execution plan, spans, output |
| `/studio` | Agentic Studio — pipeline list |
| `/studio/:id` | Pipeline canvas — validate, run locally, **run on Flink cluster** |
| `/jobs` | Flink jobs list |
| `/jobs/:id` | Job detail, cancel, link to Flink UI |

## Features

### Overview

Live pipeline health and recent Flink jobs. Uses Server-Sent Events so the page updates without polling.

### Agents

Browse agents from `examples/agents/agent-manifest.yaml`. Each detail page shows metadata, Flink YAML (when available), and a **Submit** action that starts a cluster job.

Runtime definitions come from `GET /v1/agents/{name}/definition`. This is separate from designer definitions (`GET /v1/agent-definitions/{id}`).

### Agent Designer

Build **workflow** and **ReAct** agents on a [React Flow](https://reactflow.dev/) canvas without hand-editing Python:

1. Open **Designer** → **Create agent** (Workflow or ReAct template).
2. Edit nodes and edges on `/designer/:id` — auto-save, validate, compile.
3. **Compile** generates artifacts under `.ratatoskr/agents/{definition_id}/` (Python, Flink YAML, manifest snippet, local runner).

ReAct agents require an LLM. Configure endpoint, model, and API key under **Settings** before running pipelines in LLM mode.

See [docs/AGENT_DESIGNER_PLAN.md](../docs/AGENT_DESIGNER_PLAN.md) for the full roadmap.

### Agentic Studio

Compose **linear multi-agent pipelines** (Source → Agent → … → Sink):

- **Palette** — Source (records or Kafka), agents from the catalog, Capture sink or **Kafka sink**
- **Inspector** — source records, Kafka topic, edge field mappings (e.g. `{"message": "$.doubled"}`)
- **Drill-down** — double-click an agent to view its internal action/tool graph
- **Run locally** — in-process execution; links to `/runs/:id`
- **Run on Flink cluster** — batch submit to minimal stack (`:8082`); Kafka sink default topic `workflow.test.output`

Pipelines persist in `.ratatoskr/pipelines.db`. Cluster runs need Studio Kafka (`ratatoskr kafka up`) and a healthy minimal Flink stack — use `./scripts/restart-studio-cluster.sh` after code updates.

Published ReAct agents show a **cluster warning** (Pemja classloader); prefer built-in workflow agents or local run for designer ReAct pipelines.

### Runs

Unified history for agent submits and Studio pipeline runs. Run detail shows status, execution plan, per-agent spans, and output panels.

### Settings

Platform-wide **LLM connection** for ReAct agents and **Flink cluster readiness** for Studio:

- Endpoint URL, model ID, API key (masked on read)
- **Test connection** — validates with a double-value prompt (3 → 6)
- **Cluster panel** — Docker image, JobManager/TaskManager, TaskManager slots, Studio Kafka (`:9094`)

Backed by `GET/PUT/POST /v1/designer/llm-settings` and `GET /v1/cluster/status` on the Control API.

### Jobs

Flink job list with cancel and a link to the Flink Web UI (port `8082` minimal stack, `8081` honeypot profile).

## Project structure

```text
dashboard/
├── src/
│   ├── api/              # HTTP client + TypeScript types
│   ├── components/       # Shared UI (Layout, badges, LLM settings, …)
│   ├── designer/       # Agent Designer canvas, palette, inspector
│   ├── hooks/            # SSE event stream, Flink URL
│   ├── pages/            # Route pages
│   ├── studio/           # Agentic Studio pipeline canvas
│   ├── utils/            # Run helpers
│   ├── main.tsx          # Router
│   └── styles.css
├── index.html
├── vite.config.ts        # Dev proxy → :8090
└── package.json
```

## API client

The dashboard uses a hand-written client in `src/api/client.ts`. Regenerate types from OpenAPI when the API changes:

```bash
npm run generate-api
# or from repo root:
./scripts/generate_api_client.sh
```

Key client methods:

| Method | API | Use |
|--------|-----|-----|
| `agentRuntimeDefinition(name)` | `GET /v1/agents/{name}/definition` | Catalog Flink YAML |
| `getDesignerDefinition(id)` | `GET /v1/agent-definitions/{id}` | Designer store |
| `compileAgentDefinition(id)` | `POST /v1/agent-definitions/{id}/compile` | Codegen |
| `runPipeline(id)` | `POST /v1/pipelines/{id}/run` | Studio local run |
| `submitPipeline(id)` | `POST /v1/pipelines/{id}/submit` | Studio cluster submit |
| `validatePipeline(id)` | `POST /v1/pipelines/{id}/validate` | Local + cluster validation |

## Build

```bash
npm run build
npm run preview
```

Production builds need `VITE_API_BASE_URL` pointing at the deployed Control API if not served from the same origin.

## See also

| Doc | Description |
|-----|-------------|
| [docs/PLATFORM.md](../docs/PLATFORM.md) | Control API, agents, observability |
| [docs/AGENT_DESIGNER_PLAN.md](../docs/AGENT_DESIGNER_PLAN.md) | Agent Designer phases and API |
| [docs/FLINK_AGENTS.md](../docs/FLINK_AGENTS.md) | Workflow vs ReAct concepts |
| [../README.md](../README.md) | Workspace quick start |
