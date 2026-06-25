# Apemosyne Dashboard

Web UI for the **Apemosyne Control API** — monitor Flink Agents, compose pipelines, design agents visually, and inspect runs. No honeypot dependencies.

## Prerequisites

From the repo root:

```bash
pip install -e .
apemosyne build
apemosyne up                    # JobManager + TaskManager
```

In a **second terminal**, start the Control API:

```bash
apemosyne api start
```

API docs: http://127.0.0.1:8090/docs

Local dev: leave `APEMOSYNE_API_KEY` unset so all routes are open.

## Run

**One command** (Flink must already be up):

```bash
./scripts/dev-start.sh
```

Stops API + dashboard:

```bash
./scripts/dev-stop.sh
```

**Manual** (two terminals):

```bash
apemosyne api start
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

LLM settings for ReAct agents are stored server-side (`.apemosyne/designer.db`), not in dashboard env vars. Configure them under **Settings** in the UI.

## Pages

| Route | Description |
|-------|-------------|
| `/` | Overview — live health and jobs via SSE (`GET /v1/events`) |
| `/agents` | Agent catalog (registered Flink agents) |
| `/agents/:name` | Agent detail, Flink YAML, submit to cluster |
| `/designer` | Agent Designer — list user-defined agents, catalog preview |
| `/designer/:id` | Visual editor — canvas, validate, compile to Python/YAML |
| `/settings` | Platform settings — LLM connection for ReAct agents |
| `/runs` | Run history (agents + pipelines) |
| `/runs/:id` | Run detail, execution plan, spans, output |
| `/studio` | Agentic Studio — pipeline list |
| `/studio/:id` | Pipeline canvas — validate and run locally |
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
3. **Compile** generates artifacts under `.apemosyne/agents/{definition_id}/` (Python, Flink YAML, manifest snippet, local runner).

ReAct agents require an LLM. Configure endpoint, model, and API key under **Settings** before running pipelines in LLM mode.

See [docs/AGENT_DESIGNER_PLAN.md](../docs/AGENT_DESIGNER_PLAN.md) for the full roadmap.

### Agentic Studio

Compose **linear multi-agent pipelines** (Source → Agent → … → Sink):

- **Palette** — add Source, agents from the catalog, and Sink
- **Inspector** — source JSON records and edge field mappings (e.g. `{"message": "$.doubled"}`)
- **Drill-down** — double-click an agent to view its action/tool graph
- **Run locally** — executes the chain in-process; links to `/runs/:id`

Pipelines persist in `.apemosyne/pipelines.db`. Local runs require `flink_agents` (from `apemosyne build`).

### Runs

Unified history for agent submits and Studio pipeline runs. Run detail shows status, execution plan, per-agent spans, and output panels.

### Settings

Platform-wide **LLM connection** for ReAct agents:

- Endpoint URL, model ID, API key (masked on read)
- **Test connection** — validates with a double-value prompt (3 → 6)

Backed by `GET/PUT/POST /v1/designer/llm-settings` on the Control API.

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
