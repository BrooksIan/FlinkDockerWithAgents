# Apemosyne platform — Flink Agents control plane

This document describes the **generic Flink Agents platform** in this workspace: CLI lifecycle, registered agents, and the **Control API** for a future dashboard. It does not cover the optional [honeypot](../honeypot/README.md) subproject.

## Architecture

```text
Developer / Dashboard
        │
        ▼
  Apemosyne CLI ──────────────┐
  (apemosyne agent …)         │
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
apemosyne build
apemosyne up                    # default: minimal Flink stack
```

**Terminal 2 — Control API:**

```bash
apemosyne api start
```

**Try it:**

```bash
curl http://127.0.0.1:8090/v1/health
curl http://127.0.0.1:8090/v1/agents
open http://127.0.0.1:8090/docs    # Swagger UI
```

**Run agents:**

```bash
apemosyne agent list
apemosyne agent run workflow_counter --local
apemosyne agent submit workflow_counter
apemosyne agent status
```

Flink Web UI (minimal): http://localhost:8082

### Startup modes

Presets in `apemosyne/manifests/startup-modes.yaml`:

| Mode | Command | Stack |
|------|---------|-------|
| `flink` (default) | `apemosyne up` | Minimal JM + TM |
| `platform` | `apemosyne up --mode platform` | Same + documents API URL |
| `honeypot` | `apemosyne up --mode honeypot` | Full Cowrie pipeline (optional) |

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
| `POST` | `/v1/agents/{name}/submit` | Yes | Submit agent to cluster |
| `GET` | `/v1/events` | No | SSE health + job snapshots |
| `GET` | `/metrics` | No | Prometheus metrics |
| `GET` | `/openapi.json` | No | OpenAPI schema (codegen for dashboard) |
| `GET` | `/docs` | No | Swagger UI |

### CLI

```bash
apemosyne api start              # uvicorn on :8090
apemosyne api url
apemosyne api openapi -o openapi.json
apemosyne api check              # probe /v1/health
```

### Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `APEMOSYNE_API_HOST` | `127.0.0.1` | API bind address |
| `APEMOSYNE_API_PORT` | `8090` | API port |
| `APEMOSYNE_API_KEY` | *(unset)* | Shared secret; when set, protected routes need `X-API-Key` header |
| `FLINK_REST_ADDRESS` | `localhost` | Flink JobManager host for API/CLI |
| `FLINK_REST_PORT` | `8082` (minimal) / `8081` (full) | Host Flink REST / Web UI port |
| `APEMOSYNE_PROFILE` | `minimal` | Compose profile for agent submit |
| `APEMOSYNE_LOG_JSON` | `0` | `1` = structured JSON logs from API |

**Local dev:** leave `APEMOSYNE_API_KEY` unset. All routes are open.

**Exposed / shared API:** set a long random value and pass it on every protected request:

```bash
export APEMOSYNE_API_KEY="your-secret"
apemosyne api start

curl -H "X-API-Key: your-secret" http://127.0.0.1:8090/v1/agents
```

Wrong or missing key → `401 Invalid or missing API key`.

## Agent registry

Agents are declared in [`examples/agents/agent-manifest.yaml`](../examples/agents/agent-manifest.yaml):

| Agent | Type | Description |
|-------|------|-------------|
| `workflow_counter` | workflow | Deterministic `@action` + `@tool` — doubles integers |
| `react_echo` | react | Tool-chaining lab agent (no LLM) |

```bash
apemosyne agent list
apemosyne agent describe workflow_counter
apemosyne agent run workflow_counter --local
apemosyne agent run workflow_counter --cluster
apemosyne agent submit workflow_counter
apemosyne agent cancel <job-id>
```

To add an agent:

1. Add `examples/agents/my_agent.py` (Flink Agents `Agent` subclass).
2. Add local/cluster runner scripts.
3. Register in `agent-manifest.yaml` (optional `flink_yaml:` for Flink Agents YAML definition).
4. Optionally add a demo entry in `apemosyne/manifests/demo-files.yaml`.

## Observability

| Signal | How |
|--------|-----|
| Health | `GET /v1/health` or `apemosyne doctor` |
| Prometheus | `GET /metrics` (`apemosyne_flink_reachable`, request counters, …) |
| Flink UI (minimal) | http://localhost:8082 |
| Flink UI (honeypot) | http://localhost:8081 |
| Verify | `apemosyne verify --tier quick\|standard\|full` |

`apemosyne doctor` checks agent manifest, Docker (warn), Flink REST, and API settings. Warnings for missing image/containers/API key are normal in local dev.

## Compose profiles

| Profile | File | Services |
|---------|------|----------|
| `minimal` (default) | `docker-compose.yml` | JobManager + TaskManager |
| `full` | `honeypot/docker-compose.yml` | Cowrie + Kafka + pipeline + dashboard |

```bash
apemosyne up                  # minimal (default)
apemosyne up --profile full   # honeypot (optional)
apemosyne down
apemosyne status
```

## Verification tiers

Defined in `apemosyne/manifests/verify-tiers.yaml`:

| Tier | Includes |
|------|----------|
| `quick` | Workspace smoke + agent registry (no Docker) |
| `standard` | + Control API tests + platform doctor |
| `full` | + Docker image present |
| `nightly` | Extended repeat checks |

```bash
apemosyne verify --tier quick
apemosyne test validate        # file layout only
apemosyne test launch          # Flink Agents import smoke
apemosyne test launch --cluster
```

Honeypot tests (`phase1`, `phase2`, `production`, …) require `honeypot/` and `--profile full`.

## Dashboard

Web UI in [`dashboard/`](../dashboard/) — React + Vite, talks to Control API only.

```bash
apemosyne up
apemosyne api start
```

Control API: http://127.0.0.1:8090

```bash
cd dashboard && npm install && npm run dev
```

Dashboard: http://localhost:3000

| Route | Description |
|-------|-------------|
| `/` | Overview (live via `GET /v1/events` SSE) |
| `/agents` | Agent catalog |
| `/agents/:name` | Detail, Flink YAML, submit |
| `/jobs` | Flink jobs, cancel |

**OpenAPI client codegen:**

```bash
./scripts/generate_api_client.sh
```

**Flink YAML pilot:** `workflow_counter` includes `flink_yaml: examples/agents/workflow_counter/agent.yaml` ([upstream YAML API](https://nightlies.apache.org/flink/flink-agents-docs-main/docs/development/yaml/)). Cluster submit via `load_yaml` requires Flink Agents 0.3+; catalog + definition API work today.

## Dashboard integration (API consumers)

1. Run `apemosyne api start` (and `apemosyne up` for Flink).
2. Export OpenAPI: `apemosyne api openapi -o openapi.json`.
3. Generate a client (TypeScript, etc.) from `/openapi.json`.
4. Poll `GET /v1/health` for status; use `GET /v1/agents` and `POST /v1/agents/{name}/submit` for operations.
5. Add `X-API-Key` when you deploy beyond localhost.

## See also

- [FLINK_AGENTS.md](FLINK_AGENTS.md) — workflow vs ReAct concepts
- [../examples/README.md](../examples/README.md) — example agents and demos
- [../apemosyne/README.md](../apemosyne/README.md) — CLI package layout
- [../honeypot/README.md](../honeypot/README.md) — optional Cowrie reference pipeline
