# Apemosyne Dashboard

Web UI for the Flink Agents **Control API** — overview, agents, jobs. No honeypot dependencies.

## Prerequisites

```bash
pip install -e .
apemosyne up
```

In a **second terminal**:

```bash
apemosyne api start
```

API docs: http://127.0.0.1:8090/docs

Local dev: leave `APEMOSYNE_API_KEY` unset.

## Run

**One command** (from repo root; Flink must already be up):

```bash
./scripts/dev-start.sh
```

Stop API + dashboard:

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

Vite proxies `/v1` and `/metrics` to the API on `:8090`.

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `VITE_API_BASE_URL` | `""` (use proxy) | API base when not using Vite proxy |
| `VITE_API_KEY` | unset | `X-API-Key` when API auth enabled |

## Generate API client

```bash
npm run generate-api
# or from repo root:
./scripts/generate_api_client.sh
```

## Build

```bash
npm run build
npm run preview
```

## Pages

| Route | Description |
|-------|-------------|
| `/` | Overview (SSE live health + jobs) |
| `/agents` | Agent catalog |
| `/agents/:name` | Detail, YAML, submit |
| `/jobs` | Flink jobs list |
| `/jobs/:id` | Job detail, cancel, Flink UI link |

See [docs/PLATFORM.md](../docs/PLATFORM.md).
