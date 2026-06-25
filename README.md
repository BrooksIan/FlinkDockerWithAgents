# Flink Agents CLI

**Build, run, and verify [Apache Flink Agents](https://github.com/apache/flink-agents) workflows on Docker — with pluggable subprojects.**

This repository is a **multi-project workspace**:

| Path | Description |
|------|-------------|
| [`flink_cowrie/`](flink_cowrie/README.md) | Shared CLI (`flink-cowrie`) — build, compose, demos, verify |
| [`examples/`](examples/README.md) | Generic Flink Agents demos (no honeypot deps) |
| [`honeypot/`](honeypot/README.md) | Cowrie honeypot cybersecurity pipeline (reference subproject) |
| [`test/`](test/README.md) | CLI and cross-cutting tests |
| [`docs/`](docs/README.md) | Additional guides |

Future subprojects can add their own directory with `docker-compose.yml`, `manifests/`, `src/`, and `docs/` while reusing the same CLI.

## Quick start (Flink only)

```bash
pip install -e .
flink-cowrie build
flink-cowrie up                    # minimal: JobManager + TaskManager
flink-cowrie demo datastream
flink-cowrie test launch
```

Flink Web UI: http://localhost:8081

## Quick start (honeypot)

```bash
pip install -e .
flink-cowrie build
flink-cowrie up --profile full    # Cowrie + Kafka + dashboard + pipeline
flink-cowrie dashboard
```

See [honeypot/README.md](honeypot/README.md).

![Honeypot reference architecture](honeypot/docs/images/PrettyRASlide.png)

## Repository layout

```text
.
├── flink_cowrie/              # CLI package
├── examples/                  # Generic demos
├── honeypot/                  # Honeypot subproject bundle
│   ├── src/                   # Pipeline modules (core, pipeline, traps, react, …)
│   ├── demo/                  # Cowrie-specific demos
│   ├── dashboard/             # Streamlit UI
│   ├── simulate_attack/       # Synthetic attack injection
│   └── test/                  # Honeypot tests
├── test/                      # CLI / shared tests
├── scripts/                   # Shell helpers
├── docker-compose.yml         # Minimal Flink stack (when present)
├── docker-compose-cowrie.yml  # Full honeypot stack (legacy path at repo root)
├── Dockerfile
└── README.md
```

## CLI overview

```bash
flink-cowrie build [git-ref]     # Build agent_flink_image:latest
flink-cowrie up [--profile full|minimal]
flink-cowrie down
flink-cowrie doctor [--fix]
flink-cowrie demo <name>
flink-cowrie verify --tier quick|standard|full|nightly
flink-cowrie test launch [--cluster]
flink-cowrie test phase1|phase2|phase3|production [--e2e]
```

Full command reference: [flink_cowrie/README.md](flink_cowrie/README.md)

## Prerequisites

- Docker and Docker Compose v2
- Python 3.10+
- Git (image build clones `apache/flink-agents`)

Copy `.env.example` to `.env` for optional Cloudera LLM (Phase 3 ReAct).

## Documentation

- [flink_cowrie/README.md](flink_cowrie/README.md) — CLI development
- [honeypot/README.md](honeypot/README.md) — cybersecurity demo
- [examples/README.md](examples/README.md) — generic demos
- [docs/README.md](docs/README.md) — guides index
- [docs/FLINK_AGENTS.md](docs/FLINK_AGENTS.md) — workflow vs ReAct agents

## License

Apache License 2.0
