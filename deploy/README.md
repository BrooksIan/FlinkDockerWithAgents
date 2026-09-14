# Deploy

Docker deployment configs for the Ratatoskr Flink Agents stack.

| File | Description |
| --- | --- |
| `Dockerfile` | Builds `agent_flink_image` (PyFlink + Flink Agents). Prefer `ratatoskr build`. |
| `docker-compose.yml` | Minimal JobManager + TaskManager (`ratatoskr up`) |
| `docker-compose.kafka.yml` | Studio Kafka (`ratatoskr kafka up`) |
| `docker-compose-cowrie.yml` | Deprecated pointer → `honeypot/docker-compose.yml` |

NiFi lab compose lives under [`nifi/docker-compose.yml`](../nifi/docker-compose.yml) and is stacked with this minimal file when using `ratatoskr up --profile nifi`.

**Not Dockerized here:** Control API and dashboard run on the host (`ratatoskr api start`, Vite). Agents can run on the host (`--local`) or inside the Flink image (cluster submit).

For **CDP Private Cloud Base**, **Knox VIP**, and hybrid setups (what changes vs this Compose lab), see [docs/DEPLOYMENT_SCENARIOS.md](../docs/DEPLOYMENT_SCENARIOS.md).

| Guide | Description |
|-------|-------------|
| [docs/DEPLOYMENT_SCENARIOS.md](../docs/DEPLOYMENT_SCENARIOS.md) | Docker vs CDP Base / Knox — env and test checklist |
| [docs/KAFKA_MONITOR.md](../docs/KAFKA_MONITOR.md) | Studio Kafka monitor / heal (architecture Mermaid) |
| [nifi/README.md](../nifi/README.md) | NiFi lab quickstart + diagrams |
| [docs/SIGNAL_CORRELATE.md](../docs/SIGNAL_CORRELATE.md) | Cross-signal on shared Kafka→NiFi demo |
| [docs/CM_MONITOR.md](../docs/CM_MONITOR.md) | CM via Knox (recommend-only) |

Honeypot / full profile compose remains under [`honeypot/docker-compose.yml`](../honeypot/docker-compose.yml).
