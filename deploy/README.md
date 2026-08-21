# Deploy

Docker deployment configs for the Ratatoskr Flink Agents stack.

| File | Description |
| --- | --- |
| `Dockerfile` | Builds `agent_flink_image` (PyFlink + Flink Agents). Prefer `ratatoskr build`. |
| `docker-compose.yml` | Minimal JobManager + TaskManager (`ratatoskr up`) |
| `docker-compose.kafka.yml` | Studio Kafka (`ratatoskr kafka up`) |
| `docker-compose-cowrie.yml` | Deprecated pointer → `honeypot/docker-compose.yml` |

NiFi lab compose lives under [`nifi/docker-compose.yml`](../nifi/docker-compose.yml) and is stacked with this minimal file when using `ratatoskr up --profile nifi`.

Honeypot / full profile compose remains under [`honeypot/docker-compose.yml`](../honeypot/docker-compose.yml).
