# Deploy

Docker deployment configs for the Ratatoskr Flink Agents stack.

| File | Description |
| --- | --- |
| `Dockerfile` | Builds `agent_flink_image` (PyFlink + Flink Agents). Prefer `ratatoskr build`. |
| `docker-compose.yml` | Minimal JobManager + TaskManager (`ratatoskr up`) |
| `docker-compose.kafka.yml` | Studio Kafka (`ratatoskr kafka up`) |
| `docker-compose-cowrie.yml` | Deprecated pointer → `honeypot/docker-compose.yml` |

Honeypot / full profile compose remains under [`honeypot/docker-compose.yml`](../honeypot/docker-compose.yml).
