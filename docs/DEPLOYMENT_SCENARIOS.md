# Deployment scenarios — Docker lab vs CDP / Knox

This guide answers: **how services run**, and **what changes** when Flink (or NiFi / CM / Kafka) lives on **CDP Private Cloud Base** or behind a **Knox VIP** instead of local Docker Compose.

## How services run in this repo (default lab)

Most stack pieces are **Docker Compose services** built from Dockerfiles or pulled images — not bare-metal installs.

| Service | How it runs | Compose / image |
|---------|-------------|-----------------|
| Flink JobManager + TaskManager | Docker containers | [`deploy/docker-compose.yml`](../deploy/docker-compose.yml) + [`deploy/Dockerfile`](../deploy/Dockerfile) (`agent_flink_image`) |
| Studio Kafka | Docker containers | [`deploy/docker-compose.kafka.yml`](../deploy/docker-compose.kafka.yml) |
| NiFi (lab) | Docker containers | [`nifi/docker-compose.yml`](../nifi/docker-compose.yml) |
| Cowrie honeypot (optional) | Docker containers | [`honeypot/docker-compose.yml`](../honeypot/docker-compose.yml) |
| Control API | Host process (`ratatoskr api start`) | Python package — not a Dockerfile |
| Dashboard (dev) | Host Vite process | [`dashboard/`](../dashboard/) — not a Dockerfile |
| Agents (local mode) | Host Python | `ratatoskr agent run … --local` |
| Agents (cluster mode) | Inside Flink TM containers | Same image as Flink stack |

Bring-up helpers: `ratatoskr build`, `ratatoskr up`, `ratatoskr kafka up`, `ratatoskr up --profile nifi|full`. Details: [`deploy/README.md`](../deploy/README.md).

**Summary:** Flink, Kafka, NiFi, and the honeypot are Dockerized. The CLI, Control API, and dashboard are host-side tooling that talk to those containers (or to remote CDP endpoints).

---

## Why Flink is in the picture

[Apache Flink](https://flink.apache.org/) is a distributed **stream-processing** engine: low-latency, stateful pipelines over unbounded event streams (Kafka topics, sockets, files). Cloudera platforms often use Flink for continuous analytics and CDC-style processing.

**Apache Flink Agents** (used here) adds an agent programming model **on top of Flink**: `@action` / `@tool` graphs that can run as a local process **or** as Flink operators on the cluster. That is the control/automation layer — not a replacement for NiFi or Kafka.

| Layer | Product | Role in Ratatoskr |
|-------|---------|-------------------|
| Messaging | Apache Kafka | Event bus — topics for monitors, polls, runbooks, Studio pipelines |
| Integration / flows | Apache NiFi (CDF) | Move and transform data; canvas processors and queues |
| Stream compute | Apache Flink | Stateful continuous processing; host for Flink Agents jobs |
| Platform ops | Cloudera Manager | Cluster/service/role health (recommend-only in this repo) |
| Agents | Flink Agents | Deterministic monitors/heals + optional ReAct explainers |

Primary ops use case: **monitor → classify → recommend/runbook → (optional) gated heal** across NiFi, Kafka, and CM — with Flink as the scalable runtime when you submit agents to the cluster. Deeper primer: [FLINK_AGENTS.md](FLINK_AGENTS.md#what-is-apache-flink).

---

## Scenario matrix

| Scenario | Flink | NiFi | Kafka | CM | Typical auth |
|----------|-------|------|-------|-----|--------------|
| **A. Local Docker lab** | Compose JM/TM | Compose NiFi | Studio Kafka `:9094` | Optional remote Knox | Local ports / basic NiFi |
| **B. CDP Base + Knox VIP** | Flink on Base (or CDE) | CDF / NiFi via Knox | Streams Messaging / broker list | CM via Knox `cm-api` | Knox JWT (`KNOX_TOKEN`) |
| **C. Hybrid** | Local Docker Flink for Studio demos | CDP NiFi via MCP/Knox | Local Studio Kafka *or* CDP brokers | CDP CM via Knox | Mix of local + JWT |

### A — Local Docker (validated in-repo)

```bash
ratatoskr build && ratatoskr up && ratatoskr kafka up
ratatoskr up --profile nifi   # optional
ratatoskr api start
```

| Concern | Lab behavior |
|---------|----------------|
| Flink REST | `http://localhost:8082` (minimal) or `:8081` (full/honeypot) |
| Kafka bootstrap | `localhost:9094` |
| NiFi API | `https://localhost:8443/nifi-api` (`NIFI_VERIFY_SSL=false` often required) |
| SSL | Self-signed NiFi certs are normal |
| CM | Not started by Compose — point `CM_API_BASE` at a remote cluster if needed |

### B — CDP Private Cloud Base / Knox VIP (validated path: CM via Knox)

Customers often expose APIs through a **Knox gateway VIP** (load-balanced HTTPS), e.g.:

```text
https://<knox-vip>/<topology>/cdp-proxy-token/cm-api
https://<knox-vip>/<topology>/cdp-proxy-token/...
```

**Validated in this project (CM monitor):**

| Item | Value / finding |
|------|-----------------|
| Env | `CM_API_BASE=…/cdp-proxy-token/cm-api`, `CM_CLUSTER=…`, `export KNOX_TOKEN=<jwt>` |
| Auth | `Authorization: Bearer <token>` (not basic auth) |
| API path | Knox CM proxy uses `{base}/v49/…` (**not** `{base}/api/v49/…`) |
| Cluster list | `/clusters` may return empty; client falls back to host `clusterRef` |
| Console deep links | Derive UI from `cdp-proxy-token/cm-api` → `cdp-proxy/cmf` |
| Latency | Full CM snapshots often >5s → `CM_SLOW`; raise `CM_PROBE_SLOW_MS` if noisy |
| Heal | CM agent is **recommend-only** — no CM mutations through Knox |

Guide: [CM_MONITOR.md](CM_MONITOR.md). Live probe: `scripts/cm_monitor_live_probe.py`.

**NiFi on CDP (CDF):** prefer [NiFi-MCP-Server](https://github.com/cloudera/NiFi-MCP-Server) via Knox rather than raw lab REST URLs. Same heal policy semantics as the Docker lab; transport differs. See [NIFI_MONITOR.md](NIFI_MONITOR.md) · [FLINK_AGENTS_CDF_FLOWS.md](FLINK_AGENTS_CDF_FLOWS.md).

**Flink on CDP Base / CDE (expected deviations — validate per cluster):**

| Lab assumption | CDP Base / Knox likely deviation |
|----------------|----------------------------------|
| Submit via local Docker JM REST `:8082` | Point Control API / Studio at Flink REST **through Knox or an internal VIP**; ports and TLS differ |
| `ratatoskr up` starts JM/TM | Cluster already exists — skip Compose Flink; configure `FLINK_REST_URL` (or dashboard Settings) to the remote REST endpoint |
| Unsigned / HTTP REST | HTTPS + Knox token or SPNEGO; cookies/tokens expire — refresh `KNOX_TOKEN` |
| Studio “Run on cluster” syncs Python into Compose TM | Remote clusters need your org’s artifact/deploy path (image, jar, or CDE job) — Compose sync scripts may not apply |
| Kafka `localhost:9094` | Use CDP broker bootstrap + security protocol (SASL/SSL); update Studio / monitor env |
| Open Flink UI on localhost | Use Knox `…/flink` (or CDE UI) topology path instead |

**Test checklist before a customer demo on Base:**

1. Obtain a short-lived Knox JWT; confirm `curl -H "Authorization: Bearer $KNOX_TOKEN" "$CM_API_BASE/version"` (or Flink/NiFi health) succeeds.
2. Run `scripts/cm_monitor_live_probe.py` — expect `worldwidebank`-style cluster name discovery and recommendations without `CM_UNREACHABLE`.
3. For NiFi: enable MCP + Knox in Settings; run one `workflow_nifi_monitor` cycle in **monitor** phase only.
4. For Flink Agents on Base: confirm job submit path with the account team (REST vs CDE); do not assume `restart-studio-cluster.sh` works against remote TMs.
5. Document any topology path differences (`cdp-proxy` vs `cdp-proxy-token`, VIP hostname) in the customer’s `.env.example` fork — **never commit JWTs**.

### C — Hybrid (common for POCs)

| Keep local | Point remote |
|------------|--------------|
| Dashboard + Control API + Studio authoring | CM via Knox (`workflow_cm_monitor`) |
| Optional local Flink for Designer demos | CDP NiFi via MCP for heal demos |
| Studio Kafka for fixture topics | Or inject CDP Kafka bootstrap for lag demos |

Correlate runner loads `.env` automatically; still **export `KNOX_TOKEN`** in the shell.

---

## Environment cheat sheet

| Variable | Lab Docker | CDP / Knox |
|----------|------------|------------|
| `FLINK_REST_URL` / dashboard Settings | `http://localhost:8082` | Knox or internal Flink REST URL |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9094` | CDP brokers + security extras |
| `NIFI_API_BASE` | `https://localhost:8443/nifi-api` | Often unused if MCP/Knox path is enabled |
| `KNOX_TOKEN` | Unused | Required for Knox APIs |
| `CM_API_BASE` | Optional remote | `https://<vip>/…/cdp-proxy-token/cm-api` |
| `CM_CLUSTER` | Optional | CDP cluster name |
| `NIFI_HEAL_PHASE` / `KAFKA_HEAL_PHASE` | `monitor` default | Keep `monitor` until change control approves `safe`/`lab` |

Templates: [`.env.example`](../.env.example).

---

## Related docs

| Doc | Focus |
|-----|--------|
| [FLINK_AGENTS.md](FLINK_AGENTS.md) | What Flink / Flink Agents are |
| [PLATFORM.md](PLATFORM.md) | Control plane on the Docker lab |
| [CM_MONITOR.md](CM_MONITOR.md) | CM + Knox validated usage |
| [NIFI_MONITOR.md](NIFI_MONITOR.md) · [KAFKA_MONITOR.md](KAFKA_MONITOR.md) | Monitor / heal phases |
| [SIGNAL_CORRELATE.md](SIGNAL_CORRELATE.md) | Cross-stack incidents |
| [FLINK_AGENTS_CDF_FLOWS.md](FLINK_AGENTS_CDF_FLOWS.md) | CDF vs NiFi retry |
