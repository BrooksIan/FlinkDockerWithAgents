# Apache NiFi — Flow Monitoring and Healing

Optional lab stack for monitoring and healing Apache NiFi flows with a Ratatoskr **workflow agent**. Local demos use the NiFi REST API; CDP deployments can use the same operations via [Cloudera NiFi-MCP-Server](https://github.com/cloudera/NiFi-MCP-Server) (Knox).

## Quickstart

```bash
# From repo root (requires agent_flink_image — ratatoskr build)
source .venv/bin/activate
ratatoskr kafka up          # Studio Kafka (NiFi joins its network for kafka:9092)
ratatoskr up --profile nifi

# Wait for NiFi health, then load flows
./scripts/nifi_load_sample_flow.sh      # synthetic Generate→Log
./scripts/nifi_load_kafka_flow.sh       # ConsumeKafka ← nifi.kafka.demo

# Monitor only (Phase 1A — default)
export NIFI_HEAL_PHASE=monitor
# Host venv: direct NiFi poll (flink_agents not required).
# Flink image / cluster: uses AgentsExecutionEnvironment when available.
ratatoskr agent run workflow_nifi_monitor --local
# or: python examples/agents/run_workflow_nifi_monitor_local.py
```

| URL | Service |
|-----|---------|
| https://localhost:8443/nifi | NiFi UI (accept the self-signed cert warning) |
| https://localhost:8443/nifi-api | NiFi REST API |
| http://localhost:8082 | Flink Web UI |

## Login credentials (single-user)

This lab NiFi image uses Apache NiFi **single-user mode**. Username and password are **not** generated at runtime — they are set by Docker Compose when the container starts.

| | Default | Source |
|---|---------|--------|
| **Username** | `admin` | `NIFI_USERNAME` → `SINGLE_USER_CREDENTIALS_USERNAME` |
| **Password** | `RatatoskrNiFi1!` | `NIFI_PASSWORD` → `SINGLE_USER_CREDENTIALS_PASSWORD` |

Defined in [`nifi/docker-compose.yml`](docker-compose.yml):

```yaml
SINGLE_USER_CREDENTIALS_USERNAME: ${NIFI_USERNAME:-admin}
SINGLE_USER_CREDENTIALS_PASSWORD: ${NIFI_PASSWORD:-RatatoskrNiFi1!}
```

**How to find or change them**

1. **Defaults** — use the table above if you did not set env vars.
2. **Your overrides** — check `.env` in the repo root (copy from [`.env.example`](../.env.example)):

   ```bash
   NIFI_USERNAME=admin
   NIFI_PASSWORD=RatatoskrNiFi1!
   ```

3. **What Compose will inject** — from the repo root:

   ```bash
   grep -E 'NIFI_USERNAME|NIFI_PASSWORD|SINGLE_USER' .env nifi/docker-compose.yml 2>/dev/null
   docker compose -f deploy/docker-compose.yml -f nifi/docker-compose.yml config | grep -A1 SINGLE_USER
   ```

4. **UI login** — open https://localhost:8443/nifi , accept the cert warning, sign in with the username/password above.

**Note:** Single-user credentials are applied when the NiFi data volumes are first created. If login or `./scripts/nifi_load_sample_flow.sh` fails with **401 Unauthorized**, the container was likely bootstrapped with a different password.

Check for a generated password in logs:

```bash
docker logs deploy-nifi-1 2>&1 | grep -iE 'Generated Username|Generated Password'
```

Or reset volumes and recreate with the known defaults (`admin` / `RatatoskrNiFi1!`):

```bash
ratatoskr down --profile nifi
docker compose -f deploy/docker-compose.yml -f nifi/docker-compose.yml down -v
ratatoskr up --profile nifi
./scripts/nifi_load_sample_flow.sh
```

The monitoring agent and sample-flow scripts authenticate the same way the UI does: `POST /nifi-api/access/token` with `NIFI_USERNAME` / `NIFI_PASSWORD`, then use a **Bearer** token (HTTP Basic is not used for NiFi 2.x API calls).

## Continuous monitoring

```bash
# Managed host loops (background)
ratatoskr monitor start --interval 10 --phase monitor
ratatoskr monitor status
ratatoskr monitor stop

# Or one agent in the foreground
ratatoskr agent run workflow_nifi_monitor --local --continuous --interval 10
```

Cluster continuous jobs consume Kafka poll ticks:

```bash
ratatoskr agent run workflow_nifi_monitor --cluster --continuous --profile nifi
python scripts/publish_monitor_poll_ticks.py --continuous --target nifi
```

Host interval / Kafka ticks (direct runner):

```bash
export NIFI_HEAL_PHASE=monitor
python examples/agents/run_workflow_nifi_monitor_local.py --interval 10 --count 6
# --count 0 = forever (Ctrl-C to stop)
```

**Kafka-triggered polls** (Studio Kafka):

```bash
ratatoskr kafka up
# Terminal A — consumer / agent
python examples/agents/run_workflow_nifi_monitor_local.py --kafka-topic nifi.monitor.poll
# Terminal B — publish ticks
python scripts/publish_monitor_poll_ticks.py --count 5 --interval 2 --target nifi --phase monitor
```

## Cluster path (Flink Agents in Docker)

Requires `ratatoskr up --profile nifi` (JobManager reaches NiFi at `https://nifi:8443/nifi-api`).

```bash
export NIFI_HEAL_PHASE=monitor
export NIFI_MONITOR_POLLS=5   # finite burst; job completes after N polls
ratatoskr agent run workflow_nifi_monitor --cluster
# or: ratatoskr agent submit workflow_nifi_monitor
```

Cluster submit copies `ratatoskr/nifi/` into the JobManager. Rebuild the image (`ratatoskr build`) after pulling Dockerfile changes that add `requests` + `ratatoskr/nifi` into the image for a colder start without copy.

## Heal phases

| Phase | Env | Behavior |
|-------|-----|----------|
| **1A monitor** | `NIFI_HEAL_PHASE=monitor` | Poll health; emit alerts; **no** NiFi mutations |
| **1B safe** | `NIFI_HEAL_PHASE=safe` | Start STOPPED processors; enable DISABLED controller services |
| **1C lab** | `NIFI_HEAL_PHASE=lab` | Safe + templated `fix_processor_config`, `stop_processor` (queue relief), `restart_processor` (repeated bulletins), `terminate_processor` (INVALID without template); `empty_connection_queue` if `NIFI_HEAL_ALLOW_EMPTY_QUEUE=1` |

**Warning:** emptying queues permanently drops flowfiles. Lab only.

Full agent guide + heal matrix: [docs/NIFI_MONITOR.md](../docs/NIFI_MONITOR.md).

## Sample flow

`GenerateFlowFile → UpdateAttribute → LogAttribute` in process group **Ratatoskr Sample**.

Fault injection:

```bash
python scripts/nifi_fault_inject.py --stop-generate   # 1B — STOPPED GenerateFlowFile
python scripts/nifi_fault_inject.py --invalid-log     # LogAttribute INVALID
python scripts/nifi_fault_inject.py --queue-backlog   # queue buildup (LogAttribute stopped)
python scripts/nifi_fault_inject.py --lab-demo        # INVALID + backlog for 1C
python scripts/nifi_fault_inject.py --restore         # repair auto-terminate + restart
```

### Heal examples (sample flow)

**Safe — start GenerateFlowFile**

```bash
python scripts/nifi_fault_inject.py --stop-generate
export NIFI_HEAL_PHASE=safe
python examples/agents/run_workflow_nifi_monitor_local.py --count 1
# Expect: start_processor on GenerateFlowFile
```

**Lab — templated config fix**

```bash
python scripts/nifi_fault_inject.py --invalid-log
export NIFI_HEAL_PHASE=lab
python examples/agents/run_workflow_nifi_monitor_local.py --count 1
# Expect: fix_processor_config (auto_terminate_success)
```

**Lab — empty queue**

```bash
python scripts/nifi_fault_inject.py --queue-backlog --settle-sec 5
export NIFI_HEAL_PHASE=lab NIFI_HEAL_ALLOW_EMPTY_QUEUE=1
python examples/agents/run_workflow_nifi_monitor_local.py --count 1
```

## Kafka→NiFi demo flow

Shared base for NiFi + Kafka monitors (topic `nifi.kafka.demo`):

```bash
ratatoskr kafka up && ratatoskr up --profile nifi
./scripts/nifi_load_kafka_flow.sh
```

### Orchestrated heal examples

```bash
python3 scripts/demo_nifi_kafka_heal.py --list
python3 scripts/demo_nifi_kafka_heal.py --scenario stop-consume
python3 scripts/demo_nifi_kafka_heal.py --scenario disable-cs
python3 scripts/demo_nifi_kafka_heal.py --scenario invalid-log
python3 scripts/demo_nifi_kafka_heal.py --scenario queue-backlog
python3 scripts/demo_nifi_kafka_heal.py --scenario delete-topic
python3 scripts/demo_nifi_kafka_heal.py --scenario increase-partitions
python3 scripts/demo_nifi_kafka_heal.py --scenario lag-group
python3 scripts/demo_nifi_kafka_heal.py --scenario lag-earliest
python3 scripts/demo_nifi_kafka_heal.py --scenario cross-topic
python3 scripts/demo_nifi_kafka_heal.py --scenario cross-lag
```

| Scenario | What it shows |
|----------|----------------|
| `stop-consume` / `disable-cs` | Safe NiFi heals on ConsumeKafka / Studio Kafka CS |
| `invalid-log` / `queue-backlog` | Lab config fix and queue drain |
| `delete-topic` … `lag-earliest` | Kafka create / partitions / lag heals |
| `cross-topic` / `cross-lag` | Coordinated Kafka↔NiFi playbooks |

See [docs/NIFI_MONITOR.md](../docs/NIFI_MONITOR.md#orchestrated-heal-examples) and [docs/SIGNAL_CORRELATE.md](../docs/SIGNAL_CORRELATE.md).

## Architecture

```mermaid
flowchart TB
  subgraph Stack["ratatoskr up --profile nifi"]
    NiFi["Apache NiFi :8443"]
    Sample["Sample / Kafka demo flows"]
    Flink["Flink JM/TM :8082"]
  end

  subgraph Agent["workflow_nifi_monitor"]
    Poll["get_flow_health_status"]
    Classify["classify_health + score"]
    Plan["build_heal_plan"]
    Heal["apply_heal_policy"]
    Out["OutputEvent"]
  end

  subgraph Triggers["Poll triggers"]
    Host["Host --interval / one-shot"]
    Ticks["nifi.monitor.poll Kafka ticks"]
    Cluster["Cluster NIFI_MONITOR_POLLS"]
  end

  subgraph CDP["CDP dual path"]
    MCP["NiFi-MCP-Server via Knox"]
  end

  Sample --> NiFi
  Host --> Poll
  Ticks --> Poll
  Cluster --> Poll
  Poll --> NiFi
  Poll --> Classify --> Plan --> Heal --> Out
  Heal -->|"safe / lab"| NiFi
  MCP -.->|"same tool names"| NiFiCDP["CDP NiFi"]
```

### Monitor → heal cycle

```mermaid
flowchart LR
  A["Poll health"] --> B["Classify\nseverities + score"]
  B --> C{"NIFI_HEAL_PHASE"}
  C -->|monitor| D["OutputEvent\nheal_actions: []"]
  C -->|safe / lab| E["Ordered heal plan"]
  E --> F{"dry-run / allowlist\ncooldown / blast"}
  F -->|skip| G["skipped actions"]
  F -->|execute| H["Mutate NiFi"]
  H --> I["Verify re-poll"]
  I --> J["OutputEvent\n+ audit"]
  G --> J
  D --> J
```

### Heal phases

```mermaid
flowchart TB
  M["monitor — observe only"] --> S["safe — enable CS, start processors"]
  S --> L["lab — + config fix, stop upstream, restart,\nterminate, empty queue if allowed"]
```

## Dual path: REST vs MCP

| Path | When | How |
|------|------|-----|
| **Local REST** | Docker lab (`nifi` profile) | [`ratatoskr/nifi/client.py`](../ratatoskr/nifi/client.py) — basic auth, `NIFI_API_BASE` |
| **NiFi-MCP** | CDP / Designer / Claude | Catalog entry in `examples/mcp/mcp-server-catalog.yaml`; set `NIFI_API_BASE` + `KNOX_TOKEN`; see [NiFi-MCP-Server](https://github.com/cloudera/NiFi-MCP-Server) |

Tool names (`get_flow_health_status`, `start_processor`, `enable_controller_service`, `terminate_processor`, `empty_connection_queue`) match MCP semantics so policies stay portable.

## Tests

```bash
python3 test/test_nifi_monitor.py
```

## Layout

| Path | Description |
|------|-------------|
| `nifi/docker-compose.yml` | Apache NiFi 2.x service |
| `nifi/flows/` | Sample flow notes |
| `ratatoskr/nifi/` | REST client + heal policy |
| `examples/agents/workflow_nifi_monitor.py` | Workflow agent |
| `scripts/nifi_load_sample_flow.sh` | Bootstrap sample PG |
| `scripts/nifi_fault_inject.py` | Demo fault injector |

Extended guide: [docs/NIFI_MONITOR.md](../docs/NIFI_MONITOR.md).
