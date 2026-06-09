# Production architecture: hot path vs enrichment

This document explains three deliberate choices for a production Cowrie + Flink Agents deployment.

## Target topology

```text
Internet → Cowrie
              ↓
         Kafka (cowrie.events → cowrie.normalized)
              ├─ Phase 2 workflow (Flink/sidecar) → cowrie.alerts     ← HOT PATH (auto-block, alerts)
              └─ Phase 3 ReAct sidecar (async)    → cowrie.react_alerts ← ENRICHMENT (LLM, counter-attacks)
         log-processor → dashboard JSON (mirror / dedupe only when COWRIE_KAFKA_PIPELINE=1)
         Streamlit dashboard → internal ops only
```

---

## 1. Do not polish Streamlit for external users

**What the dashboard is for**

- Engineering validation (ReAct Agent Lab, compare workflow vs ReAct)
- Visualizing merged alerts from `cowrie-dashboard-data.json` and Kafka topics
- Counter-attack and geo demos

**What it is not**

- A customer-facing SOC portal
- An authenticated multi-tenant product
- A substitute for SIEM, Grafana, or PagerDuty

**For production visibility**, export alerts to:

- Kafka consumers → Splunk / Elastic / Sentinel
- Prometheus metrics (attack rate, pipeline lag, block success)
- `cowrie_security_alert` → real Slack/PagerDuty (not Streamlit)

**If you ever need external users**, build a separate read-only API + auth layer; do not expose Streamlit on `:8501` publicly.

---

## 2. Do not run LLM (ReAct) on every event

**Why workflow on the hot path**

| | Workflow (Phase 2) | ReAct (Phase 3) |
|--|-------------------|-----------------|
| Latency | Milliseconds–seconds | Often 10–60s per event |
| Predictability | Fixed rules + tools | Model variance (needs guardrails) |
| Auto-block | Immediate | Should not gate blocking |
| Cost | Low | LLM API per event |

**Recommended split**

- **Every event**: Phase 2 `cowrie_workflow_detect` — block, alert, counter-attacks with deterministic policy
- **Selected events**: Phase 3 `kafka-react-augmentor` — LLM reasoning, extra counter-attacks, misinformation (async)
- **Optional**: Sample ReAct (e.g. CRITICAL only) via sidecar filter, not inline

**Environment**

```bash
# log-processor / synchronous Flink (default in docker-compose-cowrie.yml)
COWRIE_HOT_PATH_ENGINE=workflow
COWRIE_COUNTER_ATTACK_ENGINE=workflow

# Lab only — do not use in production hot path
COWRIE_ALLOW_REACT_ON_HOT_PATH=1
```

---

## 3. Do not run ReAct inside the synchronous Flink hot path

**Anti-pattern**: `cowrie_log_processor` or Phase 2 Flink `map()` calling Cloudera ReAct per record.

- Blocks the consumer on LLM latency
- Submits short-lived Flink jobs that wait on HTTP 200 from Qwen
- Couples auto-block to model availability

**Correct pattern (already in this repo)**

| Component | Role | Engine |
|-----------|------|--------|
| `flink-pipeline-supervisor` | Kafka topics + Phase 1/1.5/2 Flink jobs | Flink cluster (`flink run`) |
| `kafka-workflow` / Phase 2 | Sync alert generation | `cowrie_workflow_detect` |
| `kafka-react-augmentor` | Async ReAct augmentation | `cowrie_phase3_react_augmentor` |
| `log-processor` | Tail logs → dashboard JSON | **workflow only** when `COWRIE_KAFKA_PIPELINE=1` |
| `kafka-alerts-to-dashboard` | Merge `cowrie.alerts` + `cowrie.react_alerts` into JSON | Read-only |

Phase 3 is a **Kafka consumer sidecar**, not a Flink operator on the critical path.

**Code enforcement**: `cowrie_pipeline.resolve_hot_path_engine()` returns `workflow` when `COWRIE_KAFKA_PIPELINE=1` unless `COWRIE_ALLOW_REACT_ON_HOT_PATH=1`.

---

## Quick checklist

- [ ] Phase 2 on `COWRIE_COUNTER_ATTACK_ENGINE=workflow`
- [ ] Log-processor on `COWRIE_HOT_PATH_ENGINE=workflow` with `COWRIE_KAFKA_PIPELINE=1`
- [ ] Phase 3 sidecar running separately; failures do not stop blocking
- [ ] Dashboard on private network or behind auth proxy
- [ ] Real alerts via `cowrie_security_alert.py`, not dashboard buttons
- [ ] ReAct HIGH/CRITICAL blocks write to `./cowrie-data` (`COWRIE_REACT_EXECUTE_BLOCK_IP=1`, mounted on sidecars)
- [ ] ReAct Agent Lab used for tests only, not production ops

### ReAct block IP follow-through

When `COWRIE_REACT_EXECUTE_RESPONSE_ACTIONS=1` (default), Phase 3 and the dashboard bridge call `react_response_executor.py`, which:

- Sends Slack/email mocks via `cowrie_security_alert.send_security_alert` when `send_security_alert` is in `actions_taken`
- **Auto-blocks** attacker IPs on **HIGH** and **CRITICAL** threats via `cowrie_block_ip.block_ip_cowrie` (writes `./cowrie-data/blocked_ips.txt`)
- Honors `COWRIE_REACT_EXECUTE_BLOCK_IP=0` to keep blocks as dashboard recommendations only
- Uses `COWRIE_DATA_DIR` (default `/cowrie/cowrie/data`; compose mounts `./cowrie-data` there on `log-processor`, `kafka-react-augmentor`, and `dashboard`)

Phase 2 workflow blocking remains the production hot-path gate; ReAct blocks are enrichment that mirror the same Cowrie blocklist.

## Phase 3 summary agent (`COWRIE_PHASE3_SUMMARY`)

Optional Sprint C1 enrichment: a **workflow** agent (`CowrieSummaryAgent`) with one Cloudera chat step — no ReAct tool loop.

| Variable | Default | Behavior |
|----------|---------|----------|
| `COWRIE_PHASE3_SUMMARY` | `0` | `1` = summary path for qualifying events; `0` = full ReAct on all events (unchanged) |
| `COWRIE_PHASE3_SUMMARY_MIN_SEVERITY` | `CRITICAL` | Uses `classify_cowrie_event` + `cowrie_policy.severity_at_least` |
| `COWRIE_PHASE3_SUMMARY_SAMPLE_RATE` | `1.0` | Random sample within qualifying events (0–1) |

When summary mode is on, non-qualifying events skip Phase 3 LLM entirely. Alerts use `detection_source=cloudera_summary` and optional `llm_summary` on `cowrie.react_alerts`. Response actions (block/alert mocks) still run via `react_response_executor`; counter-attacks are not invoked by the summary agent.

Verify: `python3 test/test_phase3_summary.py` and `./scripts/verify_sprint_c.sh`.

## Phase 3 MCP threat intel (`COWRIE_MCP_THREAT_INTEL`)

Sprint C2: ReAct `check_ip_reputation` can call **AbuseIPDB** when enabled; otherwise uses the offline mock table (same as pre-C2 default).

| Variable | Default | Behavior |
|----------|---------|----------|
| `COWRIE_MCP_THREAT_INTEL` | `0` | `1` = AbuseIPDB lookup in Phase 3 ReAct tool |
| `ABUSEIPDB_API_KEY` | — | Required when MCP intel enabled |
| `COWRIE_MCP_THREAT_INTEL_SOURCE` | `abuseipdb` | Only `abuseipdb` supported today |
| `COWRIE_MCP_ABUSEIPDB_MAX_AGE_DAYS` | `90` | AbuseIPDB `maxAgeInDays` |

API failures fall back to mock (`source=mock`, `fallback_reason` set). External intel is **enrichment only** — severity floors remain in `cowrie_policy.py`. Phase 2 must not import `mcp_threat_intel.py`.

Verify: `python3 test/test_mcp_threat_intel.py`.

## Phase 2 engine spike (`COWRIE_PHASE2_ENGINE`)

Phase 2 selects its Flink graph via `cowrie_pipeline.resolve_phase2_engine()`:

| Value | Graph | `detection_source` | Status |
|-------|--------|-------------------|--------|
| `pure_python` (default) | PyFlink `map(workflow_map_line)` | `pure_python` | **Production default** — stable on Flink 2.2 + Kafka connector |
| `flink_agents` | `AgentsExecutionEnvironment.from_datastream` + `CowrieResponseAgent` | `flink_agents` | **Spike** — same policy via `cowrie_workflow_detect`; may hit classloader issues combining Agents operator + Kafka in one job |

**Go/no-go (Sprint B):**

- **GO for production:** keep `COWRIE_PHASE2_ENGINE=pure_python` (compose default).
- **Local parity:** `python3 test/test_phase2_engine.py` (or `./scripts/verify_sprint_b.sh` in Docker) compares core alert fields between map UDF and Flink Agents local runner.
- **Cluster spike:** set `COWRIE_PHASE2_ENGINE=flink_agents` on `kafka-workflow-processor`, recreate the sidecar, then `flink-cowrie test phase2 --e2e`. If the job fails to RUNNING or alerts stall, stay on `pure_python` and treat `flink_agents` as experimental until classloader issues are resolved upstream.

Policy for both engines lives in `cowrie_policy.py` / `cowrie_workflow_detect.py` — the flag only changes *how* detection is wired in Flink, not the rules.

## Tests

Run locally (no Docker required):

```bash
python3 test/test_cowrie_policy.py           # shared event/severity policy
python3 test/test_phase2_engine.py           # COWRIE_PHASE2_ENGINE + alert parity (Docker in verify_sprint_b.sh)
python3 test/test_production_pipeline.py      # topic routing + hot-path policy
python3 test/test_cowrie_security_alert.py      # Slack/email mock alerting
python3 test/test_react_dashboard_bridge.py     # guardrails + executors → dashboard rows
python3 test/test_react_counter_attack_executor.py
python3 test/test_cowrie_counter_attack.py

bash scripts/verify_sprint_b.sh              # Sprint A+B exit criteria
bash scripts/verify_sprint_c.sh              # + Phase 3 summary agent (Sprint C1)
```

With full stack:

```bash
flink-cowrie test production
flink-cowrie test production --e2e
flink-cowrie test react --compare
```

Roadmap (Sprint C–D): [SPRINT_ROADMAP.md](SPRINT_ROADMAP.md)

See also: [COWRIE_RESPONSE_GUIDE.md](COWRIE_RESPONSE_GUIDE.md), [REACT_AGENT_GUIDE.md](REACT_AGENT_GUIDE.md), [COUNTER_ATTACK_GUIDE.md](COUNTER_ATTACK_GUIDE.md), [FLINK_AGENTS_VERSION.md](FLINK_AGENTS_VERSION.md).
