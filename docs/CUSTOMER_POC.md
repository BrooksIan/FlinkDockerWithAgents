# Customer POC (10–15 minutes)

Scripted path for a **single lab NiFi + Studio Kafka**. Goal: show agents that
**monitor → explain → propose → ack → apply → verify** without claiming fleet
management for hundreds of nodes.

## Prereqs

```bash
source .venv/bin/activate
ratatoskr kafka up
ratatoskr up --profile nifi
./scripts/nifi_load_dataplane_flow.sh
```

NiFi UI: https://localhost:8443/nifi (default `admin` / `RatatoskrNiFi1!`).

## Run the demo

```bash
# Narrated (pause between steps)
python3 scripts/demo_customer_poc.py --pause

# Unattended
python3 scripts/demo_customer_poc.py

# Safe for shared labs (no NiFi write on apply)
python3 scripts/demo_customer_poc.py --dry-run-apply
```

## Story beats (~12 min)

| Step | What happens | What to say |
|------|----------------|-------------|
| **0 Ensure** | Data Plane PG + topics | Lab spine only — not a multi-cluster product |
| **1 Break** | Drift `ratatoskr.env`, publish valid + invalid JSON | Realistic fault: bad events + wrong desired state |
| **2 Monitor** | Schema gate + route enrich (`monitor`) + correlate + scribe | **Zero mutations**; violations quarantined; drift detected |
| **3 Propose → ack → apply** | `dataplane.propose` / `dataplane.ack` then `config_apply` | Human (or policy) must ack before write |
| **4 Verify** | Re-poll route/schema | Drift cleared (unless `--dry-run-apply`) |

## Safety defaults (POC)

| Mode | Behavior |
|------|----------|
| `monitor` | Observe only |
| Approval bus | Propose is inert until ack |
| This demo | Does **not** start/stop/empty-queue heal |
| `--dry-run-apply` | Ack path exercised without NiFi property write |

## Agents on stage

| Agent | Role in POC |
|-------|-------------|
| `workflow_schema_gate` | Contract gate / `schema.violations` |
| `workflow_route_enrich` | Desired enrich/route vs live props |
| `workflow_dataplane_approval` | Propose → ack → apply |
| `workflow_signal_correlate` | Incidents (`schema_violation_spike`, `route_config_drift`) |
| `react_incident_scribe` | Explain-only brief |

## Out of POC scope (say so)

- Hundreds of NiFi nodes / CDP fleet inventory  
- NiFi Registry promote/revert  
- Mandatory IdP / ServiceNow productization  

Optional separate heal catalog (infra start/stop):  
`python3 scripts/demo_nifi_kafka_heal.py --list`

## Docs

- [SCHEMA_GATE.md](SCHEMA_GATE.md) · [ROUTE_ENRICH.md](ROUTE_ENRICH.md) · [DATAPLANE_APPROVAL.md](DATAPLANE_APPROVAL.md) · [SIGNAL_CORRELATE.md](SIGNAL_CORRELATE.md)
