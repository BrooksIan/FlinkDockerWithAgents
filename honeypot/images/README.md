# Dashboard screenshots

UI captures referenced from [honeypot/README.md](../README.md#screenshots).

| File | Description |
|------|-------------|
| `HoneypotDashboard.png` | Main Streamlit dashboard |
| `AttackTimeline.png` | Attack frequency timeline |
| `ThreatAlertsDetails.png` | Alert detail panel |
| `ReponseActions.png` | Automated response actions |
| `BlockedIps.png` | Blocked IP list |
| `CounterAttackActions.png` | Counter-attack actions |
| `CounterAttackTimeline.png` | Counter-attack timeline |
| `RepeatIPattackers.png` | Repeat attacker chart (optional) |
| `CounterAttackStats.png` / `CounterAttackStats1.png` | Counter-attack stats (optional) |

Generate fresh captures after `flink-cowrie dashboard` and simulated attacks, or run:

```bash
.venv/bin/python scripts/generate_dashboard_images.py
```
