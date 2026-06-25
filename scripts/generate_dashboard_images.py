#!/usr/bin/env python3
"""Generate representative dashboard PNGs for honeypot/README.md."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import numpy as np

OUT = Path(__file__).resolve().parent.parent / "honeypot" / "images"
OUT.mkdir(parents=True, exist_ok=True)

BG = "#0e1117"
PANEL = "#1a1f2e"
TEXT = "#fafafa"
ACCENT = "#ff4b4b"
WARN = "#ffa421"
OK = "#21c354"
BLUE = "#4da6ff"


def _save(fig: plt.Figure, name: str) -> None:
    path = OUT / name
    fig.savefig(path, dpi=150, facecolor=BG, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)
    print(f"Wrote {path}")


def honeypot_dashboard() -> None:
    fig = plt.figure(figsize=(14, 8), facecolor=BG)
    gs = GridSpec(2, 3, figure=fig, hspace=0.35, wspace=0.3)

    fig.suptitle("HoneyPot Threat Detection Dashboard", color=TEXT, fontsize=16, fontweight="bold", y=0.98)

    ax0 = fig.add_subplot(gs[0, 0])
    ax0.set_facecolor(PANEL)
    ax0.bar(["Critical", "High", "Medium", "Low"], [12, 34, 56, 89], color=[ACCENT, WARN, BLUE, OK])
    ax0.set_title("Alerts by severity", color=TEXT, fontsize=10)
    ax0.tick_params(colors=TEXT, labelsize=8)

    ax1 = fig.add_subplot(gs[0, 1:])
    ax1.set_facecolor(PANEL)
    xs = np.arange(24)
    ax1.plot(xs, 5 + np.cumsum(np.random.poisson(2, 24)), color=ACCENT, linewidth=2)
    ax1.fill_between(xs, 0, 5 + np.cumsum(np.random.poisson(2, 24)), alpha=0.2, color=ACCENT)
    ax1.set_title("Events (24h)", color=TEXT, fontsize=10)
    ax1.tick_params(colors=TEXT, labelsize=8)

    ax2 = fig.add_subplot(gs[1, :])
    ax2.set_facecolor(PANEL)
    ax2.axis("off")
    rows = [
        ("203.0.113.77", "SUCCESSFUL_INTRUSION", "CRITICAL", "BLOCK_IP"),
        ("198.51.100.42", "BRUTE_FORCE", "HIGH", "SEND_ALERT"),
        ("192.0.2.15", "MALICIOUS_COMMAND", "HIGH", "TARPIT"),
    ]
    for i, row in enumerate(rows):
        y = 0.75 - i * 0.28
        ax2.text(0.02, y, row[0], color=TEXT, fontsize=11, transform=ax2.transAxes, family="monospace")
        ax2.text(0.28, y, row[1], color=WARN if "HIGH" in row[2] else ACCENT, fontsize=10, transform=ax2.transAxes)
        ax2.text(0.62, y, row[2], color=ACCENT, fontsize=10, transform=ax2.transAxes)
        ax2.text(0.82, y, row[3], color=OK, fontsize=9, transform=ax2.transAxes)
    ax2.set_title("Recent alerts", color=TEXT, fontsize=10, loc="left")

    _save(fig, "HoneypotDashboard.png")


def attack_timeline() -> None:
    fig, ax = plt.subplots(figsize=(10, 4), facecolor=BG)
    ax.set_facecolor(PANEL)
    hours = np.arange(0, 48)
    attacks = np.random.poisson(3, 48) + (np.sin(hours / 4) * 2 + 2).astype(int)
    ax.bar(hours, attacks, color=ACCENT, alpha=0.85, width=0.8)
    ax.set_title("Attack timeline (48h)", color=TEXT, fontsize=12)
    ax.set_xlabel("Hours ago", color=TEXT)
    ax.set_ylabel("Events", color=TEXT)
    ax.tick_params(colors=TEXT)
    _save(fig, "AttackTimeline.png")


def threat_alerts_details() -> None:
    fig, ax = plt.subplots(figsize=(10, 5), facecolor=BG)
    ax.set_facecolor(PANEL)
    ax.axis("off")
    ax.set_title("Threat alert detail — 203.0.113.77", color=TEXT, fontsize=12, loc="left", pad=12)
    lines = [
        ("Severity", "CRITICAL", ACCENT),
        ("Threat type", "SUCCESSFUL_INTRUSION", TEXT),
        ("Event", "cowrie.login.success", TEXT),
        ("Session", "a3f9c2e1-…", TEXT),
        ("Recommended", "BLOCK_IP, SEND_ALERT, QUARANTINE", OK),
        ("Detection", "pure_python workflow", BLUE),
    ]
    for i, (k, v, c) in enumerate(lines):
        y = 0.82 - i * 0.14
        ax.text(0.05, y, k + ":", color="#aaa", fontsize=11, transform=ax.transAxes)
        ax.text(0.32, y, v, color=c, fontsize=11, transform=ax.transAxes, family="monospace")
    _save(fig, "ThreatAlertsDetails.png")


def response_actions() -> None:
    fig, ax = plt.subplots(figsize=(10, 4), facecolor=BG)
    ax.set_facecolor(PANEL)
    ax.axis("off")
    ax.set_title("Automated response actions", color=TEXT, fontsize=12, loc="left")
    actions = [
        ("BLOCK_IP_COWRIE", OK, "203.0.113.77 blocked"),
        ("SEND_ALERT", WARN, "Slack #security-alerts"),
        ("LOG_INCIDENT", BLUE, "Incident logged"),
        ("QUARANTINE", ACCENT, "Session isolated"),
    ]
    for i, (name, color, detail) in enumerate(actions):
        y = 0.78 - i * 0.2
        rect = mpatches.FancyBboxPatch((0.04, y - 0.04), 0.92, 0.14, boxstyle="round,pad=0.01",
                                       facecolor="#252a38", edgecolor=color, linewidth=1.5,
                                       transform=ax.transAxes)
        ax.add_patch(rect)
        ax.text(0.06, y, name, color=color, fontsize=11, fontweight="bold", transform=ax.transAxes)
        ax.text(0.06, y - 0.06, detail, color="#ccc", fontsize=9, transform=ax.transAxes)
    _save(fig, "ReponseActions.png")


def blocked_ips() -> None:
    fig, ax = plt.subplots(figsize=(8, 5), facecolor=BG)
    ax.set_facecolor(PANEL)
    ax.axis("off")
    ax.set_title("Blocked IPs (Cowrie blocklist)", color=TEXT, fontsize=12, loc="left")
    ips = ["203.0.113.77", "198.51.100.42", "192.0.2.15", "10.0.0.55", "172.16.0.99"]
    for i, ip in enumerate(ips):
        ax.text(0.08, 0.82 - i * 0.14, "[BLOCK]", color=OK, fontsize=9, transform=ax.transAxes)
        ax.text(0.16, 0.82 - i * 0.14, ip, color=ACCENT, fontsize=12, family="monospace", transform=ax.transAxes)
        ax.text(0.55, 0.82 - i * 0.14, "blocked", color=OK, fontsize=10, transform=ax.transAxes)
    _save(fig, "BlockedIps.png")


def counter_attack_actions() -> None:
    fig, ax = plt.subplots(figsize=(10, 5), facecolor=BG)
    ax.set_facecolor(PANEL)
    tools = ["gather_intel", "deploy_tarpit", "feed_disinfo", "share_threat_intel", "report_authorities"]
    counts = [18, 12, 9, 14, 6]
    colors = [BLUE, WARN, "#b366ff", OK, ACCENT]
    ax.barh(tools, counts, color=colors)
    ax.set_title("Counter-attack actions (24h)", color=TEXT, fontsize=12)
    ax.tick_params(colors=TEXT)
    ax.set_xlabel("Count", color=TEXT)
    _save(fig, "CounterAttackActions.png")


def counter_attack_timeline() -> None:
    fig, ax = plt.subplots(figsize=(10, 4), facecolor=BG)
    ax.set_facecolor(PANEL)
    t = np.arange(0, 24)
    intel = np.cumsum(np.random.poisson(1, 24))
    tarpit = np.cumsum(np.random.poisson(0.5, 24))
    ax.plot(t, intel, label="Intel gathered", color=BLUE, linewidth=2)
    ax.plot(t, tarpit, label="Tarpits deployed", color=WARN, linewidth=2)
    ax.legend(facecolor=PANEL, edgecolor="#444", labelcolor=TEXT)
    ax.set_title("Counter-attack timeline", color=TEXT, fontsize=12)
    ax.set_xlabel("Hour", color=TEXT)
    ax.tick_params(colors=TEXT)
    _save(fig, "CounterAttackTimeline.png")


def main() -> None:
    np.random.seed(42)
    honeypot_dashboard()
    attack_timeline()
    threat_alerts_details()
    response_actions()
    blocked_ips()
    counter_attack_actions()
    counter_attack_timeline()
    print(f"Done — {len(list(OUT.glob('*.png')))} PNGs in {OUT}")


if __name__ == "__main__":
    main()
