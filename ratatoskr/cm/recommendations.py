"""Declarative recommendation catalog for Cloudera Manager monitoring."""

from __future__ import annotations

from typing import Any

from ratatoskr.cm.env import cm_console_base

RECOMMEND_RULES: list[dict[str, Any]] = [
    {
        "id": "restart_stopped_role",
        "match_severity": "ROLE_DOWN",
        "source": "stopped_roles",
        "priority": "high",
        "risk": "low",
        "summary_tpl": "Restart role {role} in service {service}",
        "recommendation_tpl": "Restart the stopped role via Cloudera Manager.",
        "manual_steps_tpl": [
            "Open CM → {service} → Instances → {role}",
            "Select Actions → Restart",
            "Verify role health checks pass within 5 minutes",
        ],
        "api_reference_tpl": "POST /clusters/{cluster}/services/{service}/roles/{role}/commands/restart",
        "console_path_tpl": "/cmf/clusters/{cluster}/services/{service}/roles/{role}",
    },
    {
        "id": "start_stopped_service",
        "match_severity": "SERVICE_DOWN",
        "source": "stopped_services",
        "priority": "high",
        "risk": "medium",
        "summary_tpl": "Start service {service}",
        "recommendation_tpl": "Start the stopped service and verify dependent roles.",
        "manual_steps_tpl": [
            "Open CM → {service}",
            "Select Actions → Start",
            "Confirm all required roles reach STARTED state",
        ],
        "api_reference_tpl": "POST /clusters/{cluster}/services/{service}/commands/start",
        "console_path_tpl": "/cmf/clusters/{cluster}/services/{service}",
    },
    {
        "id": "investigate_service_health",
        "match_severity": "SERVICE_BAD",
        "source": "bad_services",
        "priority": "high",
        "risk": "low",
        "summary_tpl": "Investigate unhealthy service {service} ({health_summary})",
        "recommendation_tpl": "Review service health checks and recent commands before restarting.",
        "manual_steps_tpl": [
            "Open CM → {service} → Status",
            "Review failed health checks and command history",
            "Follow service-specific runbook for {health_summary} state",
        ],
        "console_path_tpl": "/cmf/clusters/{cluster}/services/{service}",
    },
    {
        "id": "deploy_client_configs",
        "match_severity": "CONFIG_STALE",
        "source": "stale_services",
        "priority": "medium",
        "risk": "low",
        "summary_tpl": "Deploy stale client configs for {service}",
        "recommendation_tpl": "Deploy client configuration and refresh stale services.",
        "manual_steps_tpl": [
            "Open CM → {service} → Actions → Deploy Client Configuration",
            "Run cluster/client refresh if prompted",
        ],
        "api_reference_tpl": "POST /clusters/{cluster}/commands/deployClientConfig",
        "console_path_tpl": "/cmf/clusters/{cluster}/services/{service}",
    },
    {
        "id": "investigate_host",
        "match_severity": "HOST_BAD",
        "source": "bad_hosts",
        "priority": "high",
        "risk": "low",
        "summary_tpl": "Investigate unhealthy host {hostname}",
        "recommendation_tpl": "Check agent heartbeat, disk, and network on the host.",
        "manual_steps_tpl": [
            "Open CM → Hosts → {hostname}",
            "Review health tests and running processes",
            "SSH to host and inspect agent logs if needed",
        ],
        "console_path_tpl": "/cmf/hosts/{host_id}",
    },
    {
        "id": "review_decommissioned_host",
        "match_severity": "HOST_DECOMMISSIONED",
        "source": "decommissioned_hosts",
        "priority": "medium",
        "risk": "medium",
        "summary_tpl": "Host {hostname} is {commission_state}",
        "recommendation_tpl": "Confirm decommission is intentional; recommission if the host should serve roles.",
        "manual_steps_tpl": [
            "Open CM → Hosts → {hostname}",
            "If unintended, use Recommission and start roles",
        ],
        "console_path_tpl": "/cmf/hosts/{host_id}",
    },
    {
        "id": "investigate_health_check",
        "match_severity": "HEALTH_CHECK_FAIL",
        "source": "failed_health_checks",
        "priority": "high",
        "risk": "low",
        "summary_tpl": "Health check failed: {name}",
        "recommendation_tpl": "{explanation}",
        "manual_steps_tpl": [
            "Open CM health check details for {name}",
            "Follow the explanation and service runbook",
        ],
    },
    {
        "id": "investigate_parcel_distribution",
        "match_severity": "PARCEL_ERROR",
        "source": "parcel_errors",
        "priority": "high",
        "risk": "medium",
        "summary_tpl": "Parcel {product}-{version} error during {stage}",
        "recommendation_tpl": "Review parcel distribution errors; retry or cancel distribution.",
        "manual_steps_tpl": [
            "Open CM → Parcels",
            "Inspect errors for {product} {version}",
            "Retry distribution or remove failed parcel download",
        ],
        "console_path_tpl": "/cmf/clusters/{cluster}/parcels",
    },
    {
        "id": "retry_failed_command",
        "match_severity": "COMMAND_FAILED",
        "source": "failed_commands",
        "priority": "high",
        "risk": "high",
        "summary_tpl": "Failed command {name} (id={id})",
        "recommendation_tpl": "Review command logs; retry or abort if safe.",
        "manual_steps_tpl": [
            "Open CM → Commands → {id}",
            "Review stderr/stdout and result message",
            "Retry only after addressing root cause",
        ],
        "api_reference_tpl": "POST /commands/{id}/retry",
        "console_path_tpl": "/cmf/commands/{id}",
    },
    {
        "id": "impala_spnego_auth",
        "match_severity": "EVENT_CRITICAL",
        "match_event_kind": "impala_spnego",
        "source": "critical_events",
        "priority": "high",
        "risk": "medium",
        "summary_tpl": "Impala metrics auth failure (SPNEGO) — {count} event(s)",
        "recommendation_tpl": (
            "Cloudera Manager cannot retrieve Impala metrics because Kerberos SPNEGO "
            "authentication failed. Verify CM monitoring principals and Impala daemon TLS/Kerberos config."
        ),
        "manual_steps_tpl": [
            "Open CM → Impala → Configuration — verify monitoring user and Kerberos settings",
            "Confirm keytabs/principals for Impala daemons and CM Service Monitor",
            "Check clock skew and KDC reachability on Impala hosts",
            "Review Impala role logs for the affected host mentioned in the event",
        ],
        "console_path_tpl": "/cmf/clusters/{cluster}/services/impala",
    },
    {
        "id": "impala_state_fetcher_failure",
        "match_severity": "EVENT_CRITICAL",
        "match_event_kind": "impala_state_fetcher",
        "source": "critical_events",
        "priority": "high",
        "risk": "medium",
        "summary_tpl": "Impala state fetcher failing — {count} event(s)",
        "recommendation_tpl": (
            "CM Impala state fetcher tasks are failing. Check catalogd/statestored "
            "health and network connectivity from CM agents to Impala endpoints."
        ),
        "manual_steps_tpl": [
            "Open CM → Impala → Instances and review role health",
            "Inspect Impala StateStore and catalogd logs on affected hosts",
            "Verify ports/firewall between CM agents and Impala web/metrics endpoints",
        ],
        "console_path_tpl": "/cmf/clusters/{cluster}/services/impala",
    },
    {
        "id": "metrics_pipeline_gap",
        "match_severity": "EVENT_CRITICAL",
        "match_event_kind": "metrics_missing",
        "source": "critical_events",
        "priority": "medium",
        "risk": "low",
        "summary_tpl": "Metrics pipeline gaps detected — {count} event(s)",
        "recommendation_tpl": (
            "Service Monitor reported missing metrics entries. Often transient; "
            "correlate with host or role restarts."
        ),
        "manual_steps_tpl": [
            "Open CM → Events and filter around {first_seen}",
            "Check Service Monitor and host agent logs if persistent",
        ],
        "console_path_tpl": "/cmf/events",
    },
    {
        "id": "review_critical_event",
        "match_severity": "EVENT_CRITICAL",
        "source": "critical_events",
        "priority": "high",
        "risk": "low",
        "skip_if_event_kind": True,
        "summary_tpl": "Critical event ({count}x): {content}",
        "recommendation_tpl": "Review the event timeline and correlate with service health.",
        "manual_steps_tpl": [
            "Open CM → Events",
            "Filter around {first_seen} to {last_seen}",
            "Correlate with affected services/roles",
        ],
        "console_path_tpl": "/cmf/events",
    },
    {
        "id": "review_event_warning",
        "match_severity": "EVENT_WARN",
        "source": "event_warnings",
        "priority": "medium",
        "risk": "low",
        "summary_tpl": "Warning event ({count}x): {content}",
        "recommendation_tpl": "Review grouped warning events; escalate if count grows or services degrade.",
        "manual_steps_tpl": [
            "Open CM → Events",
            "Filter around {first_seen} to {last_seen}",
            "Watch for matching service health check failures",
        ],
        "console_path_tpl": "/cmf/events",
    },
    {
        "id": "investigate_mgmt_service",
        "match_severity": "MGMT_UNHEALTHY",
        "source": "mgmt",
        "priority": "critical",
        "risk": "medium",
        "summary_tpl": "Cloudera Manager management service is unhealthy",
        "recommendation_tpl": "Review CM service roles (Event Server, Service Monitor, etc.).",
        "manual_steps_tpl": [
            "Open CM → Administration → Cloudera Management Service",
            "Restart unhealthy management roles if needed",
        ],
        "console_path_tpl": "/cmf/cm/service",
    },
    {
        "id": "investigate_cluster_health",
        "match_severity": "CLUSTER_BAD",
        "source": "cluster_info",
        "priority": "critical",
        "risk": "low",
        "summary_tpl": "Cluster {name} health is {health_summary}",
        "recommendation_tpl": "Triage cluster-wide health before service-level changes.",
        "manual_steps_tpl": [
            "Open CM → cluster home for {name}",
            "Review top failing health checks and recent commands",
        ],
        "console_path_tpl": "/cmf/clusters/{name}",
    },
]

_PRIORITY_RANK = {"critical": 0, "high": 1, "medium": 2, "low": 3}


def _format(template: str, entity: dict[str, Any], cluster: str) -> str:
  ctx = {"cluster": cluster, **{k: v for k, v in entity.items() if v is not None}}
  try:
      return template.format(**ctx)
  except KeyError:
      return template


def _console_url(path_tpl: str | None, entity: dict[str, Any], cluster: str) -> str | None:
    if not path_tpl:
        return None
    path = _format(path_tpl, entity, cluster)
    if not path.startswith("/"):
        path = f"/{path}"
    base = cm_console_base().rstrip("/")
    if base.endswith("/cmf") and path.startswith("/cmf/"):
        path = path[len("/cmf") :]
    return f"{base}{path}"


def _entity_matches_rule(entity: dict[str, Any], rule: dict[str, Any]) -> bool:
    want_kind = rule.get("match_event_kind")
    if want_kind and entity.get("event_kind") != want_kind:
        return False
    if rule.get("skip_if_event_kind") and entity.get("event_kind"):
        return False
    return True


def build_recommendations(
    health: dict[str, Any],
    classification: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Map health snapshot findings to structured fix recommendations (read-only)."""
    _ = classification
    cluster = str(health.get("cluster") or "")
    severities = set(health.get("severities") or [])
    recommendations: list[dict[str, Any]] = []
    seen: set[str] = set()

    for rule in RECOMMEND_RULES:
        match_sev = rule.get("match_severity")
        if match_sev and match_sev not in severities:
            continue
        source = str(rule.get("source") or "")
        if source == "mgmt":
            entities = [health.get("mgmt") or {}]
        elif source == "cluster_info":
            entities = [health.get("cluster_info") or {}]
        else:
            entities = list(health.get(source) or [])
        if not entities:
            continue

        for entity in entities:
            if not isinstance(entity, dict):
                continue
            if not _entity_matches_rule(entity, rule):
                continue
            dedupe = (
                f"{rule['id']}:"
                f"{entity.get('fingerprint') or entity.get('event_kind')}:"
                f"{entity.get('service')}:{entity.get('role')}:"
                f"{entity.get('host_id')}:{entity.get('id')}:{entity.get('name')}"
            )
            if dedupe in seen:
                continue
            seen.add(dedupe)

            rec: dict[str, Any] = {
                "rule_id": rule["id"],
                "priority": rule.get("priority", "medium"),
                "risk": rule.get("risk", "low"),
                "summary": _format(str(rule.get("summary_tpl") or ""), entity, cluster),
                "recommendation": _format(
                    str(rule.get("recommendation_tpl") or ""), entity, cluster
                ),
            }
            steps_tpl = rule.get("manual_steps_tpl") or []
            rec["manual_steps"] = [
                _format(str(step), entity, cluster) for step in steps_tpl
            ]
            if rule.get("api_reference_tpl"):
                rec["api_reference"] = _format(
                    str(rule["api_reference_tpl"]), entity, cluster
                )
            console_url = _console_url(rule.get("console_path_tpl"), entity, cluster)
            if console_url:
                rec["console_url"] = console_url
            rec["related_entity"] = {
                k: v
                for k, v in entity.items()
                if k
                in (
                    "cluster",
                    "service",
                    "role",
                    "host_id",
                    "hostname",
                    "name",
                    "id",
                    "health_summary",
                    "event_kind",
                    "fingerprint",
                    "count",
                    "first_seen",
                    "last_seen",
                    "service_hint",
                )
            }
            recommendations.append(rec)

    recommendations.sort(
        key=lambda r: (
            _PRIORITY_RANK.get(str(r.get("priority") or "medium"), 9),
            str(r.get("summary") or ""),
        )
    )
    return recommendations
