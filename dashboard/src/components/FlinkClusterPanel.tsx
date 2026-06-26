import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { ClusterCheck, ClusterReadiness } from "../api/types";

function checkBadgeClass(status: ClusterCheck["status"]): string {
  if (status === "ok") return "badge ok";
  if (status === "warn") return "badge warn";
  return "badge fail";
}

function checkIcon(status: ClusterCheck["status"]): string {
  if (status === "ok") return "✓";
  if (status === "warn") return "!";
  return "✕";
}

interface Props {
  initial?: ClusterReadiness | null;
}

export function FlinkClusterPanel({ initial = null }: Props) {
  const [status, setStatus] = useState<ClusterReadiness | null>(initial);
  const [loading, setLoading] = useState(!initial);
  const [validating, setValidating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadStatus = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await api.clusterStatus();
      setStatus(data);
    } catch (err) {
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!initial) {
      void loadStatus();
    }
  }, [initial, loadStatus]);

  async function handleValidate() {
    setValidating(true);
    setError(null);
    try {
      const data = await api.validateCluster();
      setStatus(data);
    } catch (err) {
      setError(String(err));
    } finally {
      setValidating(false);
    }
  }

  const flink = status?.flink;
  const flinkUiUrl = status?.flink_rest_url ?? "http://localhost:8082";

  return (
    <div className="card designer-tool flink-cluster-tool">
      <div className="designer-tool-header">
        <h3 style={{ margin: 0 }}>Flink cluster</h3>
        {status && (
          <span className={`badge ${status.ready ? "ok" : "warn"}`}>
            {status.ready ? "Ready for jobs" : "Not ready"}
          </span>
        )}
      </div>
      <p className="muted">
        Status for <strong>Run on Flink cluster</strong> in Studio. Validates JobManager, TaskManagers,
        REST API, and free task slots.
      </p>

      {loading && !status && <p className="muted">Loading cluster status…</p>}
      {error && <p className="error">{error}</p>}

      {status && (
        <>
          <div className="grid cluster-stats-grid">
            <div className="card stat compact">
              <div className="label">REST URL</div>
              <div className="value cluster-stat-value">
                <a href={flinkUiUrl} target="_blank" rel="noreferrer">
                  {status.flink_rest_url}
                </a>
              </div>
            </div>
            <div className="card stat compact">
              <div className="label">Flink version</div>
              <div className="value">{flink?.flink_version ?? "—"}</div>
            </div>
            <div className="card stat compact">
              <div className="label">TaskManagers</div>
              <div className="value">
                {flink?.taskmanagers ?? 0}
                <span className="muted cluster-stat-sub">
                  JM {status.containers.jobmanager.running ? "up" : "down"} · TM{" "}
                  {status.containers.taskmanager.running ? "up" : "down"}
                </span>
              </div>
            </div>
            <div className="card stat compact">
              <div className="label">Slots free</div>
              <div className="value">
                {flink?.slots_free ?? 0} / {flink?.slots_total ?? 0}
              </div>
            </div>
            <div className="card stat compact">
              <div className="label">Jobs running</div>
              <div className="value">{flink?.jobs_running ?? 0}</div>
            </div>
            <div className="card stat compact">
              <div className="label">Profile</div>
              <div className="value">
                <code>{status.profile}</code>
                <span className="muted cluster-stat-sub">{status.compose_file}</span>
              </div>
            </div>
          </div>

          <p className="muted designer-settings-meta">
            Image:{" "}
            <code>
              {status.image.name}:{status.image.tag}
            </code>
            {status.image.exists ? " · built" : " · not built"}
            {status.validated_at && (
              <>
                {" "}
                · checked {new Date(status.validated_at).toLocaleString()}
              </>
            )}
          </p>

          <ul className="cluster-check-list">
            {status.checks.map((check) => (
              <li key={check.id} className={`cluster-check cluster-check-${check.status}`}>
                <span className={checkBadgeClass(check.status)} aria-hidden>
                  {checkIcon(check.status)}
                </span>
                <div className="cluster-check-body">
                  <strong>{check.label}</strong>
                  <span className="muted">{check.detail}</span>
                </div>
              </li>
            ))}
          </ul>

          {!status.ready && (
            <div className="cluster-hint muted">
              <p style={{ marginTop: 0 }}>
                Start or repair the stack, then validate again:
              </p>
              <pre className="cluster-cmd">apemosyne up</pre>
              <p>
                If honeypot already uses port 8081, set <code>FLINK_REST_PORT=8082</code> in{" "}
                <code>.env</code> for the minimal platform stack.
              </p>
            </div>
          )}
        </>
      )}

      <div className="actions">
        <button
          type="button"
          className="secondary"
          disabled={loading || validating}
          onClick={() => void loadStatus()}
        >
          {loading ? "Refreshing…" : "Refresh status"}
        </button>
        <button type="button" disabled={loading || validating} onClick={() => void handleValidate()}>
          {validating ? "Validating…" : "Validate readiness"}
        </button>
        <a
          className="btn secondary"
          href={`${flinkUiUrl}/#/overview`}
          target="_blank"
          rel="noreferrer"
          style={{ display: "inline-block", textDecoration: "none" }}
        >
          Open Flink UI
        </a>
        <Link className="btn secondary" to="/jobs" style={{ display: "inline-block", textDecoration: "none" }}>
          View jobs
        </Link>
      </div>
    </div>
  );
}
