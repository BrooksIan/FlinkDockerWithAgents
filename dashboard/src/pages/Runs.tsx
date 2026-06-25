import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { RunSummary } from "../api/types";
import { RunStatusBadge } from "../components/RunStatusBadge";

export function RunsPage() {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  function load() {
    setLoading(true);
    api
      .runs()
      .then(setRuns)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }

  useEffect(() => {
    load();
    const t = setInterval(load, 10000);
    return () => clearInterval(t);
  }, []);

  return (
    <>
      <h2>Runs</h2>
      <p className="muted">Agent invocations — local runners and cluster submits.</p>
      <div className="actions">
        <button className="secondary" onClick={load} disabled={loading}>
          Refresh
        </button>
      </div>
      {error && <p className="error">{error}</p>}
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>Agent</th>
              <th>Kind</th>
              <th>Status</th>
              <th>Started</th>
              <th>Run ID</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.id}>
                <td>
                  <Link to={`/agents/${r.agent}`}>{r.agent}</Link>
                </td>
                <td>{r.kind}</td>
                <td>
                  <RunStatusBadge status={r.status} />
                </td>
                <td className="muted">{new Date(r.started_at).toLocaleString()}</td>
                <td>
                  <Link to={`/runs/${r.id}`}>{r.id}</Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {runs.length === 0 && !loading && (
          <p className="muted">
            No runs yet. Try <code>apemosyne agent run workflow_counter --local</code>.
          </p>
        )}
      </div>
    </>
  );
}
