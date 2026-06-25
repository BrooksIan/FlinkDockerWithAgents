import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import type { RunDetail } from "../api/types";
import { ExecutionPlan } from "../components/ExecutionPlan";
import { RunStatusBadge } from "../components/RunStatusBadge";

export function RunDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [run, setRun] = useState<RunDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    api
      .run(id)
      .then(setRun)
      .catch((e) => setError(String(e)));
  }, [id]);

  if (!id) return null;

  return (
    <>
      <p>
        <Link to="/runs">← Runs</Link>
      </p>
      <h2>
        Run {id.slice(0, 12)}… {run && <RunStatusBadge status={run.status} />}
      </h2>
      {error && <p className="error">{error}</p>}
      {!run && !error && <p className="muted">Loading…</p>}

      {run && (
        <>
          <div className="grid">
            <div className="card stat">
              <div className="label">Agent</div>
              <div className="value">
                <Link to={`/agents/${run.agent}`}>{run.agent}</Link>
              </div>
            </div>
            <div className="card stat">
              <div className="label">Kind</div>
              <div className="value">{run.kind}</div>
            </div>
            <div className="card stat">
              <div className="label">Records</div>
              <div className="value">{run.record_count}</div>
            </div>
          </div>

          {run.flink_job_id && (
            <p>
              Flink job: <Link to={`/jobs/${run.flink_job_id}`}>{run.flink_job_id}</Link>
            </p>
          )}
          {run.error && <p className="error">{run.error}</p>}

          <ExecutionPlan plan={run.plan} />

          {run.spans.length > 0 ? (
            <div className="card">
              <h3 style={{ marginTop: 0 }}>Recorded spans</h3>
              <ul className="plan-tree">
                {run.spans.map((s) => (
                  <li key={s.id}>
                    <span className="badge neutral">{s.kind}</span> <strong>{s.name}</strong>
                    {s.duration_ms != null && <span className="muted"> ({s.duration_ms}ms)</span>}
                  </li>
                ))}
              </ul>
            </div>
          ) : (
            <p className="muted">No runtime spans yet — Phase 2 will record live tool/action traces.</p>
          )}
        </>
      )}
    </>
  );
}
