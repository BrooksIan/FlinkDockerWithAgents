import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import type { RunDetail } from "../api/types";
import { ExecutionPlan } from "../components/ExecutionPlan";
import { RunOutputPanel } from "../components/RunOutputPanel";
import { RunSpanList } from "../components/RunSpanList";
import { RunStatusBadge } from "../components/RunStatusBadge";
import { isPipelineRun, pipelineRunName } from "../utils/runUtils";

export function RunDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [run, setRun] = useState<RunDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [studioHref, setStudioHref] = useState("/studio");

  useEffect(() => {
    if (!id) return;
    api
      .run(id)
      .then(setRun)
      .catch((e) => setError(String(e)));
  }, [id]);

  useEffect(() => {
    if (!run || !isPipelineRun(run.agent)) return;
    const name = pipelineRunName(run.agent);
    api
      .pipelines()
      .then((pipelines) => {
        const match = pipelines.find((p) => p.name === name);
        setStudioHref(match ? `/studio/${match.id}` : "/studio");
      })
      .catch(() => setStudioHref("/studio"));
  }, [run]);

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
              <div className="label">{isPipelineRun(run.agent) ? "Pipeline" : "Agent"}</div>
              <div className="value">
                {isPipelineRun(run.agent) ? (
                  <Link to={studioHref}>Pipeline: {pipelineRunName(run.agent)}</Link>
                ) : (
                  <Link to={`/agents/${run.agent}`}>{run.agent}</Link>
                )}
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

          {(run.output !== undefined && run.output !== null) || run.record_count > 0 ? (
            <RunOutputPanel output={run.output} spans={run.spans} recordCount={run.record_count} />
          ) : null}

          {isPipelineRun(run.agent) && run.spans.length > 0 && (
            <p className="muted">Per-agent and sink steps from the Studio pipeline run.</p>
          )}

          <RunSpanList spans={run.spans} />
        </>
      )}
    </>
  );
}
