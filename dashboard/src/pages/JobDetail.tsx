import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api/client";
import { JobStateBadge } from "../components/StatusBadge";
import { useFlinkUrl } from "../hooks/useFlinkUrl";

export function JobDetailPage() {
  const flinkUrl = useFlinkUrl();
  const { id } = useParams<{ id: string }>();
  const [job, setJob] = useState<Record<string, unknown> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [canceling, setCanceling] = useState(false);

  useEffect(() => {
    if (!id) return;
    api
      .job(id)
      .then(setJob)
      .catch((e) => setError(String(e)));
  }, [id]);

  async function handleCancel() {
    if (!id) return;
    setCanceling(true);
    try {
      await api.cancelJob(id);
      const updated = await api.job(id);
      setJob(updated);
    } catch (e) {
      setError(String(e));
    } finally {
      setCanceling(false);
    }
  }

  if (!id) return null;

  const state = String(job?.state ?? "");

  return (
    <>
      <p>
        <Link to="/jobs">← Jobs</Link>
      </p>
      <h2>
        Job {id.slice(0, 8)}… {state && <JobStateBadge state={state} />}
      </h2>
      {error && <p className="error">{error}</p>}
      {!job && !error && <p className="muted">Loading…</p>}

      {job && (
        <>
          <div className="actions">
            <button className="danger" onClick={handleCancel} disabled={canceling || state === "CANCELED"}>
              {canceling ? "Canceling…" : "Cancel job"}
            </button>
            <a
              className="btn secondary"
              href={`${flinkUrl}/#/job/${id}/overview`}
              target="_blank"
              rel="noreferrer"
              style={{ display: "inline-block", textDecoration: "none" }}
            >
              Open in Flink UI
            </a>
          </div>
          <div className="card">
            <pre className="yaml">{JSON.stringify(job, null, 2)}</pre>
          </div>
        </>
      )}
    </>
  );
}
