import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { JobSummary } from "../api/types";
import { JobStateBadge } from "../components/StatusBadge";
import { useFlinkUrl } from "../hooks/useFlinkUrl";

export function JobsPage() {
  const flinkUrl = useFlinkUrl();
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  function load() {
    setLoading(true);
    api
      .jobs()
      .then(setJobs)
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
      <h2>Jobs</h2>
      <p className="muted">
        Flink streaming jobs ·{" "}
        <a href={flinkUrl} target="_blank" rel="noreferrer">
          Open Flink UI
        </a>
      </p>
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
              <th>Name</th>
              <th>State</th>
              <th>Job ID</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((j) => (
              <tr key={j.id}>
                <td>
                  <Link to={`/jobs/${j.id}`}>{j.name || "—"}</Link>
                </td>
                <td>
                  <JobStateBadge state={j.state} />
                </td>
                <td className="muted">{j.id}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {jobs.length === 0 && !loading && <p className="muted">No jobs</p>}
      </div>
    </>
  );
}
