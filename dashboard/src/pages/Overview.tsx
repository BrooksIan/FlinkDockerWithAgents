import { Link } from "react-router-dom";
import { useEventStream } from "../hooks/useEventStream";
import { StatusBadge } from "../components/StatusBadge";

export function OverviewPage() {
  const { health, jobs, connected, error } = useEventStream();

  if (!health) {
    return (
      <>
        <h2>Overview</h2>
        <p className="muted">Connecting to event stream…</p>
        {error && <p className="error">{error}</p>}
      </>
    );
  }

  const flink = health.flink;

  return (
    <>
      <h2>
        Overview <StatusBadge status={health.status} />
      </h2>
      <p className="muted">
        Live via SSE {connected ? "(connected)" : "(reconnecting…)"}
      </p>
      {error && <p className="error">{error}</p>}

      <div className="grid">
        <div className="card stat">
          <div className="label">Flink</div>
          <div className="value">{flink.reachable ? "Up" : "Down"}</div>
          <div className="muted">{flink.url}</div>
        </div>
        <div className="card stat">
          <div className="label">Version</div>
          <div className="value">{flink.flink_version ?? "—"}</div>
        </div>
        <div className="card stat">
          <div className="label">Slots free</div>
          <div className="value">
            {flink.slots_free ?? 0} / {flink.slots_total ?? 0}
          </div>
        </div>
        <div className="card stat">
          <div className="label">Agents</div>
          <div className="value">{health.agents.registered}</div>
        </div>
        <div className="card stat">
          <div className="label">Jobs running</div>
          <div className="value">{flink.jobs_running ?? jobs.filter((j) => j.state === "RUNNING").length}</div>
        </div>
      </div>

      <div className="card">
        <h3 style={{ marginTop: 0 }}>Recent jobs</h3>
        {jobs.length === 0 ? (
          <p className="muted">No jobs</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>State</th>
                <th>Id</th>
              </tr>
            </thead>
            <tbody>
              {jobs.slice(0, 8).map((j) => (
                <tr key={j.id}>
                  <td>
                    <Link to={`/jobs/${j.id}`}>{j.name || "—"}</Link>
                  </td>
                  <td>{j.state}</td>
                  <td className="muted">{j.id}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <p style={{ marginTop: "1rem" }}>
          <Link to="/jobs">View all jobs →</Link>
        </p>
      </div>
    </>
  );
}
