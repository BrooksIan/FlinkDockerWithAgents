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

      <div className="card" style={{ marginBottom: "1rem" }}>
        <h3 style={{ marginTop: 0 }}>What you are looking at</h3>
        <p>
          <strong>Apache Flink</strong> is a stream-processing engine: continuous jobs over
          event streams (often Kafka) with durable state and low latency.{" "}
          <strong>Flink Agents</strong> adds an agent model on top — monitors and pipelines
          that run locally or as Flink cluster jobs.
        </p>
        <p>
          This workspace uses Flink Agents as a <strong>control plane</strong> for open-source
          data products — not as a replacement for them:
        </p>
        <ul>
          <li>
            <strong>Apache Kafka</strong> — messaging and consumer lag; agents probe brokers/topics
            and can apply gated heals.
          </li>
          <li>
            <strong>Apache NiFi / CDF</strong> — integration flows; agents watch processors, queues,
            and bulletins, with optional phased auto-heal.
          </li>
          <li>
            <strong>Cloudera Manager</strong> — platform health via Knox; recommend-only runbooks
            (no CM mutations from these agents).
          </li>
        </ul>
        <p className="muted" style={{ marginBottom: 0 }}>
          Primary use case: continuous monitoring, operator runbooks, and optional self-healing
          under explicit phase gates (<code>monitor</code> → <code>safe</code> → <code>lab</code>).
          Lab services usually run as Docker Compose; CDP Base / Knox VIP setups are documented
          in the repo under <code>docs/DEPLOYMENT_SCENARIOS.md</code>.
        </p>
      </div>

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
