import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api/client";
import type { AgentDetail } from "../api/types";
import { TypeBadge } from "../components/StatusBadge";

export function AgentDetailPage() {
  const { name } = useParams<{ name: string }>();
  const navigate = useNavigate();
  const [agent, setAgent] = useState<AgentDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!name) return;
    api
      .agentDefinition(name)
      .then(setAgent)
      .catch((e) => setError(String(e)));
  }, [name]);

  async function handleSubmit() {
    if (!name) return;
    setSubmitting(true);
    setMessage(null);
    setError(null);
    try {
      const res = await api.submitAgent(name);
      setMessage(`Submitted. Status: ${res.status}`);
      const running = res.jobs.find((j) => j.state === "RUNNING");
      if (running) navigate(`/jobs/${running.id}`);
    } catch (e) {
      setError(String(e));
    } finally {
      setSubmitting(false);
    }
  }

  if (!name) return null;
  if (error && !agent) return <p className="error">{error}</p>;
  if (!agent) return <p className="muted">Loading…</p>;

  return (
    <>
      <p>
        <Link to="/agents">← Agents</Link>
      </p>
      <h2>
        {agent.name} <TypeBadge type={agent.type} />
      </h2>
      <p>{agent.description}</p>

      <div className="actions">
        <button onClick={handleSubmit} disabled={submitting}>
          {submitting ? "Submitting…" : "Submit to cluster"}
        </button>
      </div>
      {message && <p className="muted">{message}</p>}
      {error && <p className="error">{error}</p>}

      <div className="card">
        <h3 style={{ marginTop: 0 }}>Catalog</h3>
        <table>
          <tbody>
            <tr>
              <th>Entry</th>
              <td>
                <code>{agent.entry}</code>
              </td>
            </tr>
            <tr>
              <th>Runner</th>
              <td>
                <code>{agent.runner}</code>
              </td>
            </tr>
            <tr>
              <th>Cluster</th>
              <td>
                <code>{agent.cluster_script}</code>
              </td>
            </tr>
            {agent.flink_yaml_path && (
              <tr>
                <th>Flink YAML</th>
                <td>
                  <code>{agent.flink_yaml_path}</code>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {agent.flink_yaml && (
        <div className="card">
          <h3 style={{ marginTop: 0 }}>Flink Agents YAML</h3>
          <pre className="yaml">{agent.flink_yaml}</pre>
        </div>
      )}

      {agent.import_note && <p className="muted">{agent.import_note}</p>}
    </>
  );
}
