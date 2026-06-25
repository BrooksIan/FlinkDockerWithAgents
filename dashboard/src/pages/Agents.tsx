import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { AgentSummary } from "../api/types";
import { TypeBadge } from "../components/StatusBadge";

export function AgentsPage() {
  const [agents, setAgents] = useState<AgentSummary[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api
      .agents()
      .then(setAgents)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, []);

  return (
    <>
      <h2>Agents</h2>
      <p className="muted">Registered in examples/agents/agent-manifest.yaml</p>
      {loading && <p className="muted">Loading…</p>}
      {error && <p className="error">{error}</p>}
      {!loading && !error && (
        <div className="card">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Type</th>
                <th>Description</th>
                <th>YAML</th>
              </tr>
            </thead>
            <tbody>
              {agents.map((a) => (
                <tr key={a.name}>
                  <td>
                    <Link to={`/agents/${a.name}`}>{a.name}</Link>
                  </td>
                  <td>
                    <TypeBadge type={a.type} />
                  </td>
                  <td>{a.description}</td>
                  <td className="muted">{a.flink_yaml ? "yes" : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}
