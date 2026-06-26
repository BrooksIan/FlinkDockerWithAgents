import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { api } from "../api/client";
import type { PipelineSummary } from "../api/types";
import { defaultDemoPipeline, emptyPipeline } from "../studio/pipelineUtils";
import {
  SESSION_DETECT_PIPELINE_RECIPE,
  SESSION_WINDOW_PIPELINE_RECIPE,
} from "../designer/promptRecipes";

export function StudioListPage() {
  const navigate = useNavigate();
  const [pipelines, setPipelines] = useState<PipelineSummary[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  function load() {
    setLoading(true);
    api
      .pipelines()
      .then(setPipelines)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }

  useEffect(() => {
    load();
  }, []);

  async function handleCreate() {
    setError(null);
    try {
      const created = await api.createPipeline(emptyPipeline());
      navigate(`/studio/${created.id}`);
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleCreateDemo() {
    setError(null);
    try {
      const created = await api.createPipeline(defaultDemoPipeline());
      navigate(`/studio/${created.id}`);
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleDelete(id: string) {
    if (!confirm("Delete this pipeline?")) return;
    try {
      await api.deletePipeline(id);
      load();
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleCreateSessionWindow() {
    setError(null);
    try {
      const created = await api.createPipeline({
        ...SESSION_WINDOW_PIPELINE_RECIPE.pipeline,
        name: SESSION_WINDOW_PIPELINE_RECIPE.name,
      });
      navigate(`/studio/${created.id}`);
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleCreateSessionDetect() {
    setError(null);
    try {
      const created = await api.createPipeline({
        ...SESSION_DETECT_PIPELINE_RECIPE.pipeline,
        name: SESSION_DETECT_PIPELINE_RECIPE.name,
      });
      navigate(`/studio/${created.id}`);
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleDuplicate(p: PipelineSummary) {
    try {
      const copy = await api.createPipeline({
        name: `${p.name} (copy)`,
        nodes: p.nodes,
        edges: p.edges,
        layout: p.layout,
      });
      navigate(`/studio/${copy.id}`);
    } catch (e) {
      setError(String(e));
    }
  }

  return (
    <>
      <h2>Agentic Studio</h2>
      <p className="muted">Compose linear agent pipelines with drag-and-drop, then run locally.</p>
      <div className="actions">
        <button type="button" onClick={handleCreate}>
          New pipeline
        </button>
        <button type="button" className="secondary" onClick={handleCreateDemo}>
          New demo pipeline
        </button>
        <button type="button" className="secondary" onClick={handleCreateSessionWindow}>
          Session window template
        </button>
        <button type="button" className="secondary" onClick={handleCreateSessionDetect}>
          Session detect (Cowrie) template
        </button>
        <button type="button" className="secondary" onClick={load} disabled={loading}>
          Refresh
        </button>
      </div>
      {error && <p className="error">{error}</p>}
      <div className="card">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Nodes</th>
              <th>Updated</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {pipelines.map((p) => (
              <tr key={p.id}>
                <td>
                  <Link to={`/studio/${p.id}`}>{p.name || "(unnamed)"}</Link>
                </td>
                <td>{p.nodes.length}</td>
                <td>{new Date(p.updated_at).toLocaleString()}</td>
                <td>
                  <button type="button" className="secondary" onClick={() => handleDuplicate(p)}>
                    Duplicate
                  </button>{" "}
                  <button type="button" className="secondary" onClick={() => handleDelete(p.id)}>
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {!loading && pipelines.length === 0 && <p className="muted">No pipelines yet.</p>}
      </div>
    </>
  );
}
