import { Link } from "react-router-dom";
import type { AgentDefinition } from "../api/types";
import { kindLabel } from "../designer/definitionUtils";

interface Props {
  definition: AgentDefinition;
}

export function DesignerDefinitionSummary({ definition }: Props) {
  const nodeKinds = definition.nodes.reduce<Record<string, number>>((acc, node) => {
    acc[node.kind] = (acc[node.kind] || 0) + 1;
    return acc;
  }, {});

  return (
    <section className="card designer-definition-summary">
      <div className="designer-definition-summary-header">
        <h3 style={{ margin: 0 }}>{definition.name}</h3>
        <span className={`badge ${definition.status === "published" ? "ok" : "warn"}`}>
          {definition.status}
        </span>
      </div>
      <p className="muted">{definition.description || "No description yet."}</p>
      <dl className="designer-definition-summary-grid">
        <div>
          <dt>Type</dt>
          <dd>{definition.type}</dd>
        </div>
        <div>
          <dt>Nodes</dt>
          <dd>{definition.nodes.length}</dd>
        </div>
        <div>
          <dt>Edges</dt>
          <dd>{definition.edges.length}</dd>
        </div>
        <div>
          <dt>MCP servers</dt>
          <dd>{definition.mcp_servers?.length || 0}</dd>
        </div>
        <div>
          <dt>Manifest</dt>
          <dd>
            {definition.manifest_name ? (
              <Link to={`/agents/${definition.manifest_name}`}>{definition.manifest_name}</Link>
            ) : (
              "Not published"
            )}
          </dd>
        </div>
        <div>
          <dt>Schemas</dt>
          <dd>
            {Object.keys(definition.input_schema?.properties || {}).length || 0} in /{" "}
            {Object.keys(definition.output_schema?.properties || {}).length || 0} out
          </dd>
        </div>
      </dl>
      {Object.keys(nodeKinds).length > 0 && (
        <div className="designer-definition-summary-nodes">
          {Object.entries(nodeKinds).map(([kind, count]) => (
            <span key={kind} className="badge">
              {count} {kindLabel(kind as never)}
            </span>
          ))}
        </div>
      )}
      <div className="actions" style={{ marginTop: "0.75rem" }}>
        <Link to={`/designer/${definition.id}`} className="secondary-link">
          Open in editor
        </Link>
      </div>
      <details className="designer-env-hint">
        <summary>Advanced: raw JSON</summary>
        <pre className="designer-json-preview">{JSON.stringify(definition, null, 2)}</pre>
      </details>
    </section>
  );
}
