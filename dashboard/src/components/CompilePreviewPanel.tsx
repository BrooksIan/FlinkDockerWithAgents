import { useState } from "react";
import type { AgentDefinitionCompileResult } from "../api/types";

interface Props {
  result: AgentDefinitionCompileResult | null;
}

export function CompilePreviewPanel({ result }: Props) {
  const [activePath, setActivePath] = useState<string>("agent.py");

  if (!result) {
    return (
      <div className="card designer-tool">
        <h3 style={{ margin: 0 }}>Compile preview</h3>
        <p className="muted">Compile a definition to preview generated Python, YAML, and manifest artifacts.</p>
      </div>
    );
  }

  const validation = result.validation;
  const files = result.files ?? [];
  const selected = files.find((file) => file.path === activePath) ?? files[0];

  return (
    <div className="card designer-tool">
      <div className="designer-tool-header">
        <h3 style={{ margin: 0 }}>Compile preview</h3>
        <span className="badge ok">{result.status}</span>
      </div>
      <p className="muted">
        Output: <code>{result.output_dir}</code> · class <code>{result.class_name}</code> · slug{" "}
        <code>{result.agent_slug}</code>
      </p>
      {validation && (
        <div style={{ marginBottom: "0.75rem" }}>
          <span className={`badge ${validation.valid ? "ok" : "bad"}`}>
            {validation.valid ? "Compile validation OK" : "Compile validation issues"}
          </span>
          {validation.errors.map((entry) => (
            <p key={entry} className="error" style={{ margin: "0.35rem 0" }}>
              {entry}
            </p>
          ))}
          {validation.warnings.map((entry) => (
            <p key={entry} className="muted" style={{ margin: "0.35rem 0" }}>
              {entry}
            </p>
          ))}
        </div>
      )}
      <div className="compile-file-tabs">
        {files.map((file) => (
          <button
            key={file.path}
            type="button"
            className={selected?.path === file.path ? "active" : "secondary"}
            onClick={() => setActivePath(file.path)}
          >
            {file.path}
          </button>
        ))}
      </div>
      {selected && (
        <pre className="card designer-json-preview compile-preview">{selected.content}</pre>
      )}
    </div>
  );
}
