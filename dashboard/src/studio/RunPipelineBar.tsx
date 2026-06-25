import { Link } from "react-router-dom";
import type { PipelineValidation } from "../api/types";

interface Props {
  validation: PipelineValidation | null;
  running: boolean;
  lastRunId: string | null;
  onValidate: () => void;
  onConnectChain: () => void;
  onRun: () => void;
}

export function RunPipelineBar({
  validation,
  running,
  lastRunId,
  onValidate,
  onConnectChain,
  onRun,
}: Props) {
  return (
    <div className="studio-run-bar card">
      <div className="actions" style={{ margin: 0 }}>
        <button type="button" className="secondary" onClick={onConnectChain} disabled={running}>
          Connect chain
        </button>
        <button type="button" className="secondary" onClick={onValidate} disabled={running}>
          Validate
        </button>
        <button type="button" onClick={onRun} disabled={running}>
          {running ? "Running…" : "Run locally"}
        </button>
        <span className="muted" title="Cluster deploy is not available in MVP">
          Cluster submit (MVP: local only)
        </span>
      </div>
      {validation && (
        <div style={{ marginTop: "0.75rem" }}>
          {validation.valid ? (
            <span className="badge ok">Valid</span>
          ) : (
            <span className="badge bad">Invalid</span>
          )}
          {validation.errors.map((e) => (
            <p key={e} className="error" style={{ margin: "0.35rem 0" }}>
              {e}
            </p>
          ))}
          {validation.warnings.map((w) => (
            <p key={w} className="muted" style={{ margin: "0.35rem 0" }}>
              {w}
            </p>
          ))}
        </div>
      )}
      {lastRunId && (
        <p style={{ marginTop: "0.75rem", marginBottom: 0 }}>
          Last run: <Link to={`/runs/${lastRunId}`}>{lastRunId.slice(0, 14)}…</Link>
        </p>
      )}
    </div>
  );
}
