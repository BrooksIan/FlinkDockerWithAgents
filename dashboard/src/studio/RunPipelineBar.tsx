import { Link } from "react-router-dom";
import type { PipelineValidation } from "../api/types";

interface Props {
  validation: PipelineValidation | null;
  running: boolean;
  submitting: boolean;
  lastRunId: string | null;
  clusterBlockedReason?: string | null;
  onValidate: () => void;
  onConnectChain: () => void;
  onRun: () => void;
  onClusterSubmit: () => void;
}

function publishedReactClusterWarning(validation: PipelineValidation | null): string | null {
  const warnings = validation?.cluster?.warnings ?? [];
  return (
    warnings.find((w) => w.includes("published ReAct agent")) ??
    warnings.find((w) => w.includes("Pemja")) ??
    null
  );
}

export function RunPipelineBar({
  validation,
  running,
  submitting,
  lastRunId,
  clusterBlockedReason,
  onValidate,
  onConnectChain,
  onRun,
  onClusterSubmit,
}: Props) {
  const busy = running || submitting;
  const clusterValidation = validation?.cluster;
  const clusterReactWarning = publishedReactClusterWarning(validation);
  const clusterDisabled =
    busy ||
    Boolean(clusterBlockedReason) ||
    validation?.valid === false ||
    clusterValidation?.valid === false;

  const clusterTitle = (() => {
    if (clusterBlockedReason) return clusterBlockedReason;
    if (validation?.valid === false) {
      return validation.errors.join(" · ") || "Fix pipeline validation errors first";
    }
    if (clusterValidation?.valid === false) {
      return clusterValidation.errors.join(" · ") || "Fix cluster validation errors first";
    }
    if (clusterReactWarning) {
      return "Cluster submit may fail — published ReAct agents are experimental on Flink cluster";
    }
    if (busy) return "Wait for the current run to finish";
    return "Submit as a Flink job on the cluster (JobManager + TaskManagers)";
  })();

  return (
    <div className="studio-run-bar card">
      {clusterReactWarning && !clusterBlockedReason && (
        <div className="studio-cluster-warning" role="status">
          <span className="badge warn">Cluster</span>
          <p>{clusterReactWarning}</p>
        </div>
      )}
      <div className="actions" style={{ margin: 0 }}>
        <button type="button" className="secondary" onClick={onConnectChain} disabled={busy}>
          Connect chain
        </button>
        <button type="button" className="secondary" onClick={onValidate} disabled={busy}>
          Validate
        </button>
        <button type="button" onClick={onRun} disabled={busy}>
          {running ? "Running…" : "Run locally"}
        </button>
        <button
          type="button"
          className="secondary"
          onClick={onClusterSubmit}
          disabled={clusterDisabled}
          title={clusterTitle}
        >
          {submitting ? "Submitting…" : "Run on Flink cluster"}
        </button>
      </div>
      {clusterBlockedReason && (
        <p className="muted" style={{ margin: "0.5rem 0 0", fontSize: "0.85rem" }}>
          {clusterBlockedReason}
        </p>
      )}
      {!clusterBlockedReason && validation?.valid === false && (
        <p className="muted" style={{ margin: "0.5rem 0 0", fontSize: "0.85rem" }}>
          Fix validation errors before cluster submit, or click Validate to refresh.
        </p>
      )}
      {validation && (
        <div style={{ marginTop: "0.75rem" }}>
          {validation.valid ? (
            <span className="badge ok">Valid</span>
          ) : (
            <span className="badge bad">Invalid</span>
          )}
          {clusterValidation && (
            <span className="badge neutral" style={{ marginLeft: "0.35rem" }}>
              Cluster {clusterValidation.valid ? "ready" : "blocked"}
            </span>
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
          {clusterValidation?.errors.map((e) => (
            <p key={`cluster-${e}`} className="error" style={{ margin: "0.35rem 0" }}>
              Cluster: {e}
            </p>
          ))}
          {clusterValidation?.warnings
            .filter((w) => w !== clusterReactWarning)
            .map((w) => (
              <p key={`cluster-${w}`} className="muted" style={{ margin: "0.35rem 0" }}>
                Cluster: {w}
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
