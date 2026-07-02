import type { Edge, Node } from "@xyflow/react";
import type { AgentDefinitionValidation, AgentDefinitionValidationIssue } from "../api/types";
import { issueTargetLabel, validationIssues } from "./validationUtils";

interface Props {
  validation: AgentDefinitionValidation | null;
  busy: boolean;
  compileBlocked?: boolean;
  nodes: Node[];
  edges: Edge[];
  onValidate: () => void;
  onSelectIssue?: (issue: AgentDefinitionValidationIssue) => void;
}

export function DesignerValidationBar({
  validation,
  busy,
  compileBlocked,
  nodes,
  edges,
  onValidate,
  onSelectIssue,
}: Props) {
  const issues = validationIssues(validation);

  return (
    <div className="studio-run-bar card designer-validation-bar">
      <div className="actions" style={{ margin: 0 }}>
        <button type="button" className="secondary" disabled={busy} onClick={onValidate}>
          {busy ? "Validating…" : "Validate graph"}
        </button>
      </div>
      {compileBlocked && (
        <p className="muted" style={{ margin: "0.5rem 0 0", fontSize: "0.85rem" }}>
          Fix validation errors before compile or publish.
        </p>
      )}
      {validation && (
        <div style={{ marginTop: "0.75rem" }}>
          {validation.valid ? (
            <span className="badge ok">Valid</span>
          ) : (
            <span className="badge bad">Invalid</span>
          )}
          {issues.length > 0 && (
            <ul className="designer-validation-issues">
              {issues.map((issue, index) => {
                const target = issueTargetLabel(issue, nodes, edges);
                const clickable = Boolean(onSelectIssue && (issue.node_id || issue.edge_id));
                return (
                  <li key={`${issue.message}-${index}`}>
                    <button
                      type="button"
                      className={`designer-validation-issue ${issue.level} ${
                        clickable ? "clickable" : ""
                      }`}
                      disabled={!clickable}
                      onClick={() => onSelectIssue?.(issue)}
                    >
                      <span className={`badge ${issue.level === "error" ? "bad" : "warn"}`}>
                        {issue.level}
                      </span>
                      <span>{issue.message}</span>
                      {target && <span className="muted designer-validation-target">· {target}</span>}
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
