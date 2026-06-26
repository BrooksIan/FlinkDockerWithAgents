import type { AgentDefinitionValidation } from "../api/types";

interface Props {
  validation: AgentDefinitionValidation | null;
  busy: boolean;
  compileBlocked?: boolean;
  onValidate: () => void;
}

export function DesignerValidationBar({
  validation,
  busy,
  compileBlocked,
  onValidate,
}: Props) {
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
    </div>
  );
}
