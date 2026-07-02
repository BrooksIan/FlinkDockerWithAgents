interface Props {
  status: string | undefined;
  validationValid: boolean | null;
  hasCompile: boolean;
  manifestName?: string | null;
  busy: "validate" | "compile" | "publish" | "run" | null;
  compileBlocked: boolean;
  onValidate: () => void;
  onCompile: () => void;
  onPublish: () => void;
}

export function DesignerPublishSteps({
  status,
  validationValid,
  hasCompile,
  manifestName,
  busy,
  compileBlocked,
  onValidate,
  onCompile,
  onPublish,
}: Props) {
  const step1 = validationValid === true;
  const step2 = hasCompile || status === "compiled" || status === "published";
  const step3 = status === "published" || Boolean(manifestName);

  let nextAction = "Validate the graph to begin.";
  if (validationValid === false) {
    nextAction = "Fix validation errors, then validate again.";
  } else if (!step2) {
    nextAction = "Compile to generate Python and Flink artifacts.";
  } else if (!step3) {
    nextAction = "Add to catalog to use this agent in Studio pipelines.";
  } else {
    nextAction = "Published — run a local test or open the catalog entry.";
  }

  return (
    <div className="designer-publish-steps card" aria-label="Publish workflow">
      <div className="designer-publish-step-row">
        <div className={`designer-step-card ${step1 ? "done" : validationValid === false ? "blocked" : ""}`}>
          <span className="designer-step-label">1. Validate</span>
          <span className={`badge ${step1 ? "ok" : validationValid === false ? "bad" : "warn"}`}>
            {step1 ? "Done" : validationValid === false ? "Failed" : "Pending"}
          </span>
          <button
            type="button"
            className="secondary designer-step-action"
            disabled={busy !== null}
            onClick={onValidate}
          >
            {busy === "validate" ? "Validating…" : "Validate"}
          </button>
        </div>
        <span className="designer-step-arrow muted">→</span>
        <div className={`designer-step-card ${step2 ? "done" : ""}`}>
          <span className="designer-step-label">2. Compile</span>
          <span className={`badge ${step2 ? "ok" : "warn"}`}>{step2 ? "Done" : "Pending"}</span>
          <button
            type="button"
            className="designer-step-action"
            disabled={busy !== null || compileBlocked}
            onClick={onCompile}
          >
            {busy === "compile" ? "Compiling…" : "Compile"}
          </button>
        </div>
        <span className="designer-step-arrow muted">→</span>
        <div className={`designer-step-card ${step3 ? "done" : ""}`}>
          <span className="designer-step-label">3. Add to catalog</span>
          <span className={`badge ${step3 ? "ok" : "warn"}`}>{step3 ? "Done" : "Pending"}</span>
          <button
            type="button"
            className="secondary designer-step-action"
            disabled={busy !== null || compileBlocked}
            onClick={onPublish}
          >
            {busy === "publish" ? "Publishing…" : "Add to catalog"}
          </button>
        </div>
      </div>
      <p className="muted designer-publish-next" role="status">
        Next: {nextAction}
        {manifestName ? (
          <>
            {" "}
            Catalog entry: <code>{manifestName}</code>
          </>
        ) : null}
      </p>
    </div>
  );
}
