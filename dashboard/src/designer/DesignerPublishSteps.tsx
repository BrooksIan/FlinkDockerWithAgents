interface Props {
  status: string | undefined;
  validationValid: boolean | null;
  hasCompile: boolean;
  manifestName?: string | null;
}

export function DesignerPublishSteps({
  status,
  validationValid,
  hasCompile,
  manifestName,
}: Props) {
  const step1 = validationValid === true;
  const step2 = hasCompile || status === "compiled" || status === "published";
  const step3 = status === "published" || Boolean(manifestName);

  return (
    <div className="designer-publish-steps card" aria-label="Publish workflow">
      <span className={`designer-step ${step1 ? "done" : ""}`}>1. Validate</span>
      <span className="designer-step-arrow muted">→</span>
      <span className={`designer-step ${step2 ? "done" : ""}`}>2. Compile</span>
      <span className="designer-step-arrow muted">→</span>
      <span className={`designer-step ${step3 ? "done" : ""}`}>3. Add to catalog</span>
      <p className="muted" style={{ margin: "0.5rem 0 0", fontSize: "0.85rem" }}>
        Publish auto-compiles first. After catalog publish, run locally from here or use the agent
        in Studio pipelines.
      </p>
    </div>
  );
}
