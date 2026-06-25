import type { PlanStep } from "../api/types";

export function ExecutionPlan({ plan }: { plan: PlanStep[] }) {
  if (plan.length === 0) {
    return <p className="muted">No execution plan available.</p>;
  }

  const roots = plan.filter((s) => !s.parent);

  function children(parent: string) {
    return plan.filter((s) => s.parent === parent);
  }

  function renderStep(step: PlanStep, depth = 0) {
    const nested = children(step.name);
    return (
      <li key={`${step.kind}-${step.name}`} className="plan-step" style={{ marginLeft: depth * 16 }}>
        <span className="badge neutral">{step.kind}</span> <strong>{step.name}</strong>
        {step.description && <span className="muted"> — {step.description}</span>}
        {nested.length > 0 && <ul className="plan-tree">{nested.map((c) => renderStep(c, depth + 1))}</ul>}
      </li>
    );
  }

  return (
    <div className="card">
      <h3 style={{ marginTop: 0 }}>Expected execution plan</h3>
      <p className="muted">Declarative steps until runtime tracing (Phase 2) records live spans.</p>
      <ul className="plan-tree">{roots.map((s) => renderStep(s))}</ul>
    </div>
  );
}
