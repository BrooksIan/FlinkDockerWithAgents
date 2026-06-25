import type { SpanSummary } from "../api/types";

function JsonBlock({ data }: { data: unknown }) {
  if (data === undefined || data === null) return null;
  return <pre className="yaml span-json">{JSON.stringify(data, null, 2)}</pre>;
}

interface Props {
  spans: SpanSummary[];
}

export function RunSpanList({ spans }: Props) {
  if (spans.length === 0) {
    return <p className="muted">No runtime spans recorded for this run.</p>;
  }

  return (
    <div className="card">
      <h3 style={{ marginTop: 0 }}>Execution steps</h3>
      <ul className="run-span-list">
        {spans.map((s) => (
          <li key={s.id} className="run-span-item">
            <div className="run-span-header">
              <span className={`badge ${s.kind === "sink" ? "ok" : "neutral"}`}>{s.kind}</span>
              <strong>{s.name}</strong>
              {s.duration_ms != null && <span className="muted"> ({s.duration_ms}ms)</span>}
            </div>
            {(s.input !== undefined && s.input !== null) || (s.output !== undefined && s.output !== null) ? (
              <details className="run-span-details">
                <summary className="muted">Input / output</summary>
                {s.input !== undefined && s.input !== null && (
                  <>
                    <p className="muted span-io-label">Input</p>
                    <JsonBlock data={s.input} />
                  </>
                )}
                {s.output !== undefined && s.output !== null && (
                  <>
                    <p className="muted span-io-label">Output</p>
                    <JsonBlock data={s.output} />
                  </>
                )}
              </details>
            ) : null}
          </li>
        ))}
      </ul>
    </div>
  );
}
