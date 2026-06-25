import type { SpanSummary } from "../api/types";

function recordPayload(record: unknown): unknown {
  if (!record || typeof record !== "object") return record;
  const obj = record as Record<string, unknown>;
  if ("output" in obj) return obj.output;
  if ("value" in obj) return obj.value;
  return record;
}

function flattenOutput(output: unknown): Record<string, unknown>[] {
  if (!Array.isArray(output)) {
    return output && typeof output === "object" ? [output as Record<string, unknown>] : [];
  }
  return output.map((item) => {
    const payload = recordPayload(item);
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
      return payload as Record<string, unknown>;
    }
    return { value: payload };
  });
}

function sinkSpan(spans: SpanSummary[]): SpanSummary | undefined {
  return spans.find((s) => s.kind === "sink");
}

interface Props {
  output: unknown;
  spans?: SpanSummary[];
  recordCount?: number;
}

export function RunOutputPanel({ output, spans = [], recordCount }: Props) {
  const rows = flattenOutput(output);
  const sink = sinkSpan(spans);
  const sinkInput =
    sink?.input && typeof sink.input === "object"
      ? (sink.input as Record<string, unknown>)
      : null;
  const sinkType = sinkInput?.sink_type;
  const sinkTopic = sinkInput?.topic;

  if (!output && rows.length === 0) {
    return (
      <div className="card">
        <h3 style={{ marginTop: 0 }}>Output</h3>
        <p className="muted">No output records for this run.</p>
      </div>
    );
  }

  const columns =
    rows.length > 0
      ? Array.from(new Set(rows.flatMap((row) => Object.keys(row))))
      : [];

  return (
    <div className="card run-output-panel">
      <div className="run-output-header">
        <h3 style={{ margin: 0 }}>Output</h3>
        <span className="muted">
          {recordCount ?? rows.length} record{(recordCount ?? rows.length) === 1 ? "" : "s"}
        </span>
      </div>

      {sink && (
        <p className="run-output-sink-note muted">
          {sinkType === "kafka" && sinkTopic ? (
            <>
              Delivered to Kafka topic <code>{String(sinkTopic)}</code>
            </>
          ) : (
            <>Captured in run result (capture sink)</>
          )}
        </p>
      )}

      {columns.length > 0 ? (
        <div className="run-output-table-wrap">
          <table className="run-output-table">
            <thead>
              <tr>
                <th>#</th>
                {columns.map((col) => (
                  <th key={col}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr key={idx}>
                  <td className="muted">{idx + 1}</td>
                  {columns.map((col) => (
                    <td key={col}>
                      {row[col] === undefined || row[col] === null
                        ? "—"
                        : typeof row[col] === "object"
                          ? JSON.stringify(row[col])
                          : String(row[col])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <pre className="yaml run-output-json">{JSON.stringify(output, null, 2)}</pre>
      )}

      <details className="run-output-raw">
        <summary className="muted">Raw JSON</summary>
        <pre className="yaml">{JSON.stringify(output, null, 2)}</pre>
      </details>
    </div>
  );
}
