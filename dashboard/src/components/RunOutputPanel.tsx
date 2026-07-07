import { useEffect, useMemo, useState } from "react";
import { api } from "../api/client";
import type { KafkaTopicRecordsResponse, SpanSummary } from "../api/types";

function recordPayload(record: unknown): unknown {
  if (!record || typeof record !== "object") return record;
  const obj = record as Record<string, unknown>;
  if ("output" in obj) return obj.output;
  if ("value" in obj) return obj.value;
  if ("v" in obj) return obj.v;
  return record;
}

export function flattenOutput(output: unknown): Record<string, unknown>[] {
  if (!Array.isArray(output)) {
    const payload = recordPayload(output);
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
      return [payload as Record<string, unknown>];
    }
    return output && typeof output === "object"
      ? [output as Record<string, unknown>]
      : [];
  }
  return output.map((item) => {
    const payload = recordPayload(item);
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
      return payload as Record<string, unknown>;
    }
    return { value: payload };
  });
}

function columnsForRows(rows: Record<string, unknown>[]): string[] {
  const keys = new Set<string>();
  for (const row of rows) {
    for (const [key, value] of Object.entries(row)) {
      if (value !== undefined && value !== null && value !== "") {
        keys.add(key);
      }
    }
  }
  return Array.from(keys);
}

function formatCell(value: unknown): string {
  if (value === undefined || value === null) return "—";
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function sinkSpan(spans: SpanSummary[]): SpanSummary | undefined {
  return spans.find((s) => s.kind === "sink");
}

function kafkaTopicsFromRun(
  spans: SpanSummary[],
  sinkType: unknown,
  sinkTopic: unknown,
): string[] {
  const topics = new Set<string>();
  if (sinkType === "kafka" && sinkTopic) {
    topics.add(String(sinkTopic));
  }
  for (const span of spans) {
    if (span.kind !== "agent") continue;
    for (const row of flattenOutput(span.output)) {
      const topic = row.kafka_topic;
      if (topic && (row.kafka_written === true || row.kafka_written === undefined)) {
        topics.add(String(topic));
      }
    }
  }
  return [...topics];
}

function OutputTable({
  rows,
  emptyMessage,
}: {
  rows: Record<string, unknown>[];
  emptyMessage?: string;
}) {
  const columns = columnsForRows(rows);
  if (rows.length === 0) {
    return emptyMessage ? <p className="muted">{emptyMessage}</p> : null;
  }
  if (columns.length === 0) {
    return <pre className="yaml run-output-json">{JSON.stringify(rows, null, 2)}</pre>;
  }
  return (
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
                <td key={col} className="run-output-cell">
                  {formatCell(row[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

interface Props {
  output: unknown;
  spans?: SpanSummary[];
  recordCount?: number;
}

export function RunOutputPanel({ output, spans = [], recordCount }: Props) {
  const sink = sinkSpan(spans);
  const sinkInput =
    sink?.input && typeof sink.input === "object"
      ? (sink.input as Record<string, unknown>)
      : null;
  const sinkType = sinkInput?.sink_type;
  const sinkTopic = sinkInput?.topic;

  const pipelineRows = flattenOutput(output);
  const kafkaTopics = useMemo(
    () => kafkaTopicsFromRun(spans, sinkType, sinkTopic),
    [spans, sinkType, sinkTopic],
  );

  const [kafkaSamples, setKafkaSamples] = useState<KafkaTopicRecordsResponse[]>([]);
  const [kafkaLoading, setKafkaLoading] = useState(false);
  const [kafkaError, setKafkaError] = useState<string | null>(null);

  useEffect(() => {
    if (kafkaTopics.length === 0) {
      setKafkaSamples([]);
      setKafkaError(null);
      return;
    }
    let cancelled = false;
    setKafkaLoading(true);
    setKafkaError(null);
    const limit = Math.max(recordCount ?? 10, 10);
    Promise.all(
      kafkaTopics.map((topic) => api.kafkaTopicRecords(topic, { limit })),
    )
      .then((results) => {
        if (!cancelled) setKafkaSamples(results);
      })
      .catch((err) => {
        if (!cancelled) {
          setKafkaSamples([]);
          setKafkaError(String(err));
        }
      })
      .finally(() => {
        if (!cancelled) setKafkaLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [kafkaTopics.join("|"), recordCount]);

  const kafkaRows = useMemo(
    () => kafkaSamples.flatMap((sample) => flattenOutput(sample.records)),
    [kafkaSamples],
  );

  const displayRows = kafkaRows.length > 0 ? kafkaRows : pipelineRows;
  const totalRecords = recordCount ?? displayRows.length;

  if (!output && displayRows.length === 0 && kafkaTopics.length === 0) {
    return (
      <div className="card">
        <h3 style={{ marginTop: 0 }}>Output</h3>
        <p className="muted">No output records for this run.</p>
      </div>
    );
  }

  return (
    <div className="card run-output-panel">
      <div className="run-output-header">
        <h3 style={{ margin: 0 }}>Output</h3>
        <span className="muted">
          {totalRecords} record{totalRecords === 1 ? "" : "s"}
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

      {kafkaTopics.length > 0 && (
        <p className="run-output-kafka-topics muted">
          Kafka topic{kafkaTopics.length === 1 ? "" : "s"}:{" "}
          {kafkaTopics.map((t, i) => (
            <span key={t}>
              {i > 0 ? ", " : ""}
              <code>{t}</code>
            </span>
          ))}
          {kafkaLoading ? " — loading messages…" : ""}
        </p>
      )}

      {kafkaError && (
        <p className="error run-output-kafka-error">
          Could not read Kafka messages: {kafkaError}
        </p>
      )}

      {displayRows.some((row) => row.mode === "fallback") && (
        <p className="error run-output-fallback-note">
          LLM fallback was used
          {displayRows.map((row) => row.fallback_reason).find(Boolean) ? (
            <>
              {": "}
              {String(displayRows.map((row) => row.fallback_reason).find(Boolean))}
            </>
          ) : (
            <> — the agent could not parse an LLM JSON response. Recompile after updating prompts to request JSON output.</>
          )}
        </p>
      )}

      {displayRows.some((row) => row.mode === "llm" || row.react_mode === "llm") && (
        <p className="run-output-llm-note muted">LLM path was used for this run.</p>
      )}

      {kafkaRows.length > 0 ? (
        <>
          <h4 className="run-output-section-title">Kafka messages</h4>
          <OutputTable rows={kafkaRows} />
          {pipelineRows.length > 0 && kafkaRows.length !== pipelineRows.length && (
            <>
              <h4 className="run-output-section-title">Pipeline capture</h4>
              <OutputTable rows={pipelineRows} />
            </>
          )}
        </>
      ) : (
        <OutputTable
          rows={pipelineRows}
          emptyMessage={kafkaLoading ? "Loading Kafka messages…" : undefined}
        />
      )}

      <details className="run-output-raw">
        <summary className="muted">Raw JSON</summary>
        <pre className="yaml">
          {JSON.stringify(
            kafkaSamples.length > 0
              ? { pipeline: output, kafka: kafkaSamples }
              : output,
            null,
            2,
          )}
        </pre>
      </details>
    </div>
  );
}
