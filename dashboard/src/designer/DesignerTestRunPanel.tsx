import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import type { AgentDefinition, AgentDefinitionRunResult } from "../api/types";

const DEFAULT_WORKFLOW_RECORDS = '[\n  {"key": "1", "value": 3},\n  {"key": "2", "value": 10}\n]';

const DEFAULT_REACT_RECORDS =
  '[\n  {"key": "1", "message": "Please double the input value 7"},\n  {"key": "2", "message": "process value 21", "value": 21}\n]';

interface Props {
  definition: AgentDefinition | null;
  busy: boolean;
  lastResult: AgentDefinitionRunResult | null;
  suggestedRecords?: Record<string, unknown>[];
  onRun: (records: Record<string, unknown>[]) => void;
}

function defaultRecordsText(definition: AgentDefinition | null): string {
  if (!definition) return DEFAULT_WORKFLOW_RECORDS;
  if (definition.type === "react") {
    const required = (definition.input_schema?.required as string[] | undefined) || [];
    const properties = definition.input_schema?.properties as Record<string, unknown> | undefined;
    if (required.includes("message") || properties?.message) {
      return DEFAULT_REACT_RECORDS;
    }
  }
  return DEFAULT_WORKFLOW_RECORDS;
}

export function DesignerTestRunPanel({
  definition,
  busy,
  lastResult,
  suggestedRecords,
  onRun,
}: Props) {
  const [recordsText, setRecordsText] = useState(defaultRecordsText(definition));
  const [parseError, setParseError] = useState<string | null>(null);

  useEffect(() => {
    setRecordsText(defaultRecordsText(definition));
    setParseError(null);
  }, [definition?.id, definition?.type, definition?.input_schema]);

  useEffect(() => {
    if (suggestedRecords && suggestedRecords.length > 0) {
      setRecordsText(JSON.stringify(suggestedRecords, null, 2));
      setParseError(null);
    }
  }, [suggestedRecords]);

  const parsedPreview = useMemo(() => {
    try {
      const parsed = JSON.parse(recordsText) as unknown;
      if (!Array.isArray(parsed)) return null;
      return parsed;
    } catch {
      return null;
    }
  }, [recordsText]);

  function handleRun() {
    try {
      const parsed = JSON.parse(recordsText) as unknown;
      if (!Array.isArray(parsed)) {
        setParseError("Test input must be a JSON array of records.");
        return;
      }
      setParseError(null);
      onRun(parsed as Record<string, unknown>[]);
    } catch {
      setParseError("Invalid JSON. Provide an array of input records.");
    }
  }

  return (
    <section className="card designer-test-run-panel">
      <div className="designer-test-run-header">
        <h3 style={{ margin: 0 }}>Test run</h3>
        <button type="button" disabled={busy || !definition} onClick={handleRun}>
          {busy ? "Running…" : "Run with test input"}
        </button>
      </div>
      <p className="muted designer-test-run-hint">
        Provide sample input records as JSON. Results appear below after the local run completes.
      </p>
      <label className="studio-label">Test input records</label>
      <textarea
        className="studio-textarea designer-test-run-input"
        rows={6}
        value={recordsText}
        onChange={(e) => {
          setRecordsText(e.target.value);
          setParseError(null);
        }}
      />
      {parseError && <p className="error">{parseError}</p>}
      {!parseError && parsedPreview && (
        <p className="muted designer-test-run-preview">
          {parsedPreview.length} record(s) ready to run.
        </p>
      )}

      {lastResult && (
        <div className={`designer-test-run-result ${lastResult.return_code === 0 ? "ok" : "bad"}`}>
          <p style={{ margin: 0 }}>
            <span className={`badge ${lastResult.return_code === 0 ? "ok" : "bad"}`}>
              {lastResult.return_code === 0 ? "Passed" : "Failed"}
            </span>{" "}
            Run <code>{lastResult.run_id}</code> · exit {lastResult.return_code}
          </p>
          {lastResult.output != null && (
            <>
              <span className="studio-label">Output</span>
              <pre className="designer-test-run-output">
                {JSON.stringify(lastResult.output, null, 2)}
              </pre>
            </>
          )}
          {lastResult.stdout && (
            <>
              <span className="studio-label">Stdout</span>
              <pre className="designer-test-run-output">{lastResult.stdout}</pre>
            </>
          )}
          {lastResult.stderr && (
            <>
              <span className="studio-label">Stderr</span>
              <pre className="designer-test-run-output error">{lastResult.stderr}</pre>
            </>
          )}
          <Link to={`/runs/${lastResult.run_id}`} className="secondary-link">
            Open full run trace
          </Link>
        </div>
      )}
    </section>
  );
}
