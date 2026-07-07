import { useState, type ReactNode } from "react";
import type { AgentCatalog, AgentSummary, KafkaTopicSummary } from "../api/types";

interface Props {
  agents: AgentSummary[];
  catalog: AgentCatalog | null;
  kafkaTopics: KafkaTopicSummary[];
  kafkaReachable?: boolean;
  onAddSource: () => void;
  onAddKafkaSource: () => void;
  onAddWindow: () => void;
  onAddSink: () => void;
  onAddKafkaSink: () => void;
  onAddAgent: (agent: AgentSummary) => void;
}

type SourceChoice = "records" | "kafka";
type SinkChoice = "capture" | "kafka";

function agentSummaryFromCatalog(
  manifest: string,
  displayName: string,
  description: string,
  type: string,
): AgentSummary {
  return {
    name: manifest,
    type,
    description,
    display_name: displayName,
    entry: "",
    runner: "",
    cluster_script: "",
  };
}

function PaletteSection({
  title,
  hint,
  children,
}: {
  title: string;
  hint?: string;
  children: ReactNode;
}) {
  return (
    <section className="studio-palette-section">
      <label className="studio-label">{title}</label>
      {hint && (
        <p className="muted studio-palette-hint">{hint}</p>
      )}
      {children}
    </section>
  );
}

export function NodePalette({
  agents,
  catalog,
  kafkaTopics,
  kafkaReachable,
  onAddSource,
  onAddKafkaSource,
  onAddWindow,
  onAddSink,
  onAddKafkaSink,
  onAddAgent,
}: Props) {
  const agentsByName = new Map(agents.map((a) => [a.name, a]));
  const [sourceChoice, setSourceChoice] = useState<SourceChoice | "">("");
  const [agentChoice, setAgentChoice] = useState("");
  const [sinkChoice, setSinkChoice] = useState<SinkChoice | "">("");

  const kafkaHint =
    kafkaTopics.length > 0
      ? `${kafkaTopics.length} topic(s) — configure in inspector`
      : "Start Studio Kafka for topics";

  function resolveAgent(manifest: string): AgentSummary | undefined {
    const fromList = agentsByName.get(manifest);
    if (fromList) return fromList;
    if (!catalog) return undefined;
    for (const category of catalog.categories) {
      for (const sub of category.subcategories) {
        const entry = sub.agents.find((a) => a.manifest === manifest);
        if (entry) {
          return agentSummaryFromCatalog(
            entry.manifest,
            entry.display_name,
            entry.description,
            entry.type,
          );
        }
      }
    }
    return undefined;
  }

  function handleAddSource() {
    if (sourceChoice === "records") onAddSource();
    else if (sourceChoice === "kafka") onAddKafkaSource();
    setSourceChoice("");
  }

  function handleAddAgent() {
    if (!agentChoice) return;
    const agent = resolveAgent(agentChoice);
    if (agent) onAddAgent(agent);
    setAgentChoice("");
  }

  function handleAddSink() {
    if (sinkChoice === "capture") onAddSink();
    else if (sinkChoice === "kafka") onAddKafkaSink();
    setSinkChoice("");
  }

  return (
    <div className="studio-palette card">
      <h3 style={{ marginTop: 0 }}>Palette</h3>
      <p className="muted studio-palette-intro">
        Add nodes from the menus below, then connect handles left → right on the canvas.
      </p>

      <PaletteSection title="Source" hint="Optional for self-sourcing agents (e.g. API Fetch).">
        <div className="studio-palette-row">
          <select
            className="studio-select"
            value={sourceChoice}
            onChange={(e) => setSourceChoice(e.target.value as SourceChoice | "")}
          >
            <option value="">Choose source…</option>
            <option value="records">Static records</option>
            <option value="kafka">Kafka topic</option>
          </select>
          <button
            type="button"
            className="secondary studio-palette-add"
            disabled={!sourceChoice}
            onClick={handleAddSource}
          >
            Add
          </button>
        </div>
      </PaletteSection>

      <PaletteSection title="Window" hint="Groups events by key; closes on inactivity.">
        <button type="button" className="secondary studio-palette-full" onClick={onAddWindow}>
          Add session window
        </button>
      </PaletteSection>

      <PaletteSection title="Agent">
        <div className="studio-palette-row">
          <select
            className="studio-select"
            value={agentChoice}
            onChange={(e) => setAgentChoice(e.target.value)}
          >
            <option value="">Choose agent…</option>
            {catalog ? (
              catalog.categories.map((category) =>
                category.subcategories.map((sub) => (
                  <optgroup
                    key={`${category.id}-${sub.id}`}
                    label={`${category.label} · ${sub.label}`}
                  >
                    {sub.agents.map((entry) => (
                      <option key={entry.id} value={entry.manifest}>
                        {entry.display_name || entry.manifest}
                      </option>
                    ))}
                  </optgroup>
                )),
              )
            ) : (
              agents.map((a) => (
                <option key={a.name} value={a.name}>
                  {a.display_name || a.name}
                </option>
              ))
            )}
          </select>
          <button
            type="button"
            className="secondary studio-palette-add"
            disabled={!agentChoice}
            onClick={handleAddAgent}
          >
            Add
          </button>
        </div>
      </PaletteSection>

      <PaletteSection title="Sink" hint={kafkaHint}>
        <div className="studio-palette-row">
          <select
            className="studio-select"
            value={sinkChoice}
            onChange={(e) => setSinkChoice(e.target.value as SinkChoice | "")}
          >
            <option value="">Choose sink…</option>
            <option value="capture">Capture output</option>
            <option value="kafka">Kafka topic</option>
          </select>
          <button
            type="button"
            className="secondary studio-palette-add"
            disabled={!sinkChoice}
            onClick={handleAddSink}
          >
            Add
          </button>
        </div>
      </PaletteSection>

      {kafkaReachable === false && kafkaTopics.length > 0 && (
        <p className="muted studio-palette-footnote">
          Kafka broker offline — topic list may be stale.
        </p>
      )}
      {kafkaTopics.length === 0 && (
        <p className="muted studio-palette-footnote">
          No Kafka topics yet. Run <code>ratatoskr kafka up</code>.
        </p>
      )}
    </div>
  );
}
