import type { AgentCatalog, AgentSummary, KafkaTopicSummary } from "../api/types";

const DEFAULT_RECORDS = '[\n  { "key": "1", "value": 3 },\n  { "key": "2", "value": 10 }\n]';

interface Props {
  agents: AgentSummary[];
  catalog: AgentCatalog | null;
  kafkaTopics: KafkaTopicSummary[];
  kafkaReachable?: boolean;
  onAddSource: () => void;
  onAddKafkaSource: () => void;
  onAddSink: () => void;
  onAddKafkaSink: () => void;
  onAddAgent: (agent: AgentSummary) => void;
}

function dragPayload(
  kind: "source" | "agent" | "sink",
  extra?: { agent?: AgentSummary; kafkaSource?: boolean; kafkaSink?: boolean },
) {
  return JSON.stringify({
    kind,
    agent: extra?.agent?.name,
    agentType: extra?.agent?.type,
    description: extra?.agent?.description,
    displayName: extra?.agent?.display_name,
    kafkaSource: extra?.kafkaSource ?? false,
    kafkaSink: extra?.kafkaSink ?? false,
  });
}

function PaletteItem({
  label,
  sub,
  payload,
  onClick,
  className,
}: {
  label: string;
  sub?: string;
  payload: string;
  onClick: () => void;
  className?: string;
}) {
  return (
    <div
      className={`studio-palette-item${className ? ` ${className}` : ""}`}
      draggable
      onDragStart={(e) => {
        e.dataTransfer.setData("application/reactflow", payload);
        e.dataTransfer.effectAllowed = "move";
      }}
      onClick={onClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onClick();
        }
      }}
    >
      <span className="studio-palette-item-label">{label}</span>
      {sub && <span className="studio-palette-item-sub muted">{sub}</span>}
    </div>
  );
}

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

export function NodePalette({
  agents,
  catalog,
  kafkaTopics,
  kafkaReachable,
  onAddSource,
  onAddKafkaSource,
  onAddSink,
  onAddKafkaSink,
  onAddAgent,
}: Props) {
  const agentsByName = new Map(agents.map((a) => [a.name, a]));

  return (
    <div className="studio-palette card">
      <h3 style={{ marginTop: 0 }}>Palette</h3>
      <p className="muted">Drag nodes onto the canvas. Connect by dragging between the blue dots (left → right).</p>

      <h4>Sources</h4>
      <div className="studio-palette-actions">
        <PaletteItem
          label="+ Static records"
          sub="JSON input array"
          payload={dragPayload("source")}
          onClick={onAddSource}
        />
        <PaletteItem
          label="+ Kafka topic"
          sub={
            kafkaTopics.length > 0
              ? `${kafkaTopics.length} topic(s) — pick in inspector`
              : "Start full stack for topics"
          }
          payload={dragPayload("source", { kafkaSource: true })}
          onClick={onAddKafkaSource}
          className="studio-palette-kafka"
        />
      </div>
      {kafkaReachable === false && kafkaTopics.length > 0 && (
        <p className="muted" style={{ fontSize: "0.8rem" }}>
          Broker offline — topic list may be stale until{" "}
          <code>apemosyne up --profile full</code> is healthy.
        </p>
      )}
      {kafkaTopics.length === 0 && (
        <p className="muted" style={{ fontSize: "0.8rem" }}>
          Kafka topics unavailable. Start the full stack: <code>apemosyne up --profile full</code>
        </p>
      )}

      <h4>Sinks</h4>
      <div className="studio-palette-actions">
        <PaletteItem
          label="+ Capture output"
          sub="Return in run result"
          payload={dragPayload("sink")}
          onClick={onAddSink}
        />
        <PaletteItem
          label="+ Kafka topic"
          sub={
            kafkaTopics.length > 0
              ? `${kafkaTopics.length} topic(s) — pick in inspector`
              : "Start full stack for topics"
          }
          payload={dragPayload("sink", { kafkaSink: true })}
          onClick={onAddKafkaSink}
          className="studio-palette-kafka"
        />
      </div>

      <h4>Agents</h4>
      {catalog ? (
        catalog.categories.map((category) => (
          <div key={category.id} className="studio-palette-category">
            <p className="studio-palette-category-label">{category.label}</p>
            {category.subcategories.map((sub) => (
              <div key={sub.id} className="studio-palette-subcategory">
                <p className="muted studio-palette-subcategory-label">{sub.label}</p>
                <ul className="studio-agent-list">
                  {sub.agents.map((entry) => {
                    const agent =
                      agentsByName.get(entry.manifest) ??
                      agentSummaryFromCatalog(
                        entry.manifest,
                        entry.display_name,
                        entry.description,
                        entry.type,
                      );
                    const label = entry.display_name || agent.display_name || agent.name;
                    return (
                      <li key={entry.id}>
                        <PaletteItem
                          label={`+ ${label}`}
                          sub={agent.name}
                          payload={dragPayload("agent", { agent })}
                          onClick={() => onAddAgent(agent)}
                        />
                      </li>
                    );
                  })}
                </ul>
              </div>
            ))}
          </div>
        ))
      ) : (
        <ul className="studio-agent-list">
          {agents.map((a) => (
            <li key={a.name}>
              <PaletteItem
                label={`+ ${a.display_name || a.name}`}
                sub={a.type}
                payload={dragPayload("agent", { agent: a })}
                onClick={() => onAddAgent(a)}
              />
            </li>
          ))}
        </ul>
      )}
      <p className="muted" style={{ fontSize: "0.8rem", marginTop: "1rem" }}>
        Default static records:
        <pre className="yaml" style={{ marginTop: "0.5rem" }}>
          {DEFAULT_RECORDS}
        </pre>
      </p>
    </div>
  );
}
