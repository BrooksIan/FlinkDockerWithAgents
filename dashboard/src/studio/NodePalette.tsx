import type { AgentSummary, KafkaTopicSummary } from "../api/types";

const DEFAULT_RECORDS = '[\n  { "key": "1", "value": 3 },\n  { "key": "2", "value": 10 }\n]';

interface Props {
  agents: AgentSummary[];
  kafkaTopics: KafkaTopicSummary[];
  kafkaReachable?: boolean;
  onAddSource: () => void;
  onAddKafkaSource: (topic: KafkaTopicSummary) => void;
  onAddSink: () => void;
  onAddAgent: (agent: AgentSummary) => void;
}

function dragPayload(
  kind: "source" | "agent" | "sink",
  extra?: { agent?: AgentSummary; kafkaTopic?: KafkaTopicSummary },
) {
  return JSON.stringify({
    kind,
    agent: extra?.agent?.name,
    agentType: extra?.agent?.type,
    description: extra?.agent?.description,
    kafkaTopic: extra?.kafkaTopic?.name,
    kafkaDescription: extra?.kafkaTopic?.description,
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

export function NodePalette({
  agents,
  kafkaTopics,
  kafkaReachable,
  onAddSource,
  onAddKafkaSource,
  onAddSink,
  onAddAgent,
}: Props) {
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
      </div>
      {kafkaTopics.length > 0 ? (
        <ul className="studio-agent-list">
          {kafkaTopics.map((topic) => (
            <li key={topic.name}>
              <PaletteItem
                label={`+ ${topic.name}`}
                sub={topic.description}
                payload={dragPayload("source", { kafkaTopic: topic })}
                onClick={() => onAddKafkaSource(topic)}
                className="studio-palette-kafka"
              />
            </li>
          ))}
        </ul>
      ) : (
        <p className="muted" style={{ fontSize: "0.8rem" }}>
          Kafka topics unavailable. Start the full stack: <code>apemosyne up --profile full</code>
        </p>
      )}
      {kafkaReachable === false && kafkaTopics.length > 0 && (
        <p className="muted" style={{ fontSize: "0.8rem" }}>
          Broker offline — topics listed from pipeline config; local Kafka runs need the stack up.
        </p>
      )}

      <h4>Flow</h4>
      <div className="studio-palette-actions">
        <PaletteItem label="+ Sink" sub="Output" payload={dragPayload("sink")} onClick={onAddSink} />
      </div>

      <h4>Agents</h4>
      <ul className="studio-agent-list">
        {agents.map((a) => (
          <li key={a.name}>
            <PaletteItem
              label={`+ ${a.name}`}
              sub={a.type}
              payload={dragPayload("agent", { agent: a })}
              onClick={() => onAddAgent(a)}
            />
          </li>
        ))}
      </ul>
      <p className="muted" style={{ fontSize: "0.8rem", marginTop: "1rem" }}>
        Default static records:
        <pre className="yaml" style={{ marginTop: "0.5rem" }}>
          {DEFAULT_RECORDS}
        </pre>
      </p>
    </div>
  );
}
