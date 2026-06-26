import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { DesignerSkill } from "../api/types";
import { llmConfigFromNode, mergeAllowedCommands } from "./llmCallConfig";

interface Props {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export function DesignerLlmCallFields({ config, onChange }: Props) {
  const [skillCatalog, setSkillCatalog] = useState<DesignerSkill[]>([]);
  const [skillsError, setSkillsError] = useState<string | null>(null);
  const llm = llmConfigFromNode(config);

  useEffect(() => {
    api
      .designerSkills()
      .then(setSkillCatalog)
      .catch((err) => setSkillsError(String(err)));
  }, []);

  return (
    <>
      <label className="studio-label">Execution mode</label>
      <select
        className="studio-select"
        value={llm.mode || "simple"}
        onChange={(e) => {
          const mode = e.target.value === "flink_skills" ? "flink_skills" : "simple";
          const next: Record<string, unknown> = { ...config, mode };
          if (mode === "flink_skills" && (!llm.skills || llm.skills.length === 0)) {
            const skills = skillCatalog.length ? [skillCatalog[0].id] : ["math-calculator"];
            next.skills = skills;
            next.allowed_commands = mergeAllowedCommands(
              skills,
              skillCatalog,
              llm.allowed_commands || [],
            );
          }
          onChange(next);
        }}
      >
        <option value="simple">Simple (HTTP LLM)</option>
        <option value="flink_skills">Flink skills (native chat model)</option>
      </select>

      {llm.mode === "simple" ? (
        <>
          <label className="studio-label designer-checkbox-field">
            <input
              type="checkbox"
              checked={llm.use_platform_llm !== false}
              onChange={(e) => onChange({ ...config, use_platform_llm: e.target.checked })}
            />{" "}
            Use platform LLM from Settings
          </label>
          <p className="muted" style={{ fontSize: "0.85rem" }}>
            Direct OpenAI-compatible call. Compile appends JSON response rules to the system prompt.
          </p>
        </>
      ) : (
        <>
          <p className="muted" style={{ fontSize: "0.85rem" }}>
            Native <code>@chat_model_setup</code> with <code>load_skill</code> and <code>bash</code>.
            Configure endpoint and model in <Link to="/settings">Settings</Link>.
          </p>
          {skillsError ? (
            <p className="error" style={{ fontSize: "0.85rem" }}>
              {skillsError}
            </p>
          ) : null}
          <label className="studio-label">Skills</label>
          {skillCatalog.length === 0 ? (
            <p className="muted" style={{ fontSize: "0.85rem" }}>
              No skills found under <code>examples/skills/</code>.
            </p>
          ) : (
            <div className="designer-skill-list">
              {skillCatalog.map((skill) => {
                const selected = (llm.skills || []).includes(skill.id);
                return (
                  <label key={skill.id} className="designer-skill-option">
                    <input
                      type="checkbox"
                      checked={selected}
                      onChange={(e) => {
                        const current = llm.skills || [];
                        const nextSkills = e.target.checked
                          ? [...current, skill.id]
                          : current.filter((item) => item !== skill.id);
                        onChange({
                          ...config,
                          mode: "flink_skills",
                          skills: nextSkills,
                          allowed_commands: mergeAllowedCommands(
                            nextSkills,
                            skillCatalog,
                            llm.allowed_commands || [],
                          ),
                        });
                      }}
                    />
                    <span>
                      <strong>{skill.name}</strong>
                      {skill.description ? (
                        <span className="muted"> — {skill.description}</span>
                      ) : null}
                    </span>
                  </label>
                );
              })}
            </div>
          )}
          <label className="studio-label">Allowed bash commands</label>
          <input
            className="studio-input"
            type="text"
            value={(llm.allowed_commands || []).join(", ")}
            placeholder="echo, bc"
            onChange={(e) => {
              const allowed_commands = e.target.value
                .split(",")
                .map((item) => item.trim())
                .filter(Boolean);
              onChange({ ...config, mode: "flink_skills", allowed_commands });
            }}
          />
          <p className="muted" style={{ fontSize: "0.85rem" }}>
            Whitelist for the built-in <code>bash</code> tool. Auto-filled from selected skills.
          </p>
        </>
      )}
    </>
  );
}
