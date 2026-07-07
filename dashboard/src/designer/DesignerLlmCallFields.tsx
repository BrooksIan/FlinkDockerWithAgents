import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { DesignerSkill } from "../api/types";
import { llmConfigFromNode, mergeAllowedCommands } from "./llmCallConfig";

interface Props {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

const SKILL_TEMPLATE = `---
name: my-skill
description: What this skill does and when the agent should use it.
license: Apache-2.0
compatibility: Requires bash with <tools>
---

# My Skill

## When to Use
Describe when the agent should load this skill.

## Method
Explain the steps, e.g.:

\`\`\`bash
echo "hello" 
\`\`\`
`;

export function DesignerLlmCallFields({ config, onChange }: Props) {
  const [skillCatalog, setSkillCatalog] = useState<DesignerSkill[]>([]);
  const [skillsError, setSkillsError] = useState<string | null>(null);
  const [showAddSkill, setShowAddSkill] = useState(false);
  const [skillDraft, setSkillDraft] = useState("");
  const [savingSkill, setSavingSkill] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const llm = llmConfigFromNode(config);

  const loadCatalog = () =>
    api
      .designerSkills()
      .then((skills) => {
        setSkillCatalog(skills);
        setSkillsError(null);
        return skills;
      })
      .catch((err) => {
        setSkillsError(String(err));
        return [] as DesignerSkill[];
      });

  useEffect(() => {
    void loadCatalog();
  }, []);

  const selectSkill = (skillId: string, catalog: DesignerSkill[]) => {
    const current = llm.skills || [];
    if (current.includes(skillId)) return;
    const nextSkills = [...current, skillId];
    onChange({
      ...config,
      mode: "flink_skills",
      skills: nextSkills,
      allowed_commands: mergeAllowedCommands(
        nextSkills,
        catalog,
        llm.allowed_commands || [],
      ),
    });
  };

  const handleAddSkill = async () => {
    const content = skillDraft.trim();
    if (!content) {
      setSaveError("Paste a SKILL.md before saving.");
      return;
    }
    setSavingSkill(true);
    setSaveError(null);
    try {
      const created = await api.createDesignerSkill(content);
      const catalog = await loadCatalog();
      selectSkill(created.id, catalog.length ? catalog : [created]);
      setSkillDraft("");
      setShowAddSkill(false);
    } catch (err) {
      setSaveError(String(err));
    } finally {
      setSavingSkill(false);
    }
  };

  const handleDeleteSkill = async (skill: DesignerSkill) => {
    try {
      await api.deleteDesignerSkill(skill.id);
      const catalog = await loadCatalog();
      const nextSkills = (llm.skills || []).filter((item) => item !== skill.id);
      onChange({
        ...config,
        mode: "flink_skills",
        skills: nextSkills,
        allowed_commands: mergeAllowedCommands(
          nextSkills,
          catalog,
          llm.allowed_commands || [],
        ),
      });
    } catch (err) {
      setSkillsError(String(err));
    }
  };

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
              No skills found. Paste a <code>SKILL.md</code> below to add one.
            </p>
          ) : (
            <div className="designer-skill-list">
              {skillCatalog.map((skill) => {
                const selected = (llm.skills || []).includes(skill.id);
                return (
                  <div key={skill.id} className="designer-skill-option">
                    <label style={{ display: "flex", gap: "0.5rem", flex: 1 }}>
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
                        {skill.source === "user" ? (
                          <span className="badge" style={{ marginLeft: "0.4rem" }}>
                            user
                          </span>
                        ) : null}
                        {skill.description ? (
                          <span className="muted"> — {skill.description}</span>
                        ) : null}
                      </span>
                    </label>
                    {skill.source === "user" ? (
                      <button
                        type="button"
                        className="studio-button ghost"
                        title="Delete this pasted skill"
                        onClick={() => void handleDeleteSkill(skill)}
                      >
                        Delete
                      </button>
                    ) : null}
                  </div>
                );
              })}
            </div>
          )}

          {showAddSkill ? (
            <div className="designer-skill-add">
              <label className="studio-label">Paste SKILL.md</label>
              <textarea
                className="studio-input"
                rows={12}
                spellCheck={false}
                value={skillDraft}
                placeholder={SKILL_TEMPLATE}
                onChange={(e) => setSkillDraft(e.target.value)}
                style={{ fontFamily: "monospace", fontSize: "0.8rem" }}
              />
              {saveError ? (
                <p className="error" style={{ fontSize: "0.85rem" }}>
                  {saveError}
                </p>
              ) : null}
              <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.4rem" }}>
                <button
                  type="button"
                  className="studio-button"
                  disabled={savingSkill}
                  onClick={() => void handleAddSkill()}
                >
                  {savingSkill ? "Saving…" : "Save skill"}
                </button>
                <button
                  type="button"
                  className="studio-button ghost"
                  disabled={savingSkill}
                  onClick={() => {
                    setShowAddSkill(false);
                    setSaveError(null);
                  }}
                >
                  Cancel
                </button>
                {!skillDraft ? (
                  <button
                    type="button"
                    className="studio-button ghost"
                    onClick={() => setSkillDraft(SKILL_TEMPLATE)}
                  >
                    Use template
                  </button>
                ) : null}
              </div>
              <p className="muted" style={{ fontSize: "0.85rem" }}>
                Must start with a YAML frontmatter block (<code>---</code>) including{" "}
                <code>name</code> and <code>description</code>. Saved skills become
                available to every agent.
              </p>
            </div>
          ) : (
            <button
              type="button"
              className="studio-button ghost"
              style={{ marginTop: "0.4rem" }}
              onClick={() => setShowAddSkill(true)}
            >
              + Paste a skill
            </button>
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
