import type { Node } from "@xyflow/react";
import { DEFAULT_PROMPT_SYSTEM, DEFAULT_PROMPT_USER } from "./promptDefaults";
import {
  PROMPT_CONSTRAINT_NOTE,
  PROMPT_RECIPES,
  PROMPT_SAMPLE,
  applyUserPromptTemplate,
  type PromptRecipe,
} from "./promptRecipes";

interface Props {
  nodes: Node[];
  onUpdateNode: (nodeId: string, patch: { name?: string; config?: Record<string, unknown> }) => void;
  onAddPrompt: () => void;
  onApplySkillsRecipe?: () => void;
}

function promptNode(nodes: Node[]): Node | undefined {
  return nodes.find((n) => n.type === "prompt");
}

export function PromptConstraintNote() {
  return (
    <p className="designer-prompt-constraint muted" role="note">
      {PROMPT_CONSTRAINT_NOTE}
    </p>
  );
}

export function PromptRecipePicker({
  onApply,
}: {
  onApply: (recipe: PromptRecipe) => void;
}) {
  return (
    <div className="designer-prompt-recipes">
      <span className="studio-label designer-prompt-recipes-label">Start from a recipe</span>
      <div className="designer-prompt-recipe-buttons">
        {PROMPT_RECIPES.map((recipe) => (
          <button
            key={recipe.id}
            type="button"
            className="secondary designer-prompt-recipe-btn"
            title={recipe.description}
            onClick={() => onApply(recipe)}
          >
            {recipe.label}
          </button>
        ))}
      </div>
    </div>
  );
}

export function PromptUserPreview({ userTemplate }: { userTemplate: string }) {
  const resolved = applyUserPromptTemplate(userTemplate);
  return (
    <div className="designer-prompt-preview">
      <span className="studio-label">Preview (sample input)</span>
      <p className="muted designer-prompt-preview-meta">
        Record <code>{JSON.stringify({ key: "1", value: Number(PROMPT_SAMPLE.value) })}</code>
      </p>
      <pre className="designer-prompt-preview-text">{resolved}</pre>
    </div>
  );
}

export function DesignerPromptPanel({ nodes, onUpdateNode, onAddPrompt, onApplySkillsRecipe }: Props) {
  const node = promptNode(nodes);

  if (!node) {
    return (
      <section className="card designer-prompt-panel">
        <div className="designer-prompt-panel-header">
          <h3 style={{ margin: 0 }}>Prompt instructions</h3>
        </div>
        <PromptConstraintNote />
        <p className="muted" style={{ margin: "0.5rem 0 0" }}>
          ReAct agents need a prompt node on the canvas. Add one to define system and user
          instructions for the LLM, or start from a recipe after adding the node.
        </p>
        <button type="button" style={{ marginTop: "0.75rem" }} onClick={onAddPrompt}>
          + Add prompt node
        </button>
      </section>
    );
  }

  const data = node.data as { name?: string; config?: Record<string, unknown> };
  const config = data.config || {};
  const system = String(config.system ?? "");
  const user = String(config.user ?? "");

  function applyRecipe(recipe: PromptRecipe) {
    onUpdateNode(node!.id, {
      config: { ...config, system: recipe.system, user: recipe.user, template: recipe.id },
    });
    if (recipe.id === "skills_math") {
      onApplySkillsRecipe?.();
    }
  }

  return (
    <section className="card designer-prompt-panel">
      <div className="designer-prompt-panel-header">
        <h3 style={{ margin: 0 }}>Prompt instructions</h3>
        <span className="muted designer-prompt-node-ref">
          Node <code>{node.id}</code>
          {data.name ? ` · ${data.name}` : ""}
        </span>
      </div>

      <PromptConstraintNote />

      <p className="muted designer-prompt-hint">
        System instructions set behavior; the user template receives each input record. Use{" "}
        <code>{"{message}"}</code> and <code>{"{value}"}</code> placeholders.
      </p>

      <PromptRecipePicker onApply={applyRecipe} />

      <label className="studio-label">System prompt</label>
      <textarea
        className="studio-textarea designer-prompt-textarea"
        rows={7}
        value={system}
        placeholder={DEFAULT_PROMPT_SYSTEM}
        onChange={(e) =>
          onUpdateNode(node.id, {
            config: { ...config, system: e.target.value },
          })
        }
      />

      <label className="studio-label">User prompt template</label>
      <textarea
        className="studio-textarea designer-prompt-textarea"
        rows={3}
        value={user}
        placeholder={DEFAULT_PROMPT_USER}
        onChange={(e) =>
          onUpdateNode(node.id, {
            config: { ...config, user: e.target.value },
          })
        }
      />

      <PromptUserPreview userTemplate={user} />
    </section>
  );
}

/** Shared fields for the inspector when a prompt node is selected. */
export function PromptInstructionFields({
  nodeId,
  config,
  onUpdate,
}: {
  nodeId: string;
  config: Record<string, unknown>;
  onUpdate: (nodeId: string, patch: { config: Record<string, unknown> }) => void;
}) {
  const system = String(config.system ?? "");
  const user = String(config.user ?? "");

  function applyRecipe(recipe: PromptRecipe) {
    onUpdate(nodeId, { config: { ...config, system: recipe.system, user: recipe.user } });
  }

  return (
    <>
      <PromptConstraintNote />
      <p className="muted designer-prompt-hint" style={{ marginTop: 0 }}>
        Instructions sent to the LLM on each run. Edits also appear in the Prompt instructions
        panel above the canvas.
      </p>

      <PromptRecipePicker onApply={applyRecipe} />

      <label className="studio-label">System prompt</label>
      <textarea
        className="studio-textarea designer-prompt-textarea"
        rows={6}
        value={system}
        placeholder={DEFAULT_PROMPT_SYSTEM}
        onChange={(e) => onUpdate(nodeId, { config: { ...config, system: e.target.value } })}
      />
      <label className="studio-label">User prompt template</label>
      <textarea
        className="studio-textarea designer-prompt-textarea"
        rows={3}
        value={user}
        placeholder={DEFAULT_PROMPT_USER}
        onChange={(e) => onUpdate(nodeId, { config: { ...config, user: e.target.value } })}
      />
      <PromptUserPreview userTemplate={user} />
    </>
  );
}
