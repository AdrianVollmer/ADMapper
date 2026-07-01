/**
 * Batch Edit Nodes Component
 *
 * Modal for performing bulk operations on nodes by name.
 * Users paste node names (one per line), select an action, and apply it.
 * Supports: mark owned, mark not owned, set disabled, set enabled, delete.
 */

import { createModal, type ModalHandle } from "../utils/modal";
import { api } from "../api/client";
import { showSuccess, showError, showConfirm } from "../utils/notifications";
import { escapeHtml } from "../utils/html";
import { getRenderer } from "./graph-view";
import { updateDetailPanel } from "./sidebars";

/** Search result from the API */
interface SearchResult {
  id: string;
  name: string;
  type: string;
  properties: Record<string, unknown>;
}

/** Result of resolving a name to a node */
interface ResolvedNode {
  inputName: string;
  id: string;
  name: string;
  type: string;
}

/** Actions available for batch editing */
type BatchAction = "mark-owned" | "mark-not-owned" | "set-disabled" | "set-enabled" | "delete";

const BATCH_ACTIONS: { value: BatchAction; label: string; danger: boolean }[] = [
  { value: "mark-owned", label: "Mark Owned", danger: false },
  { value: "mark-not-owned", label: "Mark Not Owned", danger: false },
  { value: "set-enabled", label: "Set as Enabled", danger: false },
  { value: "set-disabled", label: "Set as Disabled", danger: false },
  { value: "delete", label: "Delete", danger: true },
];

let modal: ModalHandle | null = null;

/** Show a status message with appropriate styling */
function showStatus(el: HTMLElement, level: "info" | "error", message: string, isHtml = false): void {
  el.hidden = false;
  if (level === "error") {
    el.style.background = "rgba(239, 68, 68, 0.15)";
    el.style.color = "#fca5a5";
  } else {
    el.style.background = "rgba(59, 130, 246, 0.15)";
    el.style.color = "#93c5fd";
  }
  if (isHtml) {
    el.innerHTML = message;
  } else {
    el.textContent = message;
  }
}

/** Open the batch edit nodes modal */
export function openEditNodes(): void {
  if (!modal) {
    modal = createModal({
      id: "edit-nodes-modal",
      title: "Batch Edit Nodes",
      expandable: true,
      buttons: [
        { label: "Cancel", action: "close", className: "btn btn-secondary" },
        { label: "Apply", action: "apply", className: "btn btn-primary" },
      ],
      onClick: handleClick,
    });
  }

  renderBody();
  modal.open();

  // Focus the textarea
  const textarea = modal.body.querySelector("textarea") as HTMLTextAreaElement | null;
  textarea?.focus();
}

/** Render the modal body content */
function renderBody(): void {
  if (!modal) return;

  const actionOptions = BATCH_ACTIONS.map((a) => `<option value="${a.value}">${escapeHtml(a.label)}</option>`).join(
    "\n"
  );

  modal.body.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 12px;">
      <div>
        <label class="form-label" for="edit-nodes-names">Node Names <span style="opacity: 0.6">(one per line)</span></label>
        <textarea
          id="edit-nodes-names"
          class="form-textarea"
          rows="12"
          placeholder="YOURCOMPANY\\jsmith&#10;DC01.yourcompany.local&#10;Domain Admins@yourcompany.local"
          spellcheck="false"
        ></textarea>
      </div>
      <div>
        <label class="form-label" for="edit-nodes-action">Action</label>
        <select id="edit-nodes-action" class="form-input">
          ${actionOptions}
        </select>
      </div>
      <div id="edit-nodes-status" hidden style="padding: 8px 12px; border-radius: 6px; font-size: 0.875rem;"></div>
    </div>
  `;
}

/** Handle clicks within the modal */
function handleClick(action: string): void {
  if (action === "apply") {
    applyAction();
  }
}

/** Resolve node names to IDs via the search API */
async function resolveNames(names: string[]): Promise<{ resolved: ResolvedNode[]; unresolved: string[] }> {
  const resolved: ResolvedNode[] = [];
  const unresolved: string[] = [];

  // Resolve each name individually via exact search
  for (const name of names) {
    try {
      const results = await api.get<SearchResult[]>(`/api/graph/search?q=${encodeURIComponent(name)}&limit=5`);

      // Find exact match (case-insensitive)
      const exact = results.find((r) => r.name.toLowerCase() === name.toLowerCase());

      if (exact) {
        resolved.push({
          inputName: name,
          id: exact.id,
          name: exact.name,
          type: exact.type,
        });
      } else {
        unresolved.push(name);
      }
    } catch {
      unresolved.push(name);
    }
  }

  return { resolved, unresolved };
}

/** Apply the selected action to all resolved nodes */
async function applyAction(): Promise<void> {
  if (!modal) return;

  const textarea = document.getElementById("edit-nodes-names") as HTMLTextAreaElement;
  const actionSelect = document.getElementById("edit-nodes-action") as HTMLSelectElement;
  const statusEl = document.getElementById("edit-nodes-status") as HTMLElement;

  const rawNames = textarea.value
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  if (rawNames.length === 0) {
    showError("No node names provided");
    return;
  }

  const action = actionSelect.value as BatchAction;
  const actionDef = BATCH_ACTIONS.find((a) => a.value === action)!;

  // Show resolving status
  showStatus(statusEl, "info", `Resolving ${rawNames.length} node name(s)...`);

  // Disable controls while processing
  setControlsEnabled(false);

  try {
    const { resolved, unresolved } = await resolveNames(rawNames);

    if (resolved.length === 0) {
      showStatus(statusEl, "error", `None of the ${rawNames.length} node name(s) could be resolved.`);
      setControlsEnabled(true);
      return;
    }

    // Build confirmation message
    let confirmMsg = `${actionDef.label} on ${resolved.length} node(s)?`;
    if (unresolved.length > 0) {
      confirmMsg += `\n\n${unresolved.length} node(s) could not be resolved and will be skipped:\n${unresolved
        .slice(0, 10)
        .map((n) => "  - " + n)
        .join("\n")}`;
      if (unresolved.length > 10) {
        confirmMsg += `\n  ... and ${unresolved.length - 10} more`;
      }
    }

    const confirmed = await showConfirm(confirmMsg, {
      title: actionDef.label,
      confirmText: actionDef.label,
      danger: actionDef.danger,
    });

    if (!confirmed) {
      statusEl.hidden = true;
      setControlsEnabled(true);
      return;
    }

    showStatus(statusEl, "info", `Applying "${actionDef.label}" to ${resolved.length} node(s)...`);

    const errors: string[] = [];
    let successCount = 0;

    for (const node of resolved) {
      try {
        await applyToNode(node, action);
        successCount++;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        errors.push(`${node.name}: ${msg}`);
      }
    }

    // Update graph view
    refreshGraph();

    // Show results
    if (errors.length === 0) {
      showSuccess(`${actionDef.label}: ${successCount} node(s) updated`);
      modal.close();
    } else {
      showStatus(
        statusEl,
        "error",
        `<strong>${successCount} succeeded, ${errors.length} failed:</strong><br>` +
          errors
            .slice(0, 10)
            .map((e) => escapeHtml(e))
            .join("<br>") +
          (errors.length > 10 ? `<br>... and ${errors.length - 10} more` : ""),
        true
      );
    }
  } catch (err) {
    showStatus(statusEl, "error", `Error: ${err instanceof Error ? err.message : String(err)}`);
  } finally {
    setControlsEnabled(true);
  }
}

/** Apply a single action to a single node */
async function applyToNode(node: ResolvedNode, action: BatchAction): Promise<void> {
  const encodedId = encodeURIComponent(node.id);

  switch (action) {
    case "mark-owned":
      await api.postNoContent(`/api/graph/node/${encodedId}/owned`, { owned: true });
      break;
    case "mark-not-owned":
      await api.postNoContent(`/api/graph/node/${encodedId}/owned`, { owned: false });
      break;
    case "set-enabled":
      await api.putNoContent(`/api/graph/nodes/${encodedId}`, {
        properties: { enabled: true },
      });
      updateLocalNodeProperty(node.id, "enabled", true);
      break;
    case "set-disabled":
      await api.putNoContent(`/api/graph/nodes/${encodedId}`, {
        properties: { enabled: false },
      });
      updateLocalNodeProperty(node.id, "enabled", false);
      break;
    case "delete":
      await api.delete(`/api/graph/nodes/${encodedId}`);
      removeLocalNode(node.id);
      break;
  }
}

/** Update a node property in the local graph */
function updateLocalNodeProperty(nodeId: string, key: string, value: unknown): void {
  const graph = getRenderer()?.sigma.getGraph();
  if (!graph?.hasNode(nodeId)) return;

  const props = graph.getNodeAttribute(nodeId, "properties") ?? {};
  graph.setNodeAttribute(nodeId, "properties", { ...props, [key]: value });
}

/** Remove a node from the local graph */
function removeLocalNode(nodeId: string): void {
  const graph = getRenderer()?.sigma.getGraph();
  if (graph?.hasNode(nodeId)) {
    graph.dropNode(nodeId);
  }
}

/** Refresh the graph renderer after changes */
function refreshGraph(): void {
  const renderer = getRenderer();
  if (renderer) {
    renderer.refresh();
    updateDetailPanel(null, null);
  }
}

/** Enable or disable form controls */
function setControlsEnabled(enabled: boolean): void {
  if (!modal) return;
  const textarea = modal.body.querySelector("textarea") as HTMLTextAreaElement | null;
  const select = modal.body.querySelector("select") as HTMLSelectElement | null;
  const applyBtn = modal.footer.querySelector("[data-action='apply']") as HTMLButtonElement | null;

  if (textarea) textarea.disabled = !enabled;
  if (select) select.disabled = !enabled;
  if (applyBtn) applyBtn.disabled = !enabled;
}
