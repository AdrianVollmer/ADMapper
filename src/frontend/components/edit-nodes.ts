/**
 * Batch Edit Nodes Component
 *
 * Modal for performing bulk operations on nodes by name.
 * Users paste node names (one per line), select an action, and apply it.
 * Supports: mark owned, mark not owned, set disabled, set enabled, delete.
 *
 * Resolution and mutation happen server-side in a single request.
 */

import { createModal, type ModalHandle } from "../utils/modal";
import { api } from "../api/client";
import { showSuccess, showError, showConfirm } from "../utils/notifications";
import { escapeHtml } from "../utils/html";
import { getRenderer } from "./graph-view";
import { updateDetailPanel } from "./sidebars";

/** Actions available for batch editing (must match backend BatchEditAction) */
type BatchAction = "mark_owned" | "mark_not_owned" | "set_disabled" | "set_enabled" | "delete";

const BATCH_ACTIONS: { value: BatchAction; label: string; danger: boolean }[] = [
  { value: "mark_owned", label: "Mark Owned", danger: false },
  { value: "mark_not_owned", label: "Mark Not Owned", danger: false },
  { value: "set_enabled", label: "Set as Enabled", danger: false },
  { value: "set_disabled", label: "Set as Disabled", danger: false },
  { value: "delete", label: "Delete", danger: true },
];

/** Response from the batch edit endpoint */
interface BatchEditResponse {
  updated: number;
  failed: number;
  results: {
    name: string;
    success: boolean;
    node_id?: string;
    error?: string;
  }[];
}

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

/** Apply the selected action via a single backend request */
async function applyAction(): Promise<void> {
  if (!modal) return;

  const textarea = document.getElementById("edit-nodes-names") as HTMLTextAreaElement;
  const actionSelect = document.getElementById("edit-nodes-action") as HTMLSelectElement;
  const statusEl = document.getElementById("edit-nodes-status") as HTMLElement;

  const names = textarea.value
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  if (names.length === 0) {
    showError("No node names provided");
    return;
  }

  const action = actionSelect.value as BatchAction;
  const actionDef = BATCH_ACTIONS.find((a) => a.value === action)!;

  // Confirm before applying
  const confirmed = await showConfirm(`${actionDef.label} on ${names.length} node(s)?`, {
    title: actionDef.label,
    confirmText: actionDef.label,
    danger: actionDef.danger,
  });

  if (!confirmed) return;

  setControlsEnabled(false);
  showStatus(statusEl, "info", `Applying "${actionDef.label}" to ${names.length} node(s)...`);

  try {
    const response = await api.post<BatchEditResponse>("/api/graph/batch-edit-nodes", {
      names,
      action,
    });

    // Update local graph for delete/enabled actions
    if (action === "delete") {
      const graph = getRenderer()?.sigma.getGraph();
      if (graph) {
        for (const r of response.results) {
          if (r.success && r.node_id && graph.hasNode(r.node_id)) {
            graph.dropNode(r.node_id);
          }
        }
      }
    } else if (action === "set_enabled" || action === "set_disabled") {
      const graph = getRenderer()?.sigma.getGraph();
      const enabled = action === "set_enabled";
      if (graph) {
        for (const r of response.results) {
          if (r.success && r.node_id && graph.hasNode(r.node_id)) {
            const props = graph.getNodeAttribute(r.node_id, "properties") ?? {};
            graph.setNodeAttribute(r.node_id, "properties", { ...props, enabled });
          }
        }
      }
    }

    // Refresh graph
    const renderer = getRenderer();
    if (renderer) {
      renderer.refresh();
      updateDetailPanel(null, null);
    }

    // Show results
    if (response.failed === 0) {
      showSuccess(`${actionDef.label}: ${response.updated} node(s) updated`);
      modal.close();
    } else {
      const failedResults = response.results.filter((r) => !r.success);
      showStatus(
        statusEl,
        "error",
        `<strong>${response.updated} succeeded, ${response.failed} failed:</strong><br>` +
          failedResults
            .slice(0, 10)
            .map((r) => escapeHtml(`${r.name}: ${r.error ?? "unknown error"}`))
            .join("<br>") +
          (failedResults.length > 10 ? `<br>... and ${failedResults.length - 10} more` : ""),
        true
      );
    }
  } catch (err) {
    showStatus(statusEl, "error", `Error: ${err instanceof Error ? err.message : String(err)}`);
  } finally {
    setControlsEnabled(true);
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
