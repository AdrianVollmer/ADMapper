/**
 * Add Node/Relationship Component
 *
 * Modals for adding new nodes and relationships to the graph.
 */

import { api } from "../api/client";
import { escapeHtml } from "../utils/html";
import { getRenderer } from "./graph-view";
import { updateDetailPanel, updateDetailPanelForEdge } from "./sidebars";

/** Available relationship types */
const COMMON_EDGE_TYPES = [
  "MemberOf",
  "HasSession",
  "AdminTo",
  "CanRDP",
  "CanPSRemote",
  "ExecuteDCOM",
  "AllowedToDelegate",
  "AllowedToAct",
  "Contains",
  "GPLink",
  "TrustedBy",
  "GenericAll",
  "GenericWrite",
  "WriteDacl",
  "WriteOwner",
  "Owns",
  "ForceChangePassword",
  "AddMember",
  "ReadLAPSPassword",
  "ReadGMSAPassword",
  "DCSync",
];

/** Available node types */
const COMMON_NODE_TYPES = ["User", "Computer", "Group", "Domain", "OU", "GPO", "Container"];

/** Search result from API */
interface SearchResult {
  id: string;
  name: string;
  type: string;
}

/** Add Relationship modal element */
let addEdgeModal: HTMLElement | null = null;

/** Add Node modal element */
let addNodeModal: HTMLElement | null = null;

/** Debounce timer for search */
let searchTimer: ReturnType<typeof setTimeout> | null = null;

/** Initialize the add node/relationship modals */
export function initAddNodeEdge(): void {
  // Modals are created on demand
}

/** Open the Add Relationship modal */
export function openAddEdge(): void {
  if (!addEdgeModal) {
    createAddEdgeModal();
  }
  addEdgeModal!.hidden = false;
  resetAddEdgeForm();
  // Focus first input
  const sourceInput = document.getElementById("add-relationship-source") as HTMLInputElement;
  sourceInput?.focus();
}

/** Open the Add Node modal */
export function openAddNode(): void {
  if (!addNodeModal) {
    createAddNodeModal();
  }
  addNodeModal!.hidden = false;
  resetAddNodeForm();
  // Focus first input
  const idInput = document.getElementById("add-node-id") as HTMLInputElement;
  idInput?.focus();
}

/** Close Add Relationship modal */
function closeAddEdgeModal(): void {
  if (addEdgeModal) {
    addEdgeModal.hidden = true;
  }
}

/** Close Add Node modal */
function closeAddNodeModal(): void {
  if (addNodeModal) {
    addNodeModal.hidden = true;
  }
}

/** Create the Add Relationship modal */
function createAddEdgeModal(): void {
  addEdgeModal = document.createElement("div");
  addEdgeModal.id = "add-relationship-modal";
  addEdgeModal.className = "modal-overlay";

  // Build relationship type options
  const edgeTypeOptions = COMMON_EDGE_TYPES.map(
    (type) => `<option value="${escapeHtml(type)}">${escapeHtml(type)}</option>`
  ).join("");

  addEdgeModal.innerHTML = `
    <div class="modal-content">
      <div class="modal-header">
        <h2 class="modal-title">Add Relationship</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body">
        <form id="add-relationship-form" class="add-form">
          <div class="form-group">
            <label for="add-relationship-source" class="form-label">Source Node</label>
            <div class="search-input-wrapper">
              <input
                type="text"
                id="add-relationship-source"
                class="form-input"
                placeholder="Search for source node..."
                autocomplete="off"
              />
              <input type="hidden" id="add-relationship-source-id" />
              <div id="add-relationship-source-results" class="search-results" hidden></div>
            </div>
          </div>

          <div class="form-group">
            <label for="add-relationship-type" class="form-label">Relationship Type</label>
            <select id="add-relationship-type" class="form-select">
              <option value="">Select relationship type...</option>
              ${edgeTypeOptions}
            </select>
          </div>

          <div class="form-group">
            <label for="add-relationship-target" class="form-label">Target Node</label>
            <div class="search-input-wrapper">
              <input
                type="text"
                id="add-relationship-target"
                class="form-input"
                placeholder="Search for target node..."
                autocomplete="off"
              />
              <input type="hidden" id="add-relationship-target-id" />
              <div id="add-relationship-target-results" class="search-results" hidden></div>
            </div>
          </div>

          <div id="add-relationship-error" class="form-error" hidden></div>
        </form>
      </div>
      <div class="modal-footer">
        <button class="btn btn-secondary" data-action="close">Cancel</button>
        <button class="btn btn-primary" data-action="submit-relationship">Add Relationship</button>
      </div>
    </div>
  `;

  addEdgeModal.addEventListener("click", handleAddEdgeClick);
  setupSearchInput("add-relationship-source", "add-relationship-source-results", "add-relationship-source-id");
  setupSearchInput("add-relationship-target", "add-relationship-target-results", "add-relationship-target-id");

  document.body.appendChild(addEdgeModal);

  // Close on Escape
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && addEdgeModal && !addEdgeModal.hidden) {
      closeAddEdgeModal();
    }
  });
}

/** Create the Add Node modal */
function createAddNodeModal(): void {
  addNodeModal = document.createElement("div");
  addNodeModal.id = "add-node-modal";
  addNodeModal.className = "modal-overlay";

  // Build node type options
  const nodeTypeOptions = COMMON_NODE_TYPES.map(
    (type) => `<option value="${escapeHtml(type)}">${escapeHtml(type)}</option>`
  ).join("");

  addNodeModal.innerHTML = `
    <div class="modal-content">
      <div class="modal-header">
        <h2 class="modal-title">Add Node</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body">
        <form id="add-node-form" class="add-form">
          <div class="form-group">
            <label for="add-node-id" class="form-label">Object ID</label>
            <input
              type="text"
              id="add-node-id"
              class="form-input"
              placeholder="e.g., S-1-5-21-... or GUID"
              required
            />
            <p class="form-help">Unique identifier (SID or GUID)</p>
          </div>

          <div class="form-group">
            <label for="add-node-label" class="form-label">Display Label</label>
            <input
              type="text"
              id="add-node-label"
              class="form-input"
              placeholder="e.g., ADMIN@DOMAIN.COM"
              required
            />
            <p class="form-help">Human-readable name for the node</p>
          </div>

          <div class="form-group">
            <label for="add-node-type" class="form-label">Node Label</label>
            <select id="add-node-type" class="form-select" required>
              <option value="">Select node type...</option>
              ${nodeTypeOptions}
            </select>
          </div>

          <div id="add-node-error" class="form-error" hidden></div>
        </form>
      </div>
      <div class="modal-footer">
        <button class="btn btn-secondary" data-action="close">Cancel</button>
        <button class="btn btn-primary" data-action="submit-node">Add Node</button>
      </div>
    </div>
  `;

  addNodeModal.addEventListener("click", handleAddNodeClick);
  document.body.appendChild(addNodeModal);

  // Close on Escape
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && addNodeModal && !addNodeModal.hidden) {
      closeAddNodeModal();
    }
  });
}

/** Set up search input with autocomplete */
function setupSearchInput(inputId: string, resultsId: string, hiddenId: string): void {
  // Need to wait for modal to be appended to DOM
  setTimeout(() => {
    const input = document.getElementById(inputId) as HTMLInputElement;
    const results = document.getElementById(resultsId);
    const hidden = document.getElementById(hiddenId) as HTMLInputElement;

    if (!input || !results || !hidden) return;

    input.addEventListener("input", () => {
      const query = input.value.trim();
      hidden.value = ""; // Clear hidden ID when typing

      if (query.length < 2) {
        results.hidden = true;
        return;
      }

      // Debounce search
      if (searchTimer) clearTimeout(searchTimer);
      searchTimer = setTimeout(() => performSearch(query, results, input, hidden), 200);
    });

    input.addEventListener("focus", () => {
      if (input.value.length >= 2 && results.children.length > 0) {
        results.hidden = false;
      }
    });

    input.addEventListener("blur", () => {
      // Delay to allow click on result
      setTimeout(() => {
        results.hidden = true;
      }, 200);
    });
  }, 0);
}

/** Perform node search and display results */
async function performSearch(
  query: string,
  resultsEl: HTMLElement,
  inputEl: HTMLInputElement,
  hiddenEl: HTMLInputElement
): Promise<void> {
  try {
    const results = await api.get<SearchResult[]>(`/api/graph/search?q=${encodeURIComponent(query)}&limit=10`);

    if (results.length === 0) {
      resultsEl.innerHTML = '<div class="search-result-empty">No nodes found</div>';
    } else {
      resultsEl.innerHTML = results
        .map(
          (r) => `
          <div class="search-result-item" data-id="${escapeHtml(r.id)}" data-label="${escapeHtml(r.name)}">
            <span class="search-result-label">${escapeHtml(r.name)}</span>
            <span class="search-result-type">${escapeHtml(r.type)}</span>
          </div>
        `
        )
        .join("");

      // Add click handlers to results
      for (const item of resultsEl.querySelectorAll(".search-result-item")) {
        item.addEventListener("click", () => {
          const id = item.getAttribute("data-id") || "";
          const label = item.getAttribute("data-label") || "";
          inputEl.value = label;
          hiddenEl.value = id;
          resultsEl.hidden = true;
        });
      }
    }

    resultsEl.hidden = false;
  } catch (err) {
    console.error("Search failed:", err);
    resultsEl.innerHTML = '<div class="search-result-empty">Search failed</div>';
    resultsEl.hidden = false;
  }
}

/** Handle clicks in Add Relationship modal */
function handleAddEdgeClick(e: Event): void {
  const target = e.target as HTMLElement;

  if (target.classList.contains("modal-overlay")) {
    closeAddEdgeModal();
    return;
  }

  const actionEl = target.closest("[data-action]") as HTMLElement;
  if (!actionEl) return;

  const action = actionEl.getAttribute("data-action");

  switch (action) {
    case "close":
      closeAddEdgeModal();
      break;
    case "submit-relationship":
      submitAddEdge();
      break;
  }
}

/** Handle clicks in Add Node modal */
function handleAddNodeClick(e: Event): void {
  const target = e.target as HTMLElement;

  if (target.classList.contains("modal-overlay")) {
    closeAddNodeModal();
    return;
  }

  const actionEl = target.closest("[data-action]") as HTMLElement;
  if (!actionEl) return;

  const action = actionEl.getAttribute("data-action");

  switch (action) {
    case "close":
      closeAddNodeModal();
      break;
    case "submit-node":
      submitAddNode();
      break;
  }
}

/** Submit add relationship form */
async function submitAddEdge(): Promise<void> {
  const sourceId = (document.getElementById("add-relationship-source-id") as HTMLInputElement).value;
  const targetId = (document.getElementById("add-relationship-target-id") as HTMLInputElement).value;
  const edgeType = (document.getElementById("add-relationship-type") as HTMLSelectElement).value;
  const errorEl = document.getElementById("add-relationship-error");

  // Validate
  if (!sourceId) {
    showError(errorEl, "Please select a source node from the search results");
    return;
  }
  if (!edgeType) {
    showError(errorEl, "Please select an relationship type");
    return;
  }
  if (!targetId) {
    showError(errorEl, "Please select a target node from the search results");
    return;
  }

  try {
    await api.post("/api/graph/relationship", {
      source: sourceId,
      target: targetId,
      rel_type: edgeType,
    });

    closeAddEdgeModal();

    // Refresh the graph
    window.location.reload();
  } catch (err) {
    showError(errorEl, `Failed to add relationship: ${err instanceof Error ? err.message : String(err)}`);
  }
}

/** Submit add node form */
async function submitAddNode(): Promise<void> {
  const id = (document.getElementById("add-node-id") as HTMLInputElement).value.trim();
  const label = (document.getElementById("add-node-label") as HTMLInputElement).value.trim();
  const nodeType = (document.getElementById("add-node-type") as HTMLSelectElement).value;
  const errorEl = document.getElementById("add-node-error");

  // Validate
  if (!id) {
    showError(errorEl, "Object ID is required");
    return;
  }
  if (!label) {
    showError(errorEl, "Display label is required");
    return;
  }
  if (!nodeType) {
    showError(errorEl, "Please select a node type");
    return;
  }

  try {
    await api.post("/api/graph/node", {
      id,
      label,
      node_type: nodeType,
    });

    closeAddNodeModal();

    // Refresh the graph
    window.location.reload();
  } catch (err) {
    showError(errorEl, `Failed to add node: ${err instanceof Error ? err.message : String(err)}`);
  }
}

/** Show error message */
function showError(el: HTMLElement | null, message: string): void {
  if (el) {
    el.textContent = message;
    el.hidden = false;
  }
}

/** Reset Add Relationship form */
function resetAddEdgeForm(): void {
  const form = document.getElementById("add-relationship-form") as HTMLFormElement;
  if (form) form.reset();

  const sourceId = document.getElementById("add-relationship-source-id") as HTMLInputElement;
  const targetId = document.getElementById("add-relationship-target-id") as HTMLInputElement;
  if (sourceId) sourceId.value = "";
  if (targetId) targetId.value = "";

  const error = document.getElementById("add-relationship-error");
  if (error) error.hidden = true;

  const sourceResults = document.getElementById("add-relationship-source-results");
  const targetResults = document.getElementById("add-relationship-target-results");
  if (sourceResults) sourceResults.hidden = true;
  if (targetResults) targetResults.hidden = true;
}

/** Reset Add Node form */
function resetAddNodeForm(): void {
  const form = document.getElementById("add-node-form") as HTMLFormElement;
  if (form) form.reset();

  const error = document.getElementById("add-node-error");
  if (error) error.hidden = true;
}

// ============================================================================
// Edit Node Modal
// ============================================================================

/** Edit Node modal element */
let editNodeModal: HTMLElement | null = null;

/** Current node being edited */
let editingNodeId: string | null = null;

/** Open the Edit Node modal with current properties */
export function openEditNode(nodeId: string, properties: Record<string, unknown>): void {
  if (!editNodeModal) {
    createEditNodeModal();
  }
  editingNodeId = nodeId;
  editNodeModal!.hidden = false;
  populateEditNodeForm(properties);
}

/** Close Edit Node modal */
function closeEditNodeModal(): void {
  if (editNodeModal) {
    editNodeModal.hidden = true;
  }
  editingNodeId = null;
}

/** Create the Edit Node modal */
function createEditNodeModal(): void {
  editNodeModal = document.createElement("div");
  editNodeModal.id = "edit-node-modal";
  editNodeModal.className = "modal-overlay";

  editNodeModal.innerHTML = `
    <div class="modal-content">
      <div class="modal-header">
        <h2 class="modal-title">Edit Node</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body">
        <form id="edit-node-form" class="add-form">
          <div id="edit-node-properties" class="edit-properties-list"></div>

          <button type="button" class="btn btn-secondary btn-sm" data-action="add-property" style="margin-top: 8px">
            + Add Property
          </button>

          <div id="edit-node-error" class="form-error" hidden></div>
        </form>
      </div>
      <div class="modal-footer">
        <button class="btn btn-secondary" data-action="close">Cancel</button>
        <button class="btn btn-primary" data-action="submit-edit-node">Save</button>
      </div>
    </div>
  `;

  editNodeModal.addEventListener("click", handleEditNodeClick);
  document.body.appendChild(editNodeModal);

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && editNodeModal && !editNodeModal.hidden) {
      closeEditNodeModal();
    }
  });
}

/** Populate the edit node form with current properties */
function populateEditNodeForm(properties: Record<string, unknown>): void {
  const container = document.getElementById("edit-node-properties");
  if (!container) return;

  const error = document.getElementById("edit-node-error");
  if (error) error.hidden = true;

  container.innerHTML = "";

  const entries = Object.entries(properties);
  for (const [key, value] of entries) {
    addPropertyRow(container, key, formatPropertyValue(value), false);
  }

  if (entries.length === 0) {
    addPropertyRow(container, "", "", true);
  }
}

/** Format a property value for the edit form */
function formatPropertyValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

/** Add a property key-value row to the edit form */
function addPropertyRow(container: HTMLElement, key: string, value: string, isNew: boolean): void {
  const row = document.createElement("div");
  row.className = "edit-property-row";

  row.innerHTML = `
    <input
      type="text"
      class="form-input edit-prop-key"
      placeholder="Property name"
      value="${escapeHtml(key)}"
      ${!isNew ? 'data-original-key="' + escapeHtml(key) + '"' : ""}
    />
    <input
      type="text"
      class="form-input edit-prop-value"
      placeholder="Value"
      value="${escapeHtml(value)}"
    />
    <button type="button" class="btn-icon-sm danger" data-action="remove-property" title="Remove property" aria-label="Remove property">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16">
        <path d="M18 6L6 18M6 6l12 12"/>
      </svg>
    </button>
  `;

  container.appendChild(row);
}

/** Handle clicks in Edit Node modal */
function handleEditNodeClick(e: Event): void {
  const target = e.target as HTMLElement;

  if (target.classList.contains("modal-overlay")) {
    closeEditNodeModal();
    return;
  }

  const actionEl = target.closest("[data-action]") as HTMLElement;
  if (!actionEl) return;

  const action = actionEl.getAttribute("data-action");

  switch (action) {
    case "close":
      closeEditNodeModal();
      break;
    case "submit-edit-node":
      submitEditNode();
      break;
    case "add-property": {
      const container = document.getElementById("edit-node-properties");
      if (container) {
        addPropertyRow(container, "", "", true);
        // Focus the new key input
        const lastRow = container.lastElementChild;
        const keyInput = lastRow?.querySelector(".edit-prop-key") as HTMLInputElement;
        keyInput?.focus();
      }
      break;
    }
    case "remove-property": {
      const row = actionEl.closest(".edit-property-row");
      row?.remove();
      break;
    }
  }
}

/** Collect properties from the edit form rows */
function collectPropertiesFromForm(containerId: string): Record<string, unknown> {
  const container = document.getElementById(containerId);
  if (!container) return {};

  const properties: Record<string, unknown> = {};
  const rows = container.querySelectorAll(".edit-property-row");

  for (const row of rows) {
    const keyInput = row.querySelector(".edit-prop-key") as HTMLInputElement;
    const valueInput = row.querySelector(".edit-prop-value") as HTMLInputElement;
    const key = keyInput?.value.trim();
    const rawValue = valueInput?.value;

    if (!key) continue;

    properties[key] = parsePropertyValue(rawValue);
  }

  return properties;
}

/** Parse a property value string into its appropriate type */
function parsePropertyValue(raw: string): unknown {
  if (raw === "") return "";
  if (raw === "true") return true;
  if (raw === "false") return false;
  if (raw === "null") return null;

  // Try as number
  const num = Number(raw);
  if (!isNaN(num) && raw.trim() !== "") return num;

  // Try as JSON (for arrays/objects)
  if ((raw.startsWith("[") && raw.endsWith("]")) || (raw.startsWith("{") && raw.endsWith("}"))) {
    try {
      return JSON.parse(raw);
    } catch {
      // Not valid JSON, return as string
    }
  }

  return raw;
}

/** Submit the edit node form */
async function submitEditNode(): Promise<void> {
  if (!editingNodeId) return;

  const errorEl = document.getElementById("edit-node-error");
  const properties = collectPropertiesFromForm("edit-node-properties");

  // Extract name and label from properties if present
  const name = properties.name !== undefined ? String(properties.name) : undefined;
  const label = properties.label !== undefined ? String(properties.label) : undefined;

  try {
    await api.put(`/api/graph/nodes/${encodeURIComponent(editingNodeId)}`, {
      name,
      label,
      properties,
    });

    // Update the graph view locally
    const renderer = getRenderer();
    const graph = renderer?.sigma.getGraph();
    if (graph?.hasNode(editingNodeId)) {
      if (name !== undefined) {
        graph.setNodeAttribute(editingNodeId, "label", name);
      }
      const existingProps = graph.getNodeAttribute(editingNodeId, "properties") || {};
      graph.setNodeAttribute(editingNodeId, "properties", { ...existingProps, ...properties });
      renderer?.refresh();

      // Refresh the detail panel
      const attrs = graph.getNodeAttributes(editingNodeId);
      updateDetailPanel(editingNodeId, attrs as Parameters<typeof updateDetailPanel>[1]);
    }

    closeEditNodeModal();
  } catch (err) {
    showError(errorEl, `Failed to update node: ${err instanceof Error ? err.message : String(err)}`);
  }
}

// ============================================================================
// Edit Edge Modal
// ============================================================================

/** Edit Edge modal element */
let editEdgeModal: HTMLElement | null = null;

/** Current edge being edited */
let editingEdgeContext: {
  edgeId: string;
  sourceId: string;
  targetId: string;
  edgeType: string;
} | null = null;

/** Open the Edit Edge modal with current properties */
export function openEditEdge(
  edgeId: string,
  sourceId: string,
  targetId: string,
  edgeType: string,
  properties: Record<string, unknown>
): void {
  if (!editEdgeModal) {
    createEditEdgeModal();
  }
  editingEdgeContext = { edgeId, sourceId, targetId, edgeType };
  editEdgeModal!.hidden = false;

  // Update the title to show relationship type
  const titleEl = editEdgeModal!.querySelector(".modal-title");
  if (titleEl) titleEl.textContent = `Edit ${edgeType} Relationship`;

  populateEditEdgeForm(properties);
}

/** Close Edit Edge modal */
function closeEditEdgeModal(): void {
  if (editEdgeModal) {
    editEdgeModal.hidden = true;
  }
  editingEdgeContext = null;
}

/** Create the Edit Edge modal */
function createEditEdgeModal(): void {
  editEdgeModal = document.createElement("div");
  editEdgeModal.id = "edit-edge-modal";
  editEdgeModal.className = "modal-overlay";

  editEdgeModal.innerHTML = `
    <div class="modal-content">
      <div class="modal-header">
        <h2 class="modal-title">Edit Relationship</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body">
        <form id="edit-edge-form" class="add-form">
          <div id="edit-edge-properties" class="edit-properties-list"></div>

          <button type="button" class="btn btn-secondary btn-sm" data-action="add-edge-property" style="margin-top: 8px">
            + Add Property
          </button>

          <div id="edit-edge-error" class="form-error" hidden></div>
        </form>
      </div>
      <div class="modal-footer">
        <button class="btn btn-secondary" data-action="close">Cancel</button>
        <button class="btn btn-primary" data-action="submit-edit-edge">Save</button>
      </div>
    </div>
  `;

  editEdgeModal.addEventListener("click", handleEditEdgeClick);
  document.body.appendChild(editEdgeModal);

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && editEdgeModal && !editEdgeModal.hidden) {
      closeEditEdgeModal();
    }
  });
}

/** Populate the edit edge form with current properties */
function populateEditEdgeForm(properties: Record<string, unknown>): void {
  const container = document.getElementById("edit-edge-properties");
  if (!container) return;

  const error = document.getElementById("edit-edge-error");
  if (error) error.hidden = true;

  container.innerHTML = "";

  const entries = Object.entries(properties);
  for (const [key, value] of entries) {
    addPropertyRow(container, key, formatPropertyValue(value), false);
  }

  if (entries.length === 0) {
    addPropertyRow(container, "", "", true);
  }
}

/** Handle clicks in Edit Edge modal */
function handleEditEdgeClick(e: Event): void {
  const target = e.target as HTMLElement;

  if (target.classList.contains("modal-overlay")) {
    closeEditEdgeModal();
    return;
  }

  const actionEl = target.closest("[data-action]") as HTMLElement;
  if (!actionEl) return;

  const action = actionEl.getAttribute("data-action");

  switch (action) {
    case "close":
      closeEditEdgeModal();
      break;
    case "submit-edit-edge":
      submitEditEdge();
      break;
    case "add-edge-property": {
      const container = document.getElementById("edit-edge-properties");
      if (container) {
        addPropertyRow(container, "", "", true);
        const lastRow = container.lastElementChild;
        const keyInput = lastRow?.querySelector(".edit-prop-key") as HTMLInputElement;
        keyInput?.focus();
      }
      break;
    }
    case "remove-property": {
      const row = actionEl.closest(".edit-property-row");
      row?.remove();
      break;
    }
  }
}

/** Submit the edit edge form */
async function submitEditEdge(): Promise<void> {
  if (!editingEdgeContext) return;

  const { edgeId, sourceId, targetId, edgeType } = editingEdgeContext;
  const errorEl = document.getElementById("edit-edge-error");
  const properties = collectPropertiesFromForm("edit-edge-properties");

  try {
    await api.put(
      `/api/graph/relationships/${encodeURIComponent(sourceId)}/${encodeURIComponent(targetId)}/${encodeURIComponent(edgeType)}`,
      { properties }
    );

    // Update the graph view locally
    const renderer = getRenderer();
    const graph = renderer?.sigma.getGraph();
    if (graph?.hasEdge(edgeId)) {
      const existingProps = graph.getEdgeAttribute(edgeId, "properties") || {};
      graph.setEdgeAttribute(edgeId, "properties", { ...existingProps, ...properties });
      renderer?.refresh();

      // Refresh the detail panel
      const attrs = graph.getEdgeAttributes(edgeId);
      const sourceLabel = graph.getNodeAttribute(sourceId, "label") || sourceId;
      const targetLabel = graph.getNodeAttribute(targetId, "label") || targetId;
      updateDetailPanelForEdge(
        edgeId,
        attrs as Parameters<typeof updateDetailPanelForEdge>[1],
        sourceId,
        targetId,
        sourceLabel,
        targetLabel
      );
    }

    closeEditEdgeModal();
  } catch (err) {
    showError(errorEl, `Failed to update relationship: ${err instanceof Error ? err.message : String(err)}`);
  }
}
