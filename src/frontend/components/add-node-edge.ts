/**
 * Add Node/Edge Component
 *
 * Modals for adding new nodes and edges to the graph.
 */

import { api } from "../api/client";
import { escapeHtml } from "../utils/html";

/** Available edge types */
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

/** Add Edge modal element */
let addEdgeModal: HTMLElement | null = null;

/** Add Node modal element */
let addNodeModal: HTMLElement | null = null;

/** Debounce timer for search */
let searchTimer: ReturnType<typeof setTimeout> | null = null;

/** Initialize the add node/edge modals */
export function initAddNodeEdge(): void {
  // Modals are created on demand
}

/** Open the Add Edge modal */
export function openAddEdge(): void {
  if (!addEdgeModal) {
    createAddEdgeModal();
  }
  addEdgeModal!.hidden = false;
  resetAddEdgeForm();
  // Focus first input
  const sourceInput = document.getElementById("add-edge-source") as HTMLInputElement;
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

/** Close Add Edge modal */
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

/** Create the Add Edge modal */
function createAddEdgeModal(): void {
  addEdgeModal = document.createElement("div");
  addEdgeModal.id = "add-edge-modal";
  addEdgeModal.className = "modal-overlay";

  // Build edge type options
  const edgeTypeOptions = COMMON_EDGE_TYPES.map(
    (type) => `<option value="${escapeHtml(type)}">${escapeHtml(type)}</option>`
  ).join("");

  addEdgeModal.innerHTML = `
    <div class="modal-content">
      <div class="modal-header">
        <h2 class="modal-title">Add Edge</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body">
        <form id="add-edge-form" class="add-form">
          <div class="form-group">
            <label for="add-edge-source" class="form-label">Source Node</label>
            <div class="search-input-wrapper">
              <input
                type="text"
                id="add-edge-source"
                class="form-input"
                placeholder="Search for source node..."
                autocomplete="off"
              />
              <input type="hidden" id="add-edge-source-id" />
              <div id="add-edge-source-results" class="search-results" hidden></div>
            </div>
          </div>

          <div class="form-group">
            <label for="add-edge-type" class="form-label">Edge Type</label>
            <select id="add-edge-type" class="form-select">
              <option value="">Select edge type...</option>
              ${edgeTypeOptions}
            </select>
          </div>

          <div class="form-group">
            <label for="add-edge-target" class="form-label">Target Node</label>
            <div class="search-input-wrapper">
              <input
                type="text"
                id="add-edge-target"
                class="form-input"
                placeholder="Search for target node..."
                autocomplete="off"
              />
              <input type="hidden" id="add-edge-target-id" />
              <div id="add-edge-target-results" class="search-results" hidden></div>
            </div>
          </div>

          <div id="add-edge-error" class="form-error" hidden></div>
        </form>
      </div>
      <div class="modal-footer">
        <button class="btn btn-secondary" data-action="close">Cancel</button>
        <button class="btn btn-primary" data-action="submit-edge">Add Edge</button>
      </div>
    </div>
  `;

  addEdgeModal.addEventListener("click", handleAddEdgeClick);
  setupSearchInput("add-edge-source", "add-edge-source-results", "add-edge-source-id");
  setupSearchInput("add-edge-target", "add-edge-target-results", "add-edge-target-id");

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

/** Handle clicks in Add Edge modal */
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
    case "submit-edge":
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

/** Submit add edge form */
async function submitAddEdge(): Promise<void> {
  const sourceId = (document.getElementById("add-edge-source-id") as HTMLInputElement).value;
  const targetId = (document.getElementById("add-edge-target-id") as HTMLInputElement).value;
  const edgeType = (document.getElementById("add-edge-type") as HTMLSelectElement).value;
  const errorEl = document.getElementById("add-edge-error");

  // Validate
  if (!sourceId) {
    showError(errorEl, "Please select a source node from the search results");
    return;
  }
  if (!edgeType) {
    showError(errorEl, "Please select an edge type");
    return;
  }
  if (!targetId) {
    showError(errorEl, "Please select a target node from the search results");
    return;
  }

  try {
    await api.post("/api/graph/edge", {
      source: sourceId,
      target: targetId,
      edge_type: edgeType,
    });

    closeAddEdgeModal();

    // Refresh the graph
    window.location.reload();
  } catch (err) {
    showError(errorEl, `Failed to add edge: ${err instanceof Error ? err.message : String(err)}`);
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

/** Reset Add Edge form */
function resetAddEdgeForm(): void {
  const form = document.getElementById("add-edge-form") as HTMLFormElement;
  if (form) form.reset();

  const sourceId = document.getElementById("add-edge-source-id") as HTMLInputElement;
  const targetId = document.getElementById("add-edge-target-id") as HTMLInputElement;
  if (sourceId) sourceId.value = "";
  if (targetId) targetId.value = "";

  const error = document.getElementById("add-edge-error");
  if (error) error.hidden = true;

  const sourceResults = document.getElementById("add-edge-source-results");
  const targetResults = document.getElementById("add-edge-target-results");
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
