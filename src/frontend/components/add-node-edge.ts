/**
 * Add Node/Relationship Component
 *
 * Modals for adding new nodes and relationships to the graph.
 */

import { api } from "../api/client";
import { escapeHtml } from "../utils/html";

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
