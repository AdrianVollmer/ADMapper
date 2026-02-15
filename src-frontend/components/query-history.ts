/**
 * Query History Component
 *
 * Modal for viewing, editing, and re-running past queries.
 * History is stored in CozoDB via backend API.
 */

import { escapeHtml } from "../utils/html";
import { api } from "../api/client";
import type { QueryHistoryEntry, QueryHistoryResponse } from "../api/types";
import { executeQuery, getQueryErrorMessage } from "../utils/query";

// Re-export for backwards compatibility
export type { QueryHistoryEntry } from "../api/types";

/** Pagination state */
interface PaginationState {
  page: number;
  perPage: number;
  total: number;
}

/** Modal state */
let isOpen = false;
let entries: QueryHistoryEntry[] = [];
let pagination: PaginationState = { page: 1, perPage: 10, total: 0 };
let selectedEntry: QueryHistoryEntry | null = null;
let isEditing = false;
let editedQuery = "";
let editedName = "";
let isLoading = false;

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Initialize query history (call once at startup) */
export function initQueryHistory(): void {
  createModalElement();
  document.addEventListener("keydown", handleKeydown);
}

/** Create the modal element and append to body */
function createModalElement(): void {
  const modal = document.createElement("div");
  modal.id = "query-history-modal";
  modal.className = "modal-overlay";
  modal.setAttribute("hidden", "");
  modal.innerHTML = `
    <div class="modal-content modal-lg">
      <div class="modal-header">
        <h2 class="modal-title">Query History</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body" id="query-history-body">
        <!-- Content rendered dynamically -->
      </div>
      <div class="modal-footer" id="query-history-footer">
        <!-- Footer rendered dynamically -->
      </div>
    </div>
  `;

  modal.addEventListener("click", handleModalClick);
  document.body.appendChild(modal);
  modalEl = modal;
}

/** Open the query history modal */
export async function openQueryHistory(): Promise<void> {
  if (!modalEl) return;

  isOpen = true;
  selectedEntry = null;
  isEditing = false;
  pagination.page = 1;

  modalEl.removeAttribute("hidden");
  await loadHistory();
}

/** Close the modal */
export function closeQueryHistory(): void {
  if (!modalEl) return;

  isOpen = false;
  selectedEntry = null;
  isEditing = false;
  modalEl.setAttribute("hidden", "");
}

/** Load history from API */
async function loadHistory(): Promise<void> {
  isLoading = true;
  renderModal();

  try {
    const data = await api.get<QueryHistoryResponse>(
      `/api/query-history?page=${pagination.page}&per_page=${pagination.perPage}`
    );
    entries = data.entries;
    pagination.total = data.total;
  } catch (err) {
    console.error("Failed to load query history:", err);
    entries = [];
    pagination.total = 0;
  }

  isLoading = false;
  renderModal();
}

/** Add a query to history */
export async function addToHistory(name: string, query: string, resultCount?: number): Promise<void> {
  try {
    await api.post("/api/query-history", {
      name,
      query,
      result_count: resultCount ?? null,
    });
  } catch (err) {
    console.error("Failed to add to history:", err);
  }
}

/** Delete a query from history */
async function deleteEntry(id: string): Promise<void> {
  try {
    await api.delete(`/api/query-history/${id}`);
    if (selectedEntry?.id === id) {
      selectedEntry = null;
      isEditing = false;
    }
    await loadHistory();
  } catch (err) {
    console.error("Failed to delete from history:", err);
  }
}

/** Clear all history */
async function clearHistory(): Promise<void> {
  if (!confirm("Are you sure you want to clear all query history?")) {
    return;
  }

  try {
    await api.postNoContent("/api/query-history/clear");
    selectedEntry = null;
    isEditing = false;
    await loadHistory();
  } catch (err) {
    console.error("Failed to clear history:", err);
  }
}

/** Run a query */
async function runQuery(query: string, name: string): Promise<void> {
  closeQueryHistory();

  try {
    const result = await executeQuery(query, true);

    // Add to history
    await addToHistory(name, query, result.resultCount);

    // Show results
    if (result.graph && result.graph.nodes.length > 0) {
      // TODO: Load graph into renderer
      console.log("Query returned graph:", result.graph);
      alert(`Query returned ${result.graph.nodes.length} nodes and ${result.graph.edges.length} edges`);
    } else {
      alert(`Query returned ${result.resultCount} rows`);
    }
  } catch (err) {
    console.error("Query execution failed:", err);
    alert(`Query failed: ${getQueryErrorMessage(err)}`);
  }
}

/** Render the modal content */
function renderModal(): void {
  const body = document.getElementById("query-history-body");
  const footer = document.getElementById("query-history-footer");
  if (!body || !footer) return;

  if (isLoading) {
    body.innerHTML = `
      <div class="flex items-center justify-center py-8">
        <div class="spinner"></div>
        <span class="ml-3 text-gray-400">Loading history...</span>
      </div>
    `;
    footer.innerHTML = "";
    return;
  }

  if (isEditing && selectedEntry) {
    renderEditView(body, footer);
  } else if (selectedEntry) {
    renderDetailView(body, footer);
  } else {
    renderListView(body, footer);
  }
}

/** Render the list view */
function renderListView(body: HTMLElement, footer: HTMLElement): void {
  if (entries.length === 0) {
    body.innerHTML = `
      <div class="empty-state py-8">
        <svg class="empty-icon mx-auto" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
          <path d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"/>
        </svg>
        <p class="mt-2 text-gray-400">No query history yet</p>
      </div>
    `;
    footer.innerHTML = `
      <button class="btn btn-secondary" data-action="close">Close</button>
    `;
    return;
  }

  const totalPages = Math.ceil(pagination.total / pagination.perPage);

  body.innerHTML = `
    <div class="query-history-list">
      ${entries
        .map(
          (entry) => `
        <div class="query-history-item" data-action="select" data-id="${escapeHtml(entry.id)}">
          <div class="query-history-item-header">
            <span class="query-history-name">${escapeHtml(entry.name)}</span>
            <span class="query-history-time">${formatTimestamp(entry.timestamp)}</span>
          </div>
          <div class="query-history-query">${escapeHtml(truncate(entry.query, 100))}</div>
          ${entry.result_count !== null ? `<div class="query-history-results">${entry.result_count} results</div>` : ""}
        </div>
      `
        )
        .join("")}
    </div>

    ${
      totalPages > 1
        ? `
      <div class="query-history-pagination">
        <button class="btn btn-sm" data-action="prev-page" ${pagination.page <= 1 ? "disabled" : ""}>
          Previous
        </button>
        <span class="pagination-info">Page ${pagination.page} of ${totalPages}</span>
        <button class="btn btn-sm" data-action="next-page" ${pagination.page >= totalPages ? "disabled" : ""}>
          Next
        </button>
      </div>
    `
        : ""
    }
  `;

  footer.innerHTML = `
    <button class="btn btn-danger" data-action="clear-all">Clear All</button>
    <button class="btn btn-secondary" data-action="close">Close</button>
  `;
}

/** Render the detail view for a selected entry */
function renderDetailView(body: HTMLElement, footer: HTMLElement): void {
  if (!selectedEntry) return;

  body.innerHTML = `
    <div class="query-history-detail">
      <div class="detail-header">
        <h3 class="detail-name">${escapeHtml(selectedEntry.name)}</h3>
        <span class="detail-time">${formatTimestamp(selectedEntry.timestamp)}</span>
      </div>
      <div class="detail-section">
        <label class="detail-label">Query</label>
        <pre class="detail-query">${escapeHtml(selectedEntry.query)}</pre>
      </div>
      ${
        selectedEntry.result_count !== null
          ? `
        <div class="detail-section">
          <label class="detail-label">Last Result</label>
          <span class="detail-value">${selectedEntry.result_count} rows</span>
        </div>
      `
          : ""
      }
    </div>
  `;

  footer.innerHTML = `
    <button class="btn btn-danger" data-action="delete">Delete</button>
    <button class="btn btn-secondary" data-action="back">Back</button>
    <button class="btn btn-secondary" data-action="edit">Edit</button>
    <button class="btn btn-primary" data-action="run">Run Query</button>
  `;
}

/** Render the edit view */
function renderEditView(body: HTMLElement, footer: HTMLElement): void {
  body.innerHTML = `
    <div class="query-history-edit">
      <div class="form-group">
        <label class="form-label" for="edit-name">Name</label>
        <input
          type="text"
          id="edit-name"
          class="form-input"
          value="${escapeHtml(editedName)}"
        />
      </div>
      <div class="form-group">
        <label class="form-label" for="edit-query">Query</label>
        <textarea
          id="edit-query"
          class="form-textarea"
          rows="10"
        >${escapeHtml(editedQuery)}</textarea>
      </div>
    </div>
  `;

  footer.innerHTML = `
    <button class="btn btn-secondary" data-action="cancel-edit">Cancel</button>
    <button class="btn btn-primary" data-action="run-edited">Run Query</button>
  `;

  // Focus the query textarea
  setTimeout(() => {
    const textarea = document.getElementById("edit-query") as HTMLTextAreaElement;
    textarea?.focus();
  }, 0);
}

/** Handle clicks in the modal */
function handleModalClick(e: Event): void {
  const target = e.target as HTMLElement;

  // Close on backdrop click
  if (target.classList.contains("modal-overlay")) {
    closeQueryHistory();
    return;
  }

  // Handle action buttons
  const actionEl = target.closest("[data-action]") as HTMLElement;
  if (!actionEl) return;

  const action = actionEl.getAttribute("data-action");

  switch (action) {
    case "close":
      closeQueryHistory();
      break;

    case "select": {
      const id = actionEl.getAttribute("data-id");
      selectedEntry = entries.find((e) => e.id === id) || null;
      renderModal();
      break;
    }

    case "back":
      selectedEntry = null;
      isEditing = false;
      renderModal();
      break;

    case "edit":
      if (selectedEntry) {
        isEditing = true;
        editedName = selectedEntry.name;
        editedQuery = selectedEntry.query;
        renderModal();
      }
      break;

    case "cancel-edit":
      isEditing = false;
      renderModal();
      break;

    case "run":
      if (selectedEntry) {
        runQuery(selectedEntry.query, selectedEntry.name);
      }
      break;

    case "run-edited": {
      const nameInput = document.getElementById("edit-name") as HTMLInputElement;
      const queryTextarea = document.getElementById("edit-query") as HTMLTextAreaElement;
      if (nameInput && queryTextarea) {
        const name = nameInput.value.trim() || "Custom Query";
        const query = queryTextarea.value.trim();
        if (query) {
          runQuery(query, name);
        }
      }
      break;
    }

    case "delete":
      if (selectedEntry) {
        deleteEntry(selectedEntry.id);
      }
      break;

    case "clear-all":
      clearHistory();
      break;

    case "prev-page":
      if (pagination.page > 1) {
        pagination.page--;
        loadHistory();
      }
      break;

    case "next-page": {
      const totalPages = Math.ceil(pagination.total / pagination.perPage);
      if (pagination.page < totalPages) {
        pagination.page++;
        loadHistory();
      }
      break;
    }
  }
}

/** Handle keyboard shortcuts */
function handleKeydown(e: KeyboardEvent): void {
  if (!isOpen) return;

  if (e.key === "Escape") {
    if (isEditing) {
      isEditing = false;
      renderModal();
    } else if (selectedEntry) {
      selectedEntry = null;
      renderModal();
    } else {
      closeQueryHistory();
    }
  }
}

/** Format timestamp as relative time */
function formatTimestamp(timestamp: number): string {
  const now = Date.now() / 1000;
  const diff = now - timestamp;

  if (diff < 60) return "Just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  if (diff < 604800) return `${Math.floor(diff / 86400)}d ago`;

  const date = new Date(timestamp * 1000);
  return date.toLocaleDateString();
}

/** Truncate text */
function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text;
  return text.slice(0, maxLength) + "...";
}
