/**
 * Query History Component
 *
 * Modal for viewing, editing, and re-running past queries.
 * Shows query status (running/completed/failed/aborted), duration, and supports abort.
 */

import { escapeHtml } from "../utils/html";
import { api } from "../api/client";
import type { QueryHistoryEntry, QueryHistoryResponse, QueryStatus } from "../api/types";
import type { RawADGraph } from "../graph";
import {
  executeQueryWithHistory,
  getQueryErrorMessage,
  setForegroundQueryCallback,
  QueryAbortedError,
  formatDuration,
} from "../utils/query";
import { hasActiveQueries } from "./query-activity";
import { loadGraphData } from "./graph-view";
import { showSuccess, showError, showInfo, showConfirm } from "../utils/notifications";

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

/**
 * History navigation cursor.
 * Tracks position in foreground query history for back navigation.
 * 0 = current query, 1 = previous query, etc.
 */
let historyCursor = 0;

/** Live duration update interval */
let durationInterval: ReturnType<typeof setInterval> | null = null;

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Initialize query history (call once at startup) */
export function initQueryHistory(): void {
  createModalElement();

  // Reset history cursor when a new foreground query starts
  setForegroundQueryCallback(() => {
    historyCursor = 0;
  });
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

  // Start live duration updates for running queries
  startDurationUpdates();
}

/** Close the modal */
export function closeQueryHistory(): void {
  if (!modalEl) return;

  isOpen = false;
  selectedEntry = null;
  isEditing = false;
  stopDurationUpdates();
  modalEl.setAttribute("hidden", "");
}

/** Start live duration updates for running queries */
function startDurationUpdates(): void {
  stopDurationUpdates();
  durationInterval = setInterval(() => {
    // Check if any queries are still running in history OR if indicator shows active queries
    // The indicator tracks both sync and async queries, but only async queries appear in history
    const hasRunningInHistory = entries.some((e) => e.status === "running");
    const hasActiveInIndicator = hasActiveQueries();

    if (hasRunningInHistory || hasActiveInIndicator) {
      // Poll for updates from backend (new queries or status changes)
      refreshHistoryQuietly();
    }
    // Update durations without full re-render
    updateDurationsInPlace();
  }, 1000);
}

/** Refresh history from backend without resetting scroll position */
async function refreshHistoryQuietly(): Promise<void> {
  try {
    const data = await api.get<QueryHistoryResponse>(
      `/api/query-history?page=${pagination.page}&per_page=${pagination.perPage}`
    );
    // Check if any status changed
    const statusChanged = data.entries.some((newEntry) => {
      const oldEntry = entries.find((e) => e.id === newEntry.id);
      return oldEntry && oldEntry.status !== newEntry.status;
    });
    if (statusChanged || data.entries.length !== entries.length) {
      entries = data.entries;
      pagination.total = data.total;
      // Full re-render needed for status changes, but preserve scroll
      renderModalPreservingScroll();
    }
  } catch {
    // Silently ignore refresh errors
  }
}

/** Update live durations in place without full re-render */
function updateDurationsInPlace(): void {
  for (const entry of entries) {
    if (entry.status !== "running") continue;

    // Update duration display in list view
    const durationEl = document.querySelector(`[data-id="${entry.id}"] .query-history-duration`);
    if (durationEl) {
      durationEl.textContent = formatDuration(getLiveDuration(entry));
    }
  }

  // Update detail view duration if showing a running query
  if (selectedEntry && selectedEntry.status === "running") {
    const detailDuration = document.querySelector(".detail-value");
    if (detailDuration) {
      detailDuration.textContent = formatDuration(getLiveDuration(selectedEntry));
    }
  }
}

/** Render modal while preserving scroll position */
function renderModalPreservingScroll(): void {
  const body = document.getElementById("query-history-body");
  const scrollTop = body?.scrollTop ?? 0;
  renderModal();
  if (body) {
    body.scrollTop = scrollTop;
  }
}

/** Stop live duration updates */
function stopDurationUpdates(): void {
  if (durationInterval) {
    clearInterval(durationInterval);
    durationInterval = null;
  }
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
  const confirmed = await showConfirm("Are you sure you want to clear all query history?", {
    title: "Clear History",
    confirmText: "Clear All",
    danger: true,
  });
  if (!confirmed) {
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

/** Abort all running queries */
async function abortAllQueries(): Promise<void> {
  const running = entries.filter((e) => e.status === "running");
  try {
    await Promise.all(running.map((e) => api.postNoContent(`/api/query/abort/${e.id}`)));
    await loadHistory();
  } catch (err) {
    console.error("Failed to abort queries:", err);
  }
}

/** Abort a running query */
async function abortQuery(queryId: string): Promise<void> {
  try {
    await api.postNoContent(`/api/query/abort/${queryId}`);
    // Reload to see updated status
    await loadHistory();
  } catch (err) {
    console.error("Failed to abort query:", err);
  }
}

/** Run a query */
async function runQuery(query: string, name: string): Promise<void> {
  closeQueryHistory();

  try {
    const result = await executeQueryWithHistory(name, query, true);

    // Show results
    if (result.graph && result.graph.nodes.length > 0) {
      loadGraphData(result.graph as unknown as RawADGraph);
      showSuccess(
        `Query returned ${result.graph.nodes.length} nodes and ${result.graph.relationships.length} relationships`
      );
    } else {
      showInfo(`Query returned ${result.resultCount} rows`);
    }
  } catch (err) {
    // Silently ignore aborted queries (user started a new query)
    if (err instanceof QueryAbortedError) {
      return;
    }
    console.error("Query execution failed:", err);
    showError(`Query failed: ${getQueryErrorMessage(err)}`);
  }
}

/** Calculate live duration for running queries */
function getLiveDuration(entry: QueryHistoryEntry): number {
  if (entry.status !== "running") {
    return entry.duration_ms ?? 0;
  }
  // Calculate live duration from started_at
  const now = Math.floor(Date.now() / 1000);
  return (now - entry.started_at) * 1000;
}

/** Get status badge HTML */
function getStatusBadge(status: QueryStatus, isRunning = false): string {
  const classes: Record<QueryStatus, string> = {
    running: "status-badge status-running",
    completed: "status-badge status-completed",
    failed: "status-badge status-failed",
    aborted: "status-badge status-aborted",
  };

  const labels: Record<QueryStatus, string> = {
    running: "Running",
    completed: "Completed",
    failed: "Failed",
    aborted: "Aborted",
  };

  const spinnerHtml = isRunning ? '<span class="spinner-xs mr-1"></span>' : "";

  return `<span class="${classes[status]}">${spinnerHtml}${labels[status]}</span>`;
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

  const hasRunning = entries.some((e) => e.status === "running");

  body.innerHTML = `
    <div class="query-history-list">
      ${entries
        .map((entry) => {
          const isRunning = entry.status === "running";
          const duration = getLiveDuration(entry);
          return `
        <div class="query-history-item" data-action="select" data-id="${escapeHtml(entry.id)}" title="${escapeHtml(entry.query)}">
          <div class="query-history-item-header">
            <div class="query-history-query">${escapeHtml(truncate(entry.query, 120))}</div>
            <div class="query-history-meta">
              ${getStatusBadge(entry.status, isRunning)}
              ${entry.background ? '<span class="badge-background">BG</span>' : ""}
              ${
                isRunning
                  ? `<button class="btn btn-sm btn-danger ml-2" data-action="abort" data-id="${escapeHtml(entry.id)}">Abort</button>`
                  : ""
              }
            </div>
          </div>
          <div class="query-history-footer">
            <span class="query-history-time">${formatTimestamp(entry.started_at)}</span>
            ${entry.result_count !== null ? `<span class="query-history-results">${entry.result_count} results</span>` : ""}
            <span class="query-history-duration">${formatDuration(duration)}</span>
            ${entry.error ? `<span class="query-history-error" title="${escapeHtml(entry.error)}">Error</span>` : ""}
          </div>
        </div>
      `;
        })
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
    ${hasRunning ? '<button class="btn btn-danger" data-action="abort-all">Abort All</button>' : ""}
    <button class="btn btn-danger" data-action="clear-all">Clear All</button>
    <button class="btn btn-secondary" data-action="close">Close</button>
  `;
}

/** Render the detail view for a selected entry */
function renderDetailView(body: HTMLElement, footer: HTMLElement): void {
  if (!selectedEntry) return;

  const isRunning = selectedEntry.status === "running";
  const duration = getLiveDuration(selectedEntry);

  body.innerHTML = `
    <div class="query-history-detail">
      <div class="detail-header">
        <h3 class="detail-name">${escapeHtml(selectedEntry.name)}</h3>
        <div class="detail-meta">
          ${getStatusBadge(selectedEntry.status, isRunning)}
          <span class="detail-time">${formatTimestamp(selectedEntry.started_at)}</span>
        </div>
      </div>
      <div class="detail-section">
        <label class="detail-label">Query</label>
        <pre class="detail-query">${escapeHtml(selectedEntry.query)}</pre>
      </div>
      <div class="detail-stats">
        <div class="detail-stat">
          <label class="detail-label">Duration</label>
          <span class="detail-value">${formatDuration(duration)}</span>
        </div>
        ${
          selectedEntry.result_count !== null
            ? `
          <div class="detail-stat">
            <label class="detail-label">Results</label>
            <span class="detail-value">${selectedEntry.result_count} rows</span>
          </div>
        `
            : ""
        }
      </div>
      ${
        selectedEntry.error
          ? `
        <div class="detail-section">
          <label class="detail-label">Error</label>
          <pre class="detail-error">${escapeHtml(selectedEntry.error)}</pre>
        </div>
      `
          : ""
      }
    </div>
  `;

  if (isRunning) {
    footer.innerHTML = `
      <button class="btn btn-danger" data-action="abort" data-id="${escapeHtml(selectedEntry.id)}">Abort</button>
      <button class="btn btn-secondary" data-action="back">Back</button>
    `;
  } else {
    footer.innerHTML = `
      <button class="btn btn-danger" data-action="delete">Delete</button>
      <button class="btn btn-secondary" data-action="back">Back</button>
      <button class="btn btn-secondary" data-action="edit">Edit</button>
      <button class="btn btn-primary" data-action="run">Run Query</button>
    `;
  }
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

    case "abort": {
      const id = actionEl.getAttribute("data-id");
      if (id) {
        abortQuery(id);
      }
      break;
    }

    case "abort-all":
      abortAllQueries();
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

/** Handle Escape key for this modal (called by global Escape handler) */
export function handleEscapeKey(): void {
  if (!isOpen) return;

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

/** Reset history cursor (call when a new foreground query is run) */
export function resetHistoryCursor(): void {
  historyCursor = 0;
}

/** Go back to the previous query in history (re-run the one before the current) */
export async function goBackInHistory(): Promise<boolean> {
  try {
    // Fetch enough entries to find non-background queries
    const data = await api.get<QueryHistoryResponse>("/api/query-history?page=1&per_page=50");

    // Filter to non-background queries only
    const foregroundQueries = data.entries.filter((entry) => !entry.background);

    // Get the query at cursor+1 position (next one back from current position)
    const targetIndex = historyCursor + 1;
    const previousEntry = foregroundQueries[targetIndex];
    if (!previousEntry) {
      showInfo("No more history to go back to");
      return false;
    }

    // Increment cursor before executing
    historyCursor = targetIndex;

    // Execute as background query so it doesn't pollute history
    const result = await executeQueryWithHistory(`${previousEntry.name} (replay)`, previousEntry.query, {
      extractGraph: true,
      background: true,
    });

    // Show results and render graph
    if (result.graph && result.graph.nodes.length > 0) {
      loadGraphData(result.graph as unknown as RawADGraph);
      showSuccess(`Back to: ${previousEntry.name}`);
    } else {
      showInfo(`Query returned ${result.resultCount} rows`);
    }

    return true;
  } catch (err) {
    // Silently ignore aborted queries
    if (err instanceof QueryAbortedError) {
      return false;
    }
    console.error("Failed to go back in history:", err);
    return false;
  }
}
