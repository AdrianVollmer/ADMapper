/**
 * Run Query Modal
 *
 * Provides a modal for entering and executing database queries.
 * Supports Cypher queries (CrustDB, Neo4j, FalkorDB).
 * Queries run asynchronously with progress tracking and abort support.
 */

import { escapeHtml } from "../utils/html";
import { api } from "../api/client";
import type { QueryHistoryResponse, QueryStartResponse, QueryProgressEvent } from "../api/types";
import { loadGraphData } from "./graph-view";
import type { RawADGraph } from "../graph/types";
import { subscribe, QUERY_PROGRESS_CHANNEL, type Unsubscribe } from "../api/transport";
import {
  registerForegroundQuery,
  unregisterForegroundQuery,
  getQueryErrorMessage,
  formatDuration,
} from "../utils/query";

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Current query text */
let queryText = "";

/** Is query executing */
let isExecuting = false;

/** Current query ID (for abort) */
let currentQueryId: string | null = null;

/** Unsubscribe function for progress events */
let unsubscribeProgress: Unsubscribe | null = null;

/** Abort controller for the current query (from foreground query registration) */
let currentAbortController: AbortController | null = null;

/** Error message */
let errorMessage = "";

/** Info message (e.g., zero rows returned) */
let infoMessage = "";

/** Current duration for running queries */
let currentDurationMs = 0;

/** Duration update interval */
let durationInterval: ReturnType<typeof setInterval> | null = null;

/** Query start time */
let queryStartTime: number | null = null;

/** Initialize the run query modal */
export function initRunQuery(): void {
  createModalElement();
  // Non-Escape keyboard shortcuts (Ctrl+Enter) -- Escape is handled globally in main.ts
  document.addEventListener("keydown", handleKeydown);
}

/** Create the modal element */
function createModalElement(): void {
  const modal = document.createElement("div");
  modal.id = "run-query-modal";
  modal.className = "modal-overlay";
  modal.setAttribute("hidden", "");

  modal.innerHTML = `
    <div class="modal-content modal-lg">
      <div class="modal-header">
        <h2 class="modal-title">Run Query</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body" id="run-query-body">
        <!-- Content rendered dynamically -->
      </div>
      <div class="modal-footer" id="run-query-footer">
        <!-- Footer rendered dynamically -->
      </div>
    </div>
  `;

  modal.addEventListener("click", handleModalClick);
  document.body.appendChild(modal);
  modalEl = modal;
}

/** Open the run query modal */
export async function openRunQuery(): Promise<void> {
  if (!modalEl) return;

  isExecuting = false;
  errorMessage = "";
  infoMessage = "";
  currentQueryId = null;
  currentDurationMs = 0;
  queryStartTime = null;

  // Try to load the last query from history
  try {
    const data = await api.get<QueryHistoryResponse>("/api/query-history?page=1&per_page=1");
    if (data.entries.length > 0) {
      queryText = data.entries[0]?.query ?? "";
    }
  } catch {
    // Ignore errors, just start with empty query
  }

  modalEl.removeAttribute("hidden");
  renderModal();

  // Focus the textarea after render
  setTimeout(() => {
    const textarea = document.getElementById("query-input") as HTMLTextAreaElement;
    if (textarea) {
      textarea.focus();
      // Move cursor to end
      textarea.setSelectionRange(textarea.value.length, textarea.value.length);
    }
  }, 50);
}

/** Close the modal */
export function closeRunQuery(): void {
  if (!modalEl) return;

  // Clean up event subscription
  if (unsubscribeProgress) {
    unsubscribeProgress();
    unsubscribeProgress = null;
  }

  // Clear duration interval
  if (durationInterval) {
    clearInterval(durationInterval);
    durationInterval = null;
  }

  modalEl.setAttribute("hidden", "");
}

/** Get the documentation URL for Cypher */
function getDocsUrl(): string {
  return "https://neo4j.com/docs/cypher-manual/current/";
}

/** Render the modal content */
function renderModal(): void {
  const body = document.getElementById("run-query-body");
  const footer = document.getElementById("run-query-footer");
  if (!body || !footer) return;

  const docsUrl = getDocsUrl();

  body.innerHTML = `
    <div class="run-query-content">
      <div class="query-language-info">
        <span class="language-badge">Cypher</span>
        <a href="${docsUrl}" target="_blank" rel="noopener noreferrer" class="docs-link">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="docs-icon">
            <path d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"/>
          </svg>
          View Cypher Documentation
        </a>
      </div>

      <div class="form-group">
        <label class="form-label" for="query-input">Query</label>
        <textarea
          id="query-input"
          class="form-textarea query-textarea"
          rows="12"
          placeholder="MATCH (n:User) RETURN n LIMIT 10"
          spellcheck="false"
          ${isExecuting ? "disabled" : ""}
        >${escapeHtml(queryText)}</textarea>
      </div>

      ${
        isExecuting
          ? `
        <div class="query-executing">
          <div class="flex items-center gap-3">
            <div class="spinner"></div>
            <span class="text-gray-300">Executing query...</span>
            <span class="text-gray-500">${formatDuration(currentDurationMs)}</span>
          </div>
        </div>
      `
          : ""
      }

      ${
        errorMessage
          ? `
        <div class="query-error">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="error-icon">
            <circle cx="12" cy="12" r="10"/>
            <path d="M12 8v4m0 4h.01"/>
          </svg>
          <pre class="query-error-text"><code>${escapeHtml(errorMessage)}</code></pre>
        </div>
      `
          : ""
      }

      ${
        infoMessage
          ? `
        <div class="query-info">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="info-icon">
            <circle cx="12" cy="12" r="10"/>
            <path d="M12 16v-4m0-4h.01"/>
          </svg>
          <span>${escapeHtml(infoMessage)}</span>
        </div>
      `
          : ""
      }
    </div>
  `;

  if (isExecuting) {
    footer.innerHTML = `
      <button class="btn btn-secondary" data-action="close">Cancel</button>
      <button class="btn btn-danger" data-action="abort">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="btn-icon">
          <rect x="6" y="6" width="12" height="12" rx="2"/>
        </svg>
        Abort Query
      </button>
    `;
  } else {
    footer.innerHTML = `
      <button class="btn btn-secondary" data-action="close">Cancel</button>
      <button class="btn btn-primary" data-action="execute">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="btn-icon">
          <path d="M5 3l14 9-14 9V3z"/>
        </svg>
        Execute
      </button>
    `;
  }
}

/** Execute the query */
async function executeQuery(): Promise<void> {
  const textarea = document.getElementById("query-input") as HTMLTextAreaElement;
  if (!textarea) return;

  const query = textarea.value.trim();
  if (!query) {
    errorMessage = "Please enter a query";
    renderModal();
    return;
  }

  isExecuting = true;
  errorMessage = "";
  infoMessage = "";
  queryText = query;
  queryStartTime = Date.now();
  currentDurationMs = 0;
  renderModal();

  // Start duration update interval
  durationInterval = setInterval(() => {
    if (queryStartTime) {
      currentDurationMs = Date.now() - queryStartTime;
      renderModal();
    }
  }, 100);

  try {
    // Start the async query
    const startResponse = await api.post<QueryStartResponse>("/api/graph/query", {
      query,
      extract_graph: true,
    });

    currentQueryId = startResponse.query_id;

    // Register as the current foreground query (aborts any previous foreground query)
    currentAbortController = registerForegroundQuery(currentQueryId, () => {
      // This cleanup is called if another query aborts us
      if (unsubscribeProgress) {
        unsubscribeProgress();
        unsubscribeProgress = null;
      }
    });

    // Listen for abort from the foreground query system
    currentAbortController.signal.addEventListener("abort", () => {
      if (isExecuting) {
        cleanup();
        infoMessage = "Query was superseded by a new query";
        renderModal();
      }
    });

    // Subscribe to progress events
    unsubscribeProgress = subscribe(
      QUERY_PROGRESS_CHANNEL,
      { queryId: currentQueryId, query_id: currentQueryId },
      (progress) => {
        // Ignore events if we've been aborted
        if (currentAbortController?.signal.aborted) {
          return;
        }
        handleQueryProgress(progress as QueryProgressEvent);
      },
      () => {
        // Connection closed, check if we're still executing and not aborted
        if (isExecuting && !currentAbortController?.signal.aborted) {
          cleanup();
          errorMessage = "Lost connection to server";
          renderModal();
        }
      }
    );
  } catch (err) {
    cleanup();
    errorMessage = getQueryErrorMessage(err);
    renderModal();
  }
}

/** Handle query progress event */
function handleQueryProgress(progress: QueryProgressEvent): void {
  currentDurationMs = progress.duration_ms ?? (queryStartTime ? Date.now() - queryStartTime : 0);

  switch (progress.status) {
    case "running":
      // Still running, just update duration
      renderModal();
      break;

    case "completed":
      cleanup();
      // Load the graph if we got one with nodes
      if (progress.graph && progress.graph.nodes.length > 0) {
        closeRunQuery();
        loadGraphData(progress.graph as unknown as RawADGraph);
      } else {
        infoMessage = `Query returned ${progress.result_count ?? 0} row${progress.result_count === 1 ? "" : "s"}`;
        renderModal();
      }
      break;

    case "failed":
      cleanup();
      errorMessage = progress.error ?? "Query failed";
      renderModal();
      break;

    case "aborted":
      cleanup();
      infoMessage = "Query was aborted";
      renderModal();
      break;
  }
}

/** Abort the running query */
async function abortQuery(): Promise<void> {
  if (!currentQueryId) return;

  try {
    await api.postNoContent(`/api/query/abort/${currentQueryId}`);
    // The SSE will receive the aborted status
  } catch (err) {
    console.error("Failed to abort query:", err);
    cleanup();
    errorMessage = "Failed to abort query";
    renderModal();
  }
}

/** Clean up after query completes */
function cleanup(): void {
  isExecuting = false;

  // Unregister from foreground query tracking
  if (currentQueryId) {
    unregisterForegroundQuery(currentQueryId);
  }
  currentQueryId = null;
  currentAbortController = null;

  if (unsubscribeProgress) {
    unsubscribeProgress();
    unsubscribeProgress = null;
  }

  if (durationInterval) {
    clearInterval(durationInterval);
    durationInterval = null;
  }
}

/** Handle clicks in the modal */
function handleModalClick(e: Event): void {
  const target = e.target as HTMLElement;

  // Close on backdrop click
  if (target.classList.contains("modal-overlay")) {
    closeRunQuery();
    return;
  }

  const actionEl = target.closest("[data-action]") as HTMLElement;
  if (!actionEl) return;

  const action = actionEl.getAttribute("data-action");

  switch (action) {
    case "close":
      closeRunQuery();
      break;

    case "execute":
      executeQuery();
      break;

    case "abort":
      abortQuery();
      break;
  }
}

/** Handle non-Escape keyboard shortcuts (Escape is handled globally in main.ts) */
function handleKeydown(e: KeyboardEvent): void {
  if (!modalEl || modalEl.hasAttribute("hidden")) return;

  // Ctrl+Enter or Cmd+Enter to execute
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
    e.preventDefault();
    if (!isExecuting) {
      executeQuery();
    }
  }
}
