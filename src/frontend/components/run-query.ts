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

/** Encapsulated mutable state for the run-query modal */
interface RunQueryState {
  queryText: string;
  isExecuting: boolean;
  currentQueryId: string | null;
  unsubscribeProgress: Unsubscribe | null;
  currentAbortController: AbortController | null;
  errorMessage: string;
  infoMessage: string;
  currentDurationMs: number;
  durationInterval: ReturnType<typeof setInterval> | null;
  queryStartTime: number | null;
}

function createInitialRunQueryState(): RunQueryState {
  return {
    queryText: "",
    isExecuting: false,
    currentQueryId: null,
    unsubscribeProgress: null,
    currentAbortController: null,
    errorMessage: "",
    infoMessage: "",
    currentDurationMs: 0,
    durationInterval: null,
    queryStartTime: null,
  };
}

let state = createInitialRunQueryState();

/** Reset all mutable state (called on modal close) */
function resetState(): void {
  state = createInitialRunQueryState();
}

/** Modal element (DOM reference, not reset with state) */
let modalEl: HTMLElement | null = null;

/** Initialize the run query modal */
export function initRunQuery(): void {
  createModalElement();
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

  state.isExecuting = false;
  state.errorMessage = "";
  state.infoMessage = "";
  state.currentQueryId = null;
  state.currentDurationMs = 0;
  state.queryStartTime = null;

  // Try to load the last query from history
  try {
    const data = await api.get<QueryHistoryResponse>("/api/query-history?page=1&per_page=1");
    if (data.entries.length > 0) {
      state.queryText = data.entries[0]?.query ?? "";
    }
  } catch {
    // Ignore errors, just start with empty query
  }

  modalEl.removeAttribute("hidden");
  // Add keyboard listener when modal opens (removed on close to prevent leaks)
  document.addEventListener("keydown", handleKeydown);
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
  if (state.unsubscribeProgress) {
    state.unsubscribeProgress();
    state.unsubscribeProgress = null;
  }

  // Clear duration interval
  if (state.durationInterval) {
    clearInterval(state.durationInterval);
    state.durationInterval = null;
  }

  // Remove keyboard listener when modal closes to prevent leaks
  document.removeEventListener("keydown", handleKeydown);

  modalEl.setAttribute("hidden", "");
  resetState();
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
          ${state.isExecuting ? "disabled" : ""}
        >${escapeHtml(state.queryText)}</textarea>
      </div>

      ${
        state.isExecuting
          ? `
        <div class="query-executing">
          <div class="flex items-center gap-3">
            <div class="spinner"></div>
            <span class="text-gray-300">Executing query...</span>
            <span class="text-gray-500">${formatDuration(state.currentDurationMs)}</span>
          </div>
        </div>
      `
          : ""
      }

      ${
        state.errorMessage
          ? `
        <div class="query-error">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="error-icon">
            <circle cx="12" cy="12" r="10"/>
            <path d="M12 8v4m0 4h.01"/>
          </svg>
          <pre class="query-error-text"><code>${escapeHtml(state.errorMessage)}</code></pre>
        </div>
      `
          : ""
      }

      ${
        state.infoMessage
          ? `
        <div class="query-info">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="info-icon">
            <circle cx="12" cy="12" r="10"/>
            <path d="M12 16v-4m0-4h.01"/>
          </svg>
          <span>${escapeHtml(state.infoMessage)}</span>
        </div>
      `
          : ""
      }
    </div>
  `;

  if (state.isExecuting) {
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
    state.errorMessage = "Please enter a query";
    renderModal();
    return;
  }

  state.isExecuting = true;
  state.errorMessage = "";
  state.infoMessage = "";
  state.queryText = query;
  state.queryStartTime = Date.now();
  state.currentDurationMs = 0;
  renderModal();

  // Start duration update interval
  state.durationInterval = setInterval(() => {
    if (state.queryStartTime) {
      state.currentDurationMs = Date.now() - state.queryStartTime;
      renderModal();
    }
  }, 100);

  try {
    // Start the async query
    const startResponse = await api.post<QueryStartResponse>("/api/graph/query", {
      query,
      extract_graph: true,
    });

    state.currentQueryId = startResponse.query_id;

    // Register as the current foreground query (aborts any previous foreground query)
    state.currentAbortController = registerForegroundQuery(state.currentQueryId, () => {
      // This cleanup is called if another query aborts us
      if (state.unsubscribeProgress) {
        state.unsubscribeProgress();
        state.unsubscribeProgress = null;
      }
    });

    // Listen for abort from the foreground query system
    state.currentAbortController.signal.addEventListener("abort", () => {
      if (state.isExecuting) {
        cleanup();
        state.infoMessage = "Query was superseded by a new query";
        renderModal();
      }
    });

    // Subscribe to progress events
    state.unsubscribeProgress = subscribe(
      QUERY_PROGRESS_CHANNEL,
      { queryId: state.currentQueryId, query_id: state.currentQueryId },
      (progress) => {
        // Ignore events if we've been aborted
        if (state.currentAbortController?.signal.aborted) {
          return;
        }
        handleQueryProgress(progress as QueryProgressEvent);
      },
      () => {
        // Connection closed, check if we're still executing and not aborted
        if (state.isExecuting && !state.currentAbortController?.signal.aborted) {
          cleanup();
          state.errorMessage = "Lost connection to server";
          renderModal();
        }
      }
    );
  } catch (err) {
    cleanup();
    state.errorMessage = getQueryErrorMessage(err);
    renderModal();
  }
}

/** Handle query progress event */
function handleQueryProgress(progress: QueryProgressEvent): void {
  state.currentDurationMs = progress.duration_ms ?? (state.queryStartTime ? Date.now() - state.queryStartTime : 0);

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
        state.infoMessage = `Query returned ${progress.result_count ?? 0} row${progress.result_count === 1 ? "" : "s"}`;
        renderModal();
      }
      break;

    case "failed":
      cleanup();
      state.errorMessage = progress.error ?? "Query failed";
      renderModal();
      break;

    case "aborted":
      cleanup();
      state.infoMessage = "Query was aborted";
      renderModal();
      break;
  }
}

/** Abort the running query */
async function abortQuery(): Promise<void> {
  if (!state.currentQueryId) return;

  try {
    await api.postNoContent(`/api/query/abort/${state.currentQueryId}`);
    // The SSE will receive the aborted status
  } catch (err) {
    console.error("Failed to abort query:", err);
    cleanup();
    state.errorMessage = "Failed to abort query";
    renderModal();
  }
}

/** Clean up after query completes */
function cleanup(): void {
  state.isExecuting = false;

  // Unregister from foreground query tracking
  if (state.currentQueryId) {
    unregisterForegroundQuery(state.currentQueryId);
  }
  state.currentQueryId = null;
  state.currentAbortController = null;

  if (state.unsubscribeProgress) {
    state.unsubscribeProgress();
    state.unsubscribeProgress = null;
  }

  if (state.durationInterval) {
    clearInterval(state.durationInterval);
    state.durationInterval = null;
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
    if (!state.isExecuting) {
      executeQuery();
    }
  }
}
