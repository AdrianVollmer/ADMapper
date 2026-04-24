/**
 * Run Query Modal
 *
 * Provides a modal for entering and executing database queries.
 * Supports Cypher queries (CrustDB, Neo4j, FalkorDB).
 *
 * Uses the shared executeQuery utility so sync (Tauri IPC) and async
 * (HTTP SSE) modes are handled transparently — no mode-specific logic here.
 */

import { escapeHtml } from "../utils/html";
import { api } from "../api/client";
import type { QueryHistoryResponse } from "../api/types";
import { loadGraphData } from "./graph-view";
import type { RawADGraph } from "../graph/types";
import {
  executeQuery as sharedExecuteQuery,
  abortCurrentForegroundQuery,
  QueryAbortedError,
  getQueryErrorMessage,
  formatDuration,
} from "../utils/query";

interface RunQueryState {
  queryText: string;
  isExecuting: boolean;
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
    errorMessage: "",
    infoMessage: "",
    currentDurationMs: 0,
    durationInterval: null,
    queryStartTime: null,
  };
}

let state = createInitialRunQueryState();

function resetState(): void {
  state = createInitialRunQueryState();
}

/** Modal element (DOM reference, not reset with state) */
let modalEl: HTMLElement | null = null;

/** Initialize the run query modal */
export function initRunQuery(): void {
  createModalElement();
}

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
  state.currentDurationMs = 0;
  state.queryStartTime = null;

  try {
    const data = await api.get<QueryHistoryResponse>("/api/query-history?page=1&per_page=1");
    if (data.entries.length > 0) {
      state.queryText = data.entries[0]?.query ?? "";
    }
  } catch {
    // Ignore errors, just start with empty query
  }

  modalEl.removeAttribute("hidden");
  document.addEventListener("keydown", handleKeydown);
  renderModal();

  setTimeout(() => {
    const textarea = document.getElementById("query-input") as HTMLTextAreaElement;
    if (textarea) {
      textarea.focus();
      textarea.setSelectionRange(textarea.value.length, textarea.value.length);
    }
  }, 50);
}

/** Close the modal, aborting any in-flight query */
export function closeRunQuery(): void {
  if (!modalEl) return;

  if (state.isExecuting) {
    abortCurrentForegroundQuery();
  }

  cleanup();
  document.removeEventListener("keydown", handleKeydown);
  modalEl.setAttribute("hidden", "");
  resetState();
}

function getDocsUrl(): string {
  return "https://neo4j.com/docs/cypher-manual/current/";
}

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
            <span id="query-timer" class="text-gray-500">${formatDuration(state.currentDurationMs)}</span>
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

/** Execute the query using the shared utility (handles Tauri sync + HTTP async transparently) */
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

  // Update only the timer text every 100ms — no full re-render needed
  state.durationInterval = setInterval(() => {
    if (state.queryStartTime !== null) {
      state.currentDurationMs = Date.now() - state.queryStartTime;
      const timerEl = document.getElementById("query-timer");
      if (timerEl) {
        timerEl.textContent = formatDuration(state.currentDurationMs);
      }
    }
  }, 100);

  try {
    const result = await sharedExecuteQuery(query, { extractGraph: true });
    cleanup();
    if (result.graph && result.graph.nodes.length > 0) {
      closeRunQuery();
      loadGraphData(result.graph as unknown as RawADGraph);
    } else {
      state.infoMessage = `Query returned ${result.resultCount ?? 0} row${result.resultCount === 1 ? "" : "s"}`;
      renderModal();
    }
  } catch (err) {
    cleanup();
    // Only update UI if modal is still open (abort-on-close should be silent)
    if (modalEl && !modalEl.hasAttribute("hidden")) {
      if (err instanceof QueryAbortedError) {
        state.infoMessage = "Query was aborted";
      } else {
        state.errorMessage = getQueryErrorMessage(err);
      }
      renderModal();
    }
  }
}

function cleanup(): void {
  state.isExecuting = false;
  if (state.durationInterval) {
    clearInterval(state.durationInterval);
    state.durationInterval = null;
  }
}

function handleModalClick(e: Event): void {
  const target = e.target as HTMLElement;

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
      abortCurrentForegroundQuery();
      break;
  }
}

function handleKeydown(e: KeyboardEvent): void {
  if (!modalEl || modalEl.hasAttribute("hidden")) return;

  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
    e.preventDefault();
    if (!state.isExecuting) {
      executeQuery();
    }
  }
}
