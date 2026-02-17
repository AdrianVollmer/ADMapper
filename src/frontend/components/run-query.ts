/**
 * Run Query Modal
 *
 * Provides a modal for entering and executing database queries.
 * Supports both Cypher (for KuzuDB, Neo4j, FalkorDB) and Datalog (for CozoDB).
 */

import { appState } from "../main";
import { escapeHtml } from "../utils/html";
import { api } from "../api/client";
import type { QueryHistoryResponse } from "../api/types";
import { executeQueryWithHistory, getQueryErrorMessage } from "../utils/query";
import { loadGraphData } from "./graph-view";
import type { RawADGraph } from "../graph/types";

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Current query text */
let queryText = "";

/** Is query executing */
let isExecuting = false;

/** Error message */
let errorMessage = "";

/** Info message (e.g., zero rows returned) */
let infoMessage = "";

/** Initialize the run query modal */
export function initRunQuery(): void {
  createModalElement();
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
  modalEl.setAttribute("hidden", "");
}

/** Determine if the current database uses Cypher or Datalog */
function getQueryLanguage(): "cypher" | "datalog" {
  const dbType = appState.databaseType?.toLowerCase() ?? "";
  if (dbType.includes("cozo")) {
    return "datalog";
  }
  return "cypher";
}

/** Get the documentation URL for the query language */
function getDocsUrl(): string {
  const lang = getQueryLanguage();
  if (lang === "datalog") {
    return "https://docs.cozodb.org/en/latest/";
  }
  return "https://neo4j.com/docs/cypher-manual/current/";
}

/** Render the modal content */
function renderModal(): void {
  const body = document.getElementById("run-query-body");
  const footer = document.getElementById("run-query-footer");
  if (!body || !footer) return;

  const lang = getQueryLanguage();
  const isDatalog = lang === "datalog";
  const docsUrl = getDocsUrl();

  body.innerHTML = `
    <div class="run-query-content">
      ${
        isDatalog
          ? `
        <div class="query-hint query-hint-warning">
          <svg class="hint-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
          </svg>
          <span>CozoDB uses <strong>Datalog</strong> query language. Cypher queries are not supported.</span>
        </div>
      `
          : ""
      }

      <div class="query-language-info">
        <span class="language-badge">${isDatalog ? "Datalog" : "Cypher"}</span>
        <a href="${docsUrl}" target="_blank" rel="noopener noreferrer" class="docs-link">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="docs-icon">
            <path d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"/>
          </svg>
          View ${isDatalog ? "Datalog" : "Cypher"} Documentation
        </a>
      </div>

      <div class="form-group">
        <label class="form-label" for="query-input">Query</label>
        <textarea
          id="query-input"
          class="form-textarea query-textarea"
          rows="12"
          placeholder="${isDatalog ? "?[name] := *user{name}" : "MATCH (n:User) RETURN n LIMIT 10"}"
          spellcheck="false"
        >${escapeHtml(queryText)}</textarea>
      </div>

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

  footer.innerHTML = `
    <button class="btn btn-secondary" data-action="close">Cancel</button>
    <button class="btn btn-primary" data-action="execute" ${isExecuting ? "disabled" : ""}>
      ${
        isExecuting
          ? '<span class="spinner-sm"></span> Executing...'
          : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="btn-icon">
          <path d="M5 3l14 9-14 9V3z"/>
        </svg>
        Execute`
      }
    </button>
  `;
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
  renderModal();

  try {
    const result = await executeQueryWithHistory("Custom Query", query, true);

    // Load the graph if we got one with nodes
    if (result.graph && result.graph.nodes.length > 0) {
      // Close modal on success
      closeRunQuery();
      // Cast the GraphData to RawADGraph - the server returns compatible types
      loadGraphData(result.graph as unknown as RawADGraph);
    } else {
      // Show inline message for zero/non-graph results
      isExecuting = false;
      infoMessage = `Query returned ${result.resultCount} row${result.resultCount === 1 ? "" : "s"}`;
      renderModal();
    }
  } catch (err) {
    isExecuting = false;
    errorMessage = getQueryErrorMessage(err);
    renderModal();
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
  }
}

/** Handle keyboard shortcuts */
function handleKeydown(e: KeyboardEvent): void {
  if (!modalEl || modalEl.hasAttribute("hidden")) return;

  if (e.key === "Escape") {
    closeRunQuery();
  }

  // Ctrl+Enter or Cmd+Enter to execute
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
    e.preventDefault();
    if (!isExecuting) {
      executeQuery();
    }
  }
}
