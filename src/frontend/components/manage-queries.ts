/**
 * Manage Queries Component
 *
 * Modal for managing custom queries stored in XDG_DATA_HOME/admapper/customqueries.json.
 * Provides JSON editor with schema validation.
 */

import { escapeHtml } from "../utils/html";
import type { Query, QueryCategory } from "./queries";

const STORAGE_KEY = "admapper_custom_queries";

/** Custom queries schema for validation */
interface CustomQueriesFile {
  version: number;
  categories: QueryCategory[];
}

/** Default empty queries file */
const DEFAULT_QUERIES: CustomQueriesFile = {
  version: 1,
  categories: [],
};

/** Example queries for new users */
const EXAMPLE_QUERIES: CustomQueriesFile = {
  version: 1,
  categories: [
    {
      id: "my-queries",
      name: "My Custom Queries",
      expanded: true,
      queries: [
        {
          id: "example-1",
          name: "All Users",
          description: "Find all user objects",
          query: "MATCH (u:User) RETURN u",
        },
        {
          id: "example-2",
          name: "Admin Groups",
          description: "Find groups with 'admin' in the name",
          query: "MATCH (g:Group) WHERE g.label CONTAINS 'admin' RETURN g",
        },
      ],
    },
  ],
};

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Current JSON content being edited */
let jsonContent = "";

/** Validation error message */
let validationError = "";

/** Is saving */
let isSaving = false;

/** Initialize the manage queries component */
export function initManageQueries(): void {
  createModalElement();
  document.addEventListener("keydown", handleKeydown);
}

/** Create the modal element */
function createModalElement(): void {
  const modal = document.createElement("div");
  modal.id = "manage-queries-modal";
  modal.className = "modal-overlay";
  modal.setAttribute("hidden", "");

  modal.innerHTML = `
    <div class="modal-content modal-lg">
      <div class="modal-header">
        <h2 class="modal-title">Manage Custom Queries</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body" id="manage-queries-body">
        <!-- Content rendered dynamically -->
      </div>
      <div class="modal-footer" id="manage-queries-footer">
        <!-- Footer rendered dynamically -->
      </div>
    </div>
  `;

  modal.addEventListener("click", handleModalClick);
  document.body.appendChild(modal);
  modalEl = modal;
}

/** Open the manage queries modal */
export async function openManageQueries(): Promise<void> {
  if (!modalEl) return;

  validationError = "";
  isSaving = false;

  // Load current queries
  const queries = await loadCustomQueries();
  jsonContent = JSON.stringify(queries, null, 2);

  modalEl.removeAttribute("hidden");
  renderModal();

  // Focus the textarea after render
  setTimeout(() => {
    const textarea = document.getElementById("queries-json-input") as HTMLTextAreaElement;
    if (textarea) {
      textarea.focus();
    }
  }, 50);
}

/** Close the modal */
export function closeManageQueries(): void {
  if (!modalEl) return;
  modalEl.setAttribute("hidden", "");
}

/** Load custom queries from storage */
export async function loadCustomQueries(): Promise<CustomQueriesFile> {
  // Try Tauri storage first
  if ("__TAURI__" in window) {
    try {
      return await loadFromTauriStorage();
    } catch {
      // Fall back to localStorage
    }
  }

  return loadFromLocalStorage();
}

/** Get custom categories for the query browser */
export async function getCustomCategories(): Promise<QueryCategory[]> {
  const queries = await loadCustomQueries();
  return queries.categories;
}

/** Load from localStorage */
function loadFromLocalStorage(): CustomQueriesFile {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (!stored) return DEFAULT_QUERIES;
    const parsed = JSON.parse(stored);
    if (!validateSchema(parsed)) {
      console.warn("Invalid custom queries schema in localStorage");
      return DEFAULT_QUERIES;
    }
    return parsed;
  } catch {
    return DEFAULT_QUERIES;
  }
}

/** Save to localStorage */
function saveToLocalStorage(queries: CustomQueriesFile): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(queries));
}

/** Load from Tauri storage (XDG_DATA_HOME/admapper/customqueries.json) */
async function loadFromTauriStorage(): Promise<CustomQueriesFile> {
  // @ts-expect-error Tauri global
  const { readTextFile, BaseDirectory } = await window.__TAURI__.fs;

  try {
    const content = await readTextFile("customqueries.json", { dir: BaseDirectory.AppData });
    const parsed = JSON.parse(content);
    if (!validateSchema(parsed)) {
      console.warn("Invalid custom queries schema in Tauri storage");
      return DEFAULT_QUERIES;
    }
    return parsed;
  } catch {
    return DEFAULT_QUERIES;
  }
}

/** Save to Tauri storage */
async function saveToTauriStorage(queries: CustomQueriesFile): Promise<void> {
  // @ts-expect-error Tauri global
  const { writeTextFile, createDir, BaseDirectory } = await window.__TAURI__.fs;
  // @ts-expect-error Tauri global
  const { appDataDir } = await window.__TAURI__.path;

  // Ensure directory exists
  const appDir = await appDataDir();
  try {
    await createDir(appDir, { recursive: true });
  } catch {
    // Directory might already exist
  }

  await writeTextFile("customqueries.json", JSON.stringify(queries, null, 2), {
    dir: BaseDirectory.AppData,
  });
}

/** Validate the schema of a queries file */
function validateSchema(data: unknown): data is CustomQueriesFile {
  if (!data || typeof data !== "object") return false;
  const obj = data as Record<string, unknown>;

  if (typeof obj.version !== "number") return false;
  if (!Array.isArray(obj.categories)) return false;

  // Validate each category
  for (const cat of obj.categories) {
    if (!validateCategory(cat)) return false;
  }

  return true;
}

/** Validate a query category */
function validateCategory(cat: unknown): cat is QueryCategory {
  if (!cat || typeof cat !== "object") return false;
  const obj = cat as Record<string, unknown>;

  if (typeof obj.id !== "string" || !obj.id) return false;
  if (typeof obj.name !== "string" || !obj.name) return false;

  // Optional queries array
  if (obj.queries !== undefined) {
    if (!Array.isArray(obj.queries)) return false;
    for (const query of obj.queries) {
      if (!validateQuery(query)) return false;
    }
  }

  // Optional subcategories array
  if (obj.subcategories !== undefined) {
    if (!Array.isArray(obj.subcategories)) return false;
    for (const subcat of obj.subcategories) {
      if (!validateCategory(subcat)) return false;
    }
  }

  return true;
}

/** Validate a query */
function validateQuery(query: unknown): query is Query {
  if (!query || typeof query !== "object") return false;
  const obj = query as Record<string, unknown>;

  if (typeof obj.id !== "string" || !obj.id) return false;
  if (typeof obj.name !== "string" || !obj.name) return false;
  if (typeof obj.query !== "string" || !obj.query) return false;

  return true;
}

/** Render the modal content */
function renderModal(): void {
  const body = document.getElementById("manage-queries-body");
  const footer = document.getElementById("manage-queries-footer");
  if (!body || !footer) return;

  body.innerHTML = `
    <div class="manage-queries-content">
      <div class="queries-help">
        <p>Edit your custom queries below. The JSON must follow this schema:</p>
        <pre class="schema-example">{
  "version": 1,
  "categories": [
    {
      "id": "unique-id",
      "name": "Category Name",
      "queries": [
        {
          "id": "query-id",
          "name": "Query Name",
          "description": "Optional description",
          "query": "MATCH (n) RETURN n"
        }
      ]
    }
  ]
}</pre>
      </div>

      <div class="form-group">
        <label class="form-label" for="queries-json-input">Custom Queries JSON</label>
        <textarea
          id="queries-json-input"
          class="form-textarea json-textarea"
          rows="16"
          spellcheck="false"
        >${escapeHtml(jsonContent)}</textarea>
      </div>

      ${
        validationError
          ? `
        <div class="query-error">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="error-icon">
            <circle cx="12" cy="12" r="10"/>
            <path d="M12 8v4m0 4h.01"/>
          </svg>
          <span>${escapeHtml(validationError)}</span>
        </div>
      `
          : ""
      }
    </div>
  `;

  footer.innerHTML = `
    <button class="btn btn-secondary" data-action="load-example">Load Example</button>
    <div class="spacer"></div>
    <button class="btn btn-secondary" data-action="close">Cancel</button>
    <button class="btn btn-primary" data-action="save" ${isSaving ? "disabled" : ""}>
      ${isSaving ? '<span class="spinner-sm"></span> Saving...' : "Save"}
    </button>
  `;
}

/** Save the custom queries */
async function saveQueries(): Promise<void> {
  const textarea = document.getElementById("queries-json-input") as HTMLTextAreaElement;
  if (!textarea) return;

  const content = textarea.value.trim();
  jsonContent = content;

  // Validate JSON syntax
  let parsed: unknown;
  try {
    parsed = JSON.parse(content);
  } catch (err) {
    validationError = `Invalid JSON: ${err instanceof Error ? err.message : "Parse error"}`;
    renderModal();
    return;
  }

  // Validate schema
  if (!validateSchema(parsed)) {
    validationError = "Invalid schema. Please check that all required fields are present.";
    renderModal();
    return;
  }

  isSaving = true;
  validationError = "";
  renderModal();

  try {
    // Save to storage
    if ("__TAURI__" in window) {
      try {
        await saveToTauriStorage(parsed);
      } catch {
        // Fall back to localStorage
        saveToLocalStorage(parsed);
      }
    } else {
      saveToLocalStorage(parsed);
    }

    closeManageQueries();

    // Notify that queries have changed (could emit an event for the query browser to refresh)
    window.dispatchEvent(new CustomEvent("custom-queries-changed"));
  } catch (err) {
    isSaving = false;
    validationError = `Failed to save: ${err instanceof Error ? err.message : "Unknown error"}`;
    renderModal();
  }
}

/** Load example queries */
function loadExample(): void {
  jsonContent = JSON.stringify(EXAMPLE_QUERIES, null, 2);
  validationError = "";
  renderModal();

  // Update textarea
  const textarea = document.getElementById("queries-json-input") as HTMLTextAreaElement;
  if (textarea) {
    textarea.value = jsonContent;
  }
}

/** Handle clicks in the modal */
function handleModalClick(e: Event): void {
  const target = e.target as HTMLElement;

  // Close on backdrop click
  if (target.classList.contains("modal-overlay")) {
    closeManageQueries();
    return;
  }

  const actionEl = target.closest("[data-action]") as HTMLElement;
  if (!actionEl) return;

  const action = actionEl.getAttribute("data-action");

  switch (action) {
    case "close":
      closeManageQueries();
      break;

    case "save":
      saveQueries();
      break;

    case "load-example":
      loadExample();
      break;
  }
}

/** Handle keyboard shortcuts */
function handleKeydown(e: KeyboardEvent): void {
  if (!modalEl || modalEl.hasAttribute("hidden")) return;

  if (e.key === "Escape") {
    closeManageQueries();
  }

  // Ctrl+S to save
  if ((e.ctrlKey || e.metaKey) && e.key === "s") {
    e.preventDefault();
    if (!isSaving) {
      saveQueries();
    }
  }
}
