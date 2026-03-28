/**
 * Manage Queries Component
 *
 * Modal for managing custom queries stored in XDG_DATA_HOME/admapper/customqueries.json.
 * Provides tree-based UI with filtering and CRUD operations.
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

/** Form data for editing a query */
interface QueryFormData {
  id: string;
  name: string;
  description: string;
  query: string;
}

/** Form data for editing a category */
interface CategoryFormData {
  id: string;
  name: string;
}

/** View mode */
type ViewMode = "tree" | "edit-query" | "create-query" | "edit-category" | "create-category";

/** Context for editing (which category a new query belongs to) */
interface EditContext {
  categoryId: string;
}

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Current view mode */
let viewMode: ViewMode = "tree";

/** Categories data */
let categories: QueryCategory[] = [];

/** Filter text */
let filterText = "";

/** Track expanded categories */
const expandedCategories = new Set<string>();

/** Editing state */
let editingQuery: QueryFormData | null = null;
let editingCategory: CategoryFormData | null = null;
let editContext: EditContext | null = null;

/** Validation error message */
let validationError = "";

/** Is saving */
let isSaving = false;

/** Initialize the manage queries component */
export function initManageQueries(): void {
  createModalElement();
  // Non-Escape keyboard shortcuts (Ctrl+S, Enter) -- Escape is handled globally in main.ts
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
        <h2 class="modal-title" id="manage-queries-title">Manage Custom Queries</h2>
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

  viewMode = "tree";
  validationError = "";
  isSaving = false;
  filterText = "";
  editingQuery = null;
  editingCategory = null;
  editContext = null;

  // Load current queries
  const queries = await loadCustomQueries();
  categories = queries.categories;

  // Initialize expanded state
  expandedCategories.clear();
  for (const cat of categories) {
    if (cat.expanded) {
      expandedCategories.add(cat.id);
    }
  }

  modalEl.removeAttribute("hidden");
  renderModal();
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

/** Render the modal content based on view mode */
function renderModal(): void {
  const title = document.getElementById("manage-queries-title");
  if (title) {
    switch (viewMode) {
      case "tree":
        title.textContent = "Manage Custom Queries";
        break;
      case "edit-query":
        title.textContent = "Edit Query";
        break;
      case "create-query":
        title.textContent = "Create Query";
        break;
      case "edit-category":
        title.textContent = "Edit Category";
        break;
      case "create-category":
        title.textContent = "Create Category";
        break;
    }
  }

  switch (viewMode) {
    case "tree":
      renderTreeView();
      break;
    case "edit-query":
    case "create-query":
      renderEditQueryView();
      break;
    case "edit-category":
    case "create-category":
      renderEditCategoryView();
      break;
  }
}

/** Generate a unique ID */
function generateId(): string {
  return `q-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

/** Count total queries in categories */
function countTotalQueries(): number {
  let count = 0;
  for (const cat of categories) {
    count += countQueriesInCategory(cat);
  }
  return count;
}

/** Count queries in a category (recursive) */
function countQueriesInCategory(category: QueryCategory): number {
  let count = category.queries?.length ?? 0;
  if (category.subcategories) {
    for (const sub of category.subcategories) {
      count += countQueriesInCategory(sub);
    }
  }
  return count;
}

/** Get file location hint */
function getFileLocationHint(): string {
  if ("__TAURI__" in window) {
    // Desktop app
    const platform = navigator.platform.toLowerCase();
    if (platform.includes("win")) {
      return "%APPDATA%\\admapper\\customqueries.json";
    } else if (platform.includes("mac")) {
      return "~/Library/Application Support/admapper/customqueries.json";
    } else {
      return "~/.local/share/admapper/customqueries.json";
    }
  }
  return "Stored in browser local storage";
}

/** Filter categories and queries by filter text */
function filterCategories(cats: QueryCategory[]): QueryCategory[] {
  if (!filterText) return cats;

  const result: QueryCategory[] = [];
  const lowerFilter = filterText.toLowerCase();

  for (const cat of cats) {
    const filteredQueries = (cat.queries ?? []).filter(
      (q) => q.name.toLowerCase().includes(lowerFilter) || q.description?.toLowerCase().includes(lowerFilter)
    );

    const filteredSubcats = filterCategories(cat.subcategories ?? []);

    if (filteredQueries.length > 0 || filteredSubcats.length > 0 || cat.name.toLowerCase().includes(lowerFilter)) {
      const filtered: QueryCategory = {
        ...cat,
        queries: filteredQueries,
      };
      if (filteredSubcats.length > 0) {
        filtered.subcategories = filteredSubcats;
      }
      result.push(filtered);
    }
  }

  return result;
}

/** Render the tree view */
function renderTreeView(): void {
  const body = document.getElementById("manage-queries-body");
  const footer = document.getElementById("manage-queries-footer");
  if (!body || !footer) return;

  const totalQueries = countTotalQueries();
  const filteredCats = filterCategories(categories);

  body.innerHTML = `
    <div class="manage-queries-content">
      <div class="query-manager-toolbar">
        <div class="search-box query-manager-search">
          <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="11" cy="11" r="8"/>
            <path d="M21 21l-4.35-4.35"/>
          </svg>
          <input
            type="text"
            id="query-manager-filter"
            class="search-input"
            placeholder="Filter queries..."
            value="${escapeHtml(filterText)}"
          />
        </div>
        <button class="btn btn-secondary btn-sm" data-action="add-category">
          <svg class="btn-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 5v14M5 12h14"/>
          </svg>
          Category
        </button>
        <button class="btn btn-secondary btn-sm" data-action="add-query-root" ${categories.length === 0 ? "disabled" : ""}>
          <svg class="btn-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 5v14M5 12h14"/>
          </svg>
          Query
        </button>
        <span class="query-manager-count">${totalQueries} ${totalQueries === 1 ? "query" : "queries"}</span>
      </div>

      <div class="query-manager-tree">
        ${
          filteredCats.length > 0
            ? renderCategoriesHtml(filteredCats, 0)
            : categories.length === 0
              ? `<div class="query-manager-empty">
              <p>No custom queries yet.</p>
              <p>Click "Load Example" to get started or add your own categories and queries.</p>
            </div>`
              : `<div class="query-manager-empty">
              <p>No queries match "${escapeHtml(filterText)}"</p>
            </div>`
        }
      </div>

      <div class="query-manager-hint">
        <svg class="hint-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"/>
        </svg>
        <span>${escapeHtml(getFileLocationHint())}</span>
      </div>
    </div>
  `;

  footer.innerHTML = `
    <button class="btn btn-secondary" data-action="load-example">Load Example</button>
    <div class="spacer"></div>
    <button class="btn btn-secondary" data-action="close">Cancel</button>
    <button class="btn btn-primary" data-action="save-all" ${isSaving ? "disabled" : ""}>
      ${isSaving ? '<span class="spinner-sm"></span> Saving...' : "Save All"}
    </button>
  `;

  // Attach filter input handler
  const filterInput = document.getElementById("query-manager-filter") as HTMLInputElement;
  if (filterInput) {
    filterInput.addEventListener("input", () => {
      filterText = filterInput.value.trim();
      renderModal();
    });
    filterInput.focus();
  }
}

/** Render categories HTML recursively */
function renderCategoriesHtml(cats: QueryCategory[], depth: number): string {
  let html = "";
  for (const cat of cats) {
    html += renderCategoryHtml(cat, depth);
  }
  return html;
}

/** Render a single category HTML */
function renderCategoryHtml(category: QueryCategory, depth: number): string {
  const isExpanded = filterText ? true : expandedCategories.has(category.id);
  const queryCount = countQueriesInCategory(category);
  const indent = depth * 16;

  let html = `
    <div class="query-manager-category" data-category-id="${escapeHtml(category.id)}">
      <div class="query-manager-category-header" style="padding-left: ${indent + 8}px">
        <button
          class="query-manager-expand"
          data-action="toggle-category"
          data-category-id="${escapeHtml(category.id)}"
        >
          <svg class="query-expand-icon ${isExpanded ? "expanded" : ""}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M9 18l6-6-6-6"/>
          </svg>
        </button>
        <span class="query-manager-category-name">${escapeHtml(category.name)}</span>
        <span class="query-count">${queryCount}</span>
        <div class="query-manager-actions">
          <button class="btn-icon-sm" data-action="edit-category" data-category-id="${escapeHtml(category.id)}" title="Edit category">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/>
              <path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/>
            </svg>
          </button>
          <button class="btn-icon-sm btn-icon-danger" data-action="delete-category" data-category-id="${escapeHtml(category.id)}" title="Delete category">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/>
            </svg>
          </button>
        </div>
      </div>
  `;

  if (isExpanded) {
    html += `<div class="query-manager-category-content">`;

    // Render queries
    for (const query of category.queries ?? []) {
      html += renderQueryHtml(query, category.id, depth);
    }

    // Render subcategories
    if (category.subcategories) {
      html += renderCategoriesHtml(category.subcategories, depth + 1);
    }

    // Add query button at category level
    html += `
      <button
        class="query-manager-add-query"
        style="padding-left: ${indent + 28}px"
        data-action="add-query"
        data-category-id="${escapeHtml(category.id)}"
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M12 5v14M5 12h14"/>
        </svg>
        Add query
      </button>
    `;

    html += `</div>`;
  }

  html += `</div>`;
  return html;
}

/** Render a single query HTML */
function renderQueryHtml(query: Query, categoryId: string, depth: number): string {
  const indent = depth * 16;

  return `
    <div class="query-manager-query" style="padding-left: ${indent + 28}px" data-query-id="${escapeHtml(query.id)}" data-category-id="${escapeHtml(categoryId)}">
      <svg class="query-manager-query-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0 001.4 0l3.77-3.77a6 6 0 01-7.94 7.94l-6.91 6.91a2.12 2.12 0 01-3-3l6.91-6.91a6 6 0 017.94-7.94l-3.76 3.76z"/>
      </svg>
      <div class="query-manager-query-info">
        <span class="query-manager-query-name">${escapeHtml(query.name)}</span>
        ${query.description ? `<span class="query-manager-query-desc">${escapeHtml(query.description)}</span>` : ""}
      </div>
      <div class="query-manager-actions">
        <button class="btn-icon-sm" data-action="edit-query" data-query-id="${escapeHtml(query.id)}" data-category-id="${escapeHtml(categoryId)}" title="Edit query">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/>
            <path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/>
          </svg>
        </button>
        <button class="btn-icon-sm" data-action="duplicate-query" data-query-id="${escapeHtml(query.id)}" data-category-id="${escapeHtml(categoryId)}" title="Duplicate query">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
            <path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/>
          </svg>
        </button>
        <button class="btn-icon-sm btn-icon-danger" data-action="delete-query" data-query-id="${escapeHtml(query.id)}" data-category-id="${escapeHtml(categoryId)}" title="Delete query">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"/>
          </svg>
        </button>
      </div>
    </div>
  `;
}

/** Render the edit/create query view */
function renderEditQueryView(): void {
  const body = document.getElementById("manage-queries-body");
  const footer = document.getElementById("manage-queries-footer");
  if (!body || !footer) return;

  const data = editingQuery || { id: "", name: "", description: "", query: "" };

  body.innerHTML = `
    <div class="query-history-edit">
      <div class="form-group">
        <label class="form-label" for="query-name-input">Name *</label>
        <input
          type="text"
          id="query-name-input"
          class="form-input"
          placeholder="e.g., Admin Groups"
          value="${escapeHtml(data.name)}"
        />
      </div>

      <div class="form-group">
        <label class="form-label" for="query-desc-input">Description</label>
        <input
          type="text"
          id="query-desc-input"
          class="form-input"
          placeholder="e.g., Find groups with 'admin' in the name"
          value="${escapeHtml(data.description)}"
        />
      </div>

      <div class="form-group">
        <label class="form-label" for="query-cypher-input">Query (Cypher) *</label>
        <textarea
          id="query-cypher-input"
          class="form-textarea query-textarea"
          placeholder="MATCH (n) RETURN n"
          rows="8"
        >${escapeHtml(data.query)}</textarea>
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
    <button class="btn btn-secondary" data-action="cancel-edit">Cancel</button>
    <button class="btn btn-primary" data-action="save-query">Save</button>
  `;

  // Focus the name input
  setTimeout(() => {
    const nameInput = document.getElementById("query-name-input") as HTMLInputElement;
    if (nameInput) {
      nameInput.focus();
      nameInput.select();
    }
  }, 50);
}

/** Render the edit/create category view */
function renderEditCategoryView(): void {
  const body = document.getElementById("manage-queries-body");
  const footer = document.getElementById("manage-queries-footer");
  if (!body || !footer) return;

  const data = editingCategory || { id: "", name: "" };

  body.innerHTML = `
    <div class="query-history-edit">
      <div class="form-group">
        <label class="form-label" for="category-name-input">Name *</label>
        <input
          type="text"
          id="category-name-input"
          class="form-input"
          placeholder="e.g., Kerberos Queries"
          value="${escapeHtml(data.name)}"
        />
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
    <button class="btn btn-secondary" data-action="cancel-edit">Cancel</button>
    <button class="btn btn-primary" data-action="save-category">Save</button>
  `;

  // Focus the name input
  setTimeout(() => {
    const nameInput = document.getElementById("category-name-input") as HTMLInputElement;
    if (nameInput) {
      nameInput.focus();
      nameInput.select();
    }
  }, 50);
}

/** Find a category by ID */
function findCategory(categoryId: string, cats: QueryCategory[] = categories): QueryCategory | null {
  for (const cat of cats) {
    if (cat.id === categoryId) return cat;
    if (cat.subcategories) {
      const found = findCategory(categoryId, cat.subcategories);
      if (found) return found;
    }
  }
  return null;
}

/** Find a query by ID within a category */
function findQueryInCategory(queryId: string, category: QueryCategory): Query | null {
  return category.queries?.find((q) => q.id === queryId) ?? null;
}

/** Handle creating a new query */
function handleCreateQuery(categoryId: string): void {
  editingQuery = {
    id: generateId(),
    name: "",
    description: "",
    query: "",
  };
  editContext = { categoryId };
  validationError = "";
  viewMode = "create-query";
  renderModal();
}

/** Handle editing an existing query */
function handleEditQuery(queryId: string, categoryId: string): void {
  const category = findCategory(categoryId);
  if (!category) return;

  const query = findQueryInCategory(queryId, category);
  if (!query) return;

  editingQuery = {
    id: query.id,
    name: query.name,
    description: query.description || "",
    query: query.query,
  };
  editContext = { categoryId };
  validationError = "";
  viewMode = "edit-query";
  renderModal();
}

/** Handle duplicating a query */
function handleDuplicateQuery(queryId: string, categoryId: string): void {
  const category = findCategory(categoryId);
  if (!category) return;

  const query = findQueryInCategory(queryId, category);
  if (!query) return;

  editingQuery = {
    id: generateId(),
    name: `${query.name} (copy)`,
    description: query.description || "",
    query: query.query,
  };
  editContext = { categoryId };
  validationError = "";
  viewMode = "create-query";
  renderModal();
}

/** Handle deleting a query */
function handleDeleteQuery(queryId: string, categoryId: string): void {
  const category = findCategory(categoryId);
  if (!category || !category.queries) return;

  const queryIndex = category.queries.findIndex((q) => q.id === queryId);
  if (queryIndex === -1) return;

  category.queries.splice(queryIndex, 1);
  renderModal();
}

/** Handle creating a new category */
function handleCreateCategory(): void {
  editingCategory = {
    id: generateId(),
    name: "",
  };
  validationError = "";
  viewMode = "create-category";
  renderModal();
}

/** Handle editing an existing category */
function handleEditCategory(categoryId: string): void {
  const category = findCategory(categoryId);
  if (!category) return;

  editingCategory = {
    id: category.id,
    name: category.name,
  };
  validationError = "";
  viewMode = "edit-category";
  renderModal();
}

/** Handle deleting a category */
function handleDeleteCategory(categoryId: string): void {
  const category = findCategory(categoryId);
  if (!category) return;

  const queryCount = countQueriesInCategory(category);
  if (queryCount > 0) {
    if (
      !confirm(`Delete category "${category.name}" and its ${queryCount} ${queryCount === 1 ? "query" : "queries"}?`)
    ) {
      return;
    }
  }

  // Remove from categories array
  const index = categories.findIndex((c) => c.id === categoryId);
  if (index !== -1) {
    categories.splice(index, 1);
    renderModal();
  }
}

/** Save the query being edited */
function saveQuery(): void {
  const nameInput = document.getElementById("query-name-input") as HTMLInputElement;
  const descInput = document.getElementById("query-desc-input") as HTMLInputElement;
  const queryInput = document.getElementById("query-cypher-input") as HTMLTextAreaElement;

  if (!nameInput || !queryInput) return;

  const name = nameInput.value.trim();
  const description = descInput?.value.trim() || "";
  const query = queryInput.value.trim();

  // Validate
  if (!name) {
    validationError = "Name is required";
    renderModal();
    return;
  }

  if (!query) {
    validationError = "Query is required";
    renderModal();
    return;
  }

  if (!editingQuery || !editContext) return;

  const category = findCategory(editContext.categoryId);
  if (!category) return;

  if (!category.queries) {
    category.queries = [];
  }

  if (viewMode === "create-query") {
    // Add new query
    const newQuery: Query = {
      id: editingQuery.id,
      name,
      query,
    };
    if (description) {
      newQuery.description = description;
    }
    category.queries.push(newQuery);
  } else {
    // Update existing query
    const existingQuery = findQueryInCategory(editingQuery.id, category);
    if (existingQuery) {
      existingQuery.name = name;
      if (description) {
        existingQuery.description = description;
      } else {
        delete existingQuery.description;
      }
      existingQuery.query = query;
    }
  }

  editingQuery = null;
  editContext = null;
  validationError = "";
  viewMode = "tree";
  renderModal();
}

/** Save the category being edited */
function saveCategory(): void {
  const nameInput = document.getElementById("category-name-input") as HTMLInputElement;
  if (!nameInput) return;

  const name = nameInput.value.trim();

  // Validate
  if (!name) {
    validationError = "Name is required";
    renderModal();
    return;
  }

  if (!editingCategory) return;

  if (viewMode === "create-category") {
    // Add new category
    categories.push({
      id: editingCategory.id,
      name,
      queries: [],
      expanded: true,
    });
    expandedCategories.add(editingCategory.id);
  } else {
    // Update existing category
    const category = findCategory(editingCategory.id);
    if (category) {
      category.name = name;
    }
  }

  editingCategory = null;
  validationError = "";
  viewMode = "tree";
  renderModal();
}

/** Save all changes to storage */
async function saveAllChanges(): Promise<void> {
  isSaving = true;
  validationError = "";
  renderModal();

  const queriesFile: CustomQueriesFile = {
    version: 1,
    categories,
  };

  try {
    if ("__TAURI__" in window) {
      try {
        await saveToTauriStorage(queriesFile);
      } catch {
        // Fall back to localStorage
        saveToLocalStorage(queriesFile);
      }
    } else {
      saveToLocalStorage(queriesFile);
    }

    closeManageQueries();

    // Notify that queries have changed
    window.dispatchEvent(new CustomEvent("custom-queries-changed"));
  } catch (err) {
    isSaving = false;
    validationError = `Failed to save: ${err instanceof Error ? err.message : "Unknown error"}`;
    renderModal();
  }
}

/** Load example queries */
function loadExample(): void {
  categories = JSON.parse(JSON.stringify(EXAMPLE_QUERIES.categories));
  expandedCategories.clear();
  for (const cat of categories) {
    if (cat.expanded) {
      expandedCategories.add(cat.id);
    }
  }
  renderModal();
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
  const categoryId = actionEl.getAttribute("data-category-id");
  const queryId = actionEl.getAttribute("data-query-id");

  switch (action) {
    case "close":
      closeManageQueries();
      break;

    case "save-all":
      saveAllChanges();
      break;

    case "load-example":
      loadExample();
      break;

    case "toggle-category":
      if (categoryId) {
        if (expandedCategories.has(categoryId)) {
          expandedCategories.delete(categoryId);
        } else {
          expandedCategories.add(categoryId);
        }
        renderModal();
      }
      break;

    case "add-category":
      handleCreateCategory();
      break;

    case "edit-category":
      if (categoryId) {
        handleEditCategory(categoryId);
      }
      break;

    case "delete-category":
      if (categoryId) {
        handleDeleteCategory(categoryId);
      }
      break;

    case "add-query":
      if (categoryId) {
        handleCreateQuery(categoryId);
      }
      break;

    case "add-query-root": {
      // Add to first category if exists
      const firstCategory = categories[0];
      if (firstCategory) {
        handleCreateQuery(firstCategory.id);
      }
      break;
    }

    case "edit-query":
      if (queryId && categoryId) {
        handleEditQuery(queryId, categoryId);
      }
      break;

    case "duplicate-query":
      if (queryId && categoryId) {
        handleDuplicateQuery(queryId, categoryId);
      }
      break;

    case "delete-query":
      if (queryId && categoryId) {
        handleDeleteQuery(queryId, categoryId);
      }
      break;

    case "save-query":
      saveQuery();
      break;

    case "save-category":
      saveCategory();
      break;

    case "cancel-edit":
      editingQuery = null;
      editingCategory = null;
      editContext = null;
      validationError = "";
      viewMode = "tree";
      renderModal();
      break;
  }
}

/** Handle Escape key for this modal (called by global Escape handler) */
export function handleEscapeKey(): void {
  if (!modalEl || modalEl.hasAttribute("hidden")) return;

  if (viewMode !== "tree") {
    // Go back to tree view
    editingQuery = null;
    editingCategory = null;
    editContext = null;
    validationError = "";
    viewMode = "tree";
    renderModal();
  } else {
    closeManageQueries();
  }
}

/** Handle keyboard shortcuts (non-Escape) */
function handleKeydown(e: KeyboardEvent): void {
  if (!modalEl || modalEl.hasAttribute("hidden")) return;

  // Ctrl+S to save
  if ((e.ctrlKey || e.metaKey) && e.key === "s") {
    e.preventDefault();
    if (viewMode === "tree" && !isSaving) {
      saveAllChanges();
    } else if (viewMode === "edit-query" || viewMode === "create-query") {
      saveQuery();
    } else if (viewMode === "edit-category" || viewMode === "create-category") {
      saveCategory();
    }
  }

  // Enter to save in edit views
  if (e.key === "Enter" && !e.shiftKey) {
    if (viewMode === "edit-category" || viewMode === "create-category") {
      e.preventDefault();
      saveCategory();
    }
  }
}
