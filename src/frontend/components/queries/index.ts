/**
 * Queries Component
 *
 * Hierarchical query browser with built-in and custom queries.
 */

import { loadGraphData } from "../graph-view";
import { escapeHtml } from "../../utils/html";
import { executeQueryWithHistory, getQueryErrorMessage, QueryAbortedError } from "../../utils/query";
import { showSuccess, showError, showInfo } from "../../utils/notifications";
import type { RawADGraph } from "../../graph/types";
import type { Query, QueryCategory } from "./types";
import { BUILTIN_QUERIES } from "./builtin-queries";

// Re-export types for external use
export type { Query, QueryCategory } from "./types";

/** localStorage key for custom queries (must match manage-queries.ts) */
const STORAGE_KEY = "admapper_custom_queries";

/** Old localStorage key (hyphenated) — kept only for migration */
const OLD_STORAGE_KEY = "admapper-custom-queries";

/** Custom queries loaded from localStorage */
let customQueries: QueryCategory[] = [];

/** Current filter text */
let filterText = "";

/** DOM elements */
let queryTreeEl: HTMLElement | null = null;
let queryFilterInput: HTMLInputElement | null = null;

/** Track expanded state */
const expandedCategories = new Set<string>();

/** Initialize queries component */
export function initQueries(): void {
  queryTreeEl = document.getElementById("query-tree");
  queryFilterInput = document.getElementById("query-filter") as HTMLInputElement;

  if (queryFilterInput) {
    queryFilterInput.addEventListener("input", () => {
      filterText = queryFilterInput!.value.trim().toLowerCase();
      renderQueryTree();
    });
  }

  // Set initial expanded state
  for (const cat of BUILTIN_QUERIES) {
    if (cat.expanded) {
      expandedCategories.add(cat.id);
    }
  }

  // Load custom queries from localStorage
  loadCustomQueries();

  // Render the tree
  renderQueryTree();
}

/** Load custom queries from localStorage, migrating from the old key if needed */
function loadCustomQueries(): void {
  try {
    // Migrate data from the old hyphenated key to the canonical underscored key
    const oldData = localStorage.getItem(OLD_STORAGE_KEY);
    if (oldData) {
      // Only migrate if the new key has no data yet (avoid overwriting)
      if (!localStorage.getItem(STORAGE_KEY)) {
        localStorage.setItem(STORAGE_KEY, oldData);
      }
      localStorage.removeItem(OLD_STORAGE_KEY);
    }

    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      customQueries = JSON.parse(stored);
    }
  } catch {
    customQueries = [];
  }
}

/** Save custom queries to localStorage */
export function saveCustomQueries(): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(customQueries));
}

/** Add a custom query */
export function addCustomQuery(categoryId: string, query: Query): void {
  let category = customQueries.find((c) => c.id === categoryId);
  if (!category) {
    category = { id: categoryId, name: categoryId, queries: [] };
    customQueries.push(category);
  }
  if (!category.queries) {
    category.queries = [];
  }
  category.queries.push(query);
  saveCustomQueries();
  renderQueryTree();
}

/** Render the query tree */
function renderQueryTree(): void {
  if (!queryTreeEl) return;

  const allCategories = [...BUILTIN_QUERIES];
  if (customQueries.length > 0) {
    allCategories.push({
      id: "custom",
      name: "Custom Queries",
      subcategories: customQueries,
    });
  }

  const html = renderCategories(allCategories, 0);
  queryTreeEl.innerHTML = html || '<div class="text-sm text-gray-500 p-2">No queries match filter</div>';
}

/** Render categories recursively */
function renderCategories(categories: QueryCategory[], depth: number): string {
  let html = "";

  for (const category of categories) {
    const catHtml = renderCategory(category, depth);
    if (catHtml) {
      html += catHtml;
    }
  }

  return html;
}

/** Render a single category */
function renderCategory(category: QueryCategory, depth: number): string {
  const isExpanded = expandedCategories.has(category.id);

  // Filter queries
  const filteredQueries = (category.queries || []).filter(
    (q) => !filterText || q.name.toLowerCase().includes(filterText) || q.description?.toLowerCase().includes(filterText)
  );

  // Filter subcategories recursively
  const filteredSubcats = (category.subcategories || [])
    .map((sub) => {
      const subHtml = renderCategory(sub, depth + 1);
      return { sub, html: subHtml };
    })
    .filter((s) => s.html);

  // If nothing matches filter, skip this category
  if (filterText && filteredQueries.length === 0 && filteredSubcats.length === 0) {
    return "";
  }

  // When filtering, auto-expand categories with matches
  const shouldExpand = filterText ? true : isExpanded;

  const indent = depth * 12;
  let html = `
    <div class="query-category" data-category-id="${escapeHtml(category.id)}">
      <div
        class="query-category-header"
        style="padding-left: ${indent + 8}px"
        data-action="toggle-category"
        data-category-id="${escapeHtml(category.id)}"
      >
        <svg class="query-expand-icon ${shouldExpand ? "expanded" : ""}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M9 18l6-6-6-6"/>
        </svg>
        <span class="query-category-name">${escapeHtml(category.name)}</span>
        <span class="query-count">${countQueries(category)}</span>
      </div>
  `;

  if (shouldExpand) {
    html += `<div class="query-category-content">`;

    // Render queries
    for (const query of filteredQueries) {
      html += `
        <div
          class="query-item"
          style="padding-left: ${indent + 24}px"
          data-action="execute-sidebar-query"
          data-query-id="${escapeHtml(query.id)}"
          title="${escapeHtml(query.description || query.name)}"
        >
          <svg class="query-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M14.7 6.3a1 1 0 000 1.4l1.6 1.6a1 1 0 001.4 0l3.77-3.77a6 6 0 01-7.94 7.94l-6.91 6.91a2.12 2.12 0 01-3-3l6.91-6.91a6 6 0 017.94-7.94l-3.76 3.76z"/>
          </svg>
          <span class="query-name">${highlightMatch(query.name, filterText)}</span>
        </div>
      `;
    }

    // Render subcategories
    for (const { html: subHtml } of filteredSubcats) {
      html += subHtml;
    }

    html += `</div>`;
  }

  html += `</div>`;
  return html;
}

/** Count total queries in a category (including subcategories) */
function countQueries(category: QueryCategory): number {
  let count = category.queries?.length ?? 0;
  if (category.subcategories) {
    for (const sub of category.subcategories) {
      count += countQueries(sub);
    }
  }
  return count;
}

/** Highlight matching text */
function highlightMatch(text: string, filter: string): string {
  if (!filter) return escapeHtml(text);

  const escaped = escapeHtml(text);
  const lowerText = text.toLowerCase();
  const idx = lowerText.indexOf(filter);

  if (idx === -1) return escaped;

  const before = escapeHtml(text.slice(0, idx));
  const match = escapeHtml(text.slice(idx, idx + filter.length));
  const after = escapeHtml(text.slice(idx + filter.length));

  return `${before}<mark class="query-highlight">${match}</mark>${after}`;
}

/**
 * Handle clicks on the query tree.
 * Called from the central document click handler in main.ts.
 * Returns true if the click was handled.
 */
export function handleQueryTreeClicks(e: MouseEvent): boolean {
  const target = e.target as HTMLElement;

  // Toggle category
  const categoryHeader = target.closest('[data-action="toggle-category"]') as HTMLElement;
  if (categoryHeader) {
    const categoryId = categoryHeader.getAttribute("data-category-id");
    if (categoryId) {
      if (expandedCategories.has(categoryId)) {
        expandedCategories.delete(categoryId);
      } else {
        expandedCategories.add(categoryId);
      }
      renderQueryTree();
    }
    return true;
  }

  // Run query (use unique action name to avoid conflict with global action dispatcher)
  const queryItem = target.closest('[data-action="execute-sidebar-query"]') as HTMLElement;
  if (queryItem) {
    const queryId = queryItem.getAttribute("data-query-id");
    if (queryId) {
      runQuery(queryId);
    }
    return true;
  }

  return false;
}

/** Find a query by ID */
function findQuery(queryId: string, categories: QueryCategory[] = BUILTIN_QUERIES): Query | null {
  for (const category of categories) {
    if (category.queries) {
      const query = category.queries.find((q) => q.id === queryId);
      if (query) return query;
    }
    if (category.subcategories) {
      const found = findQuery(queryId, category.subcategories);
      if (found) return found;
    }
  }
  // Also check custom queries
  if (categories === BUILTIN_QUERIES && customQueries.length > 0) {
    return findQuery(queryId, customQueries);
  }
  return null;
}

/** Run a query */
async function runQuery(queryId: string): Promise<void> {
  const query = findQuery(queryId);
  if (!query) {
    console.warn(`Query not found: ${queryId}`);
    return;
  }

  try {
    const result = await executeQueryWithHistory(query.name, query.query, true);

    // Show results
    if (result.graph && result.graph.nodes.length > 0) {
      // Convert GraphData to RawADGraph format and load into renderer
      const rawGraph: RawADGraph = {
        nodes: result.graph.nodes.map((n) => ({
          id: n.id,
          name: n.name,
          type: n.type as RawADGraph["nodes"][0]["type"],
          properties: n.properties,
        })),
        relationships: result.graph.relationships.map((e) => ({
          source: e.source,
          target: e.target,
          type: e.type as RawADGraph["relationships"][0]["type"],
        })),
      };
      loadGraphData(rawGraph);
      showSuccess(
        `"${query.name}" returned ${result.graph.nodes.length} nodes and ${result.graph.relationships.length} relationships`
      );
    } else if (result.resultCount && result.resultCount > 0) {
      showSuccess(`"${query.name}" returned ${result.resultCount} rows`);
    } else {
      showInfo(`"${query.name}" returned no results`);
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

/** Import queries from JSON file */
export async function importQueries(file: File): Promise<void> {
  try {
    const text = await file.text();
    const imported = JSON.parse(text) as QueryCategory[];
    customQueries.push(...imported);
    saveCustomQueries();
    renderQueryTree();
  } catch (err) {
    console.error("Failed to import queries:", err);
    throw err;
  }
}

/** Export custom queries to JSON */
export function exportQueries(): string {
  return JSON.stringify(customQueries, null, 2);
}
