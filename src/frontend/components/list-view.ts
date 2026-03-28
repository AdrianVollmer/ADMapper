/**
 * List View Component
 *
 * Modal for viewing currently visible graph nodes in a sortable,
 * filterable table. Supports copying and downloading as CSV.
 */

import { escapeHtml } from "../utils/html";
import { getRenderer } from "./graph-view";
import { showInfo } from "../utils/notifications";
import { createModal, type ModalHandle } from "../utils/modal";
import type { ADNodeAttributes, ADNodeType } from "../graph/types";

/** Node data for the list */
interface NodeListItem {
  id: string;
  label: string;
  type: ADNodeType;
  properties: Record<string, unknown>;
}

/** Sort configuration */
interface SortConfig {
  column: "id" | "label" | "type";
  direction: "asc" | "desc";
}

/** Modal state */
let nodes: NodeListItem[] = [];
let filteredNodes: NodeListItem[] = [];
let filterText = "";
let sortConfig: SortConfig = { column: "label", direction: "asc" };

/** Modal handle */
let modal: ModalHandle | null = null;

/** Initialize list view (call once at startup) */
export function initListView(): void {
  createListViewModal();
}

/** Create the modal element using the shared modal utility */
function createListViewModal(): void {
  modal = createModal({
    id: "list-view-modal",
    title: "List View",
    sizeClass: "modal-xl",
    onClick(action) {
      switch (action) {
        case "close":
          closeListView();
          break;
        case "copy-csv":
          copyCSV();
          break;
        case "download-csv":
          downloadCSV();
          break;
      }
    },
  });

  modal.body.id = "list-view-body";
  modal.footer.id = "list-view-footer";

  // Sort headers use data-sort, not data-action, so add a direct listener
  modal.overlay.addEventListener("click", (e: Event) => {
    const target = e.target as HTMLElement;
    const sortHeader = target.closest("[data-sort]") as HTMLElement;
    if (sortHeader) {
      const column = sortHeader.getAttribute("data-sort") as SortConfig["column"];
      if (column) {
        toggleSort(column);
      }
    }
  });
}

/** Open the list view modal */
export function openListView(): void {
  if (!modal) return;

  const renderer = getRenderer();
  if (!renderer) {
    showInfo("No graph loaded. Run a query first to view nodes.");
    return;
  }

  // Get nodes from the graph
  const graph = renderer.sigma.getGraph();
  nodes = [];

  graph.forEachNode((nodeId: string, attrs: ADNodeAttributes) => {
    nodes.push({
      id: nodeId,
      label: attrs.label,
      type: attrs.nodeType,
      properties: attrs.properties ?? {},
    });
  });

  if (nodes.length === 0) {
    showInfo("No nodes in the current graph.");
    return;
  }

  filterText = "";
  sortConfig = { column: "label", direction: "asc" };
  applyFilterAndSort();

  modal.open();
  renderModal();

  // Focus the filter input
  setTimeout(() => {
    const input = document.getElementById("list-view-filter") as HTMLInputElement;
    input?.focus();
  }, 0);
}

/** Close the modal */
export function closeListView(): void {
  modal?.close();
}

/** Apply filter and sort to nodes */
function applyFilterAndSort(): void {
  // Filter
  const search = filterText.toLowerCase();
  filteredNodes = nodes.filter(
    (node) =>
      node.id.toLowerCase().includes(search) ||
      node.label.toLowerCase().includes(search) ||
      node.type.toLowerCase().includes(search)
  );

  // Sort
  filteredNodes.sort((a, b) => {
    const aVal = a[sortConfig.column];
    const bVal = b[sortConfig.column];
    const cmp = String(aVal).localeCompare(String(bVal));
    return sortConfig.direction === "asc" ? cmp : -cmp;
  });
}

/** Toggle sort on a column */
function toggleSort(column: SortConfig["column"]): void {
  if (sortConfig.column === column) {
    sortConfig.direction = sortConfig.direction === "asc" ? "desc" : "asc";
  } else {
    sortConfig.column = column;
    sortConfig.direction = "asc";
  }
  applyFilterAndSort();
  renderModal();
}

/** Get sort indicator for a column */
function getSortIndicator(column: SortConfig["column"]): string {
  if (sortConfig.column !== column) return "";
  return sortConfig.direction === "asc" ? " ▲" : " ▼";
}

/** Render the modal content */
function renderModal(): void {
  const body = document.getElementById("list-view-body");
  const footer = document.getElementById("list-view-footer");
  if (!body || !footer) return;

  body.innerHTML = `
    <div class="list-view-toolbar">
      <div class="search-box flex-1">
        <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="11" cy="11" r="8" />
          <path d="M21 21l-4.35-4.35" />
        </svg>
        <input
          type="text"
          placeholder="Filter nodes..."
          class="search-input"
          id="list-view-filter"
          value="${escapeHtml(filterText)}"
          aria-label="Filter nodes"
        />
      </div>
      <span class="list-view-count">${filteredNodes.length} of ${nodes.length} nodes</span>
    </div>

    <div class="list-view-table-container">
      <table class="list-view-table">
        <thead>
          <tr>
            <th class="sortable" data-sort="id">ID${getSortIndicator("id")}</th>
            <th class="sortable" data-sort="label">Label${getSortIndicator("label")}</th>
            <th class="sortable" data-sort="type">Type${getSortIndicator("type")}</th>
          </tr>
        </thead>
        <tbody>
          ${
            filteredNodes.length === 0
              ? `<tr><td colspan="3" class="text-center text-gray-500 py-4">No matching nodes</td></tr>`
              : filteredNodes
                  .map(
                    (node) => `
            <tr>
              <td class="font-mono text-xs">${escapeHtml(node.id)}</td>
              <td>${escapeHtml(node.label)}</td>
              <td><span class="node-type-badge type-${node.type.toLowerCase()}">${escapeHtml(node.type)}</span></td>
            </tr>
          `
                  )
                  .join("")
          }
        </tbody>
      </table>
    </div>
  `;

  footer.innerHTML = `
    <div class="flex items-center gap-2">
      <button class="btn btn-secondary" data-action="copy-csv" title="Copy to clipboard as CSV">
        <svg class="w-4 h-4 mr-1" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
          <path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"></path>
        </svg>
        Copy CSV
      </button>
      <button class="btn btn-secondary" data-action="download-csv" title="Download as CSV file">
        <svg class="w-4 h-4 mr-1" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"></path>
          <polyline points="7 10 12 15 17 10"></polyline>
          <line x1="12" y1="15" x2="12" y2="3"></line>
        </svg>
        Download CSV
      </button>
    </div>
    <button class="btn btn-primary" data-action="close">Close</button>
  `;

  // Attach filter input handler
  const filterInput = document.getElementById("list-view-filter") as HTMLInputElement;
  if (filterInput) {
    filterInput.addEventListener("input", (e) => {
      filterText = (e.target as HTMLInputElement).value;
      applyFilterAndSort();
      renderModal();
      // Re-focus and restore cursor position
      const newInput = document.getElementById("list-view-filter") as HTMLInputElement;
      if (newInput) {
        newInput.focus();
        newInput.setSelectionRange(filterText.length, filterText.length);
      }
    });
  }
}

/** Generate CSV content */
function generateCSV(): string {
  const headers = ["ID", "Label", "Type"];
  const rows = filteredNodes.map((node) => [
    escapeCsvField(node.id),
    escapeCsvField(node.label),
    escapeCsvField(node.type),
  ]);

  return [headers.join(","), ...rows.map((row) => row.join(","))].join("\n");
}

/** Escape a field for CSV (handle commas, quotes, newlines) */
function escapeCsvField(value: string): string {
  if (value.includes(",") || value.includes('"') || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

/** Copy CSV to clipboard */
async function copyCSV(): Promise<void> {
  const csv = generateCSV();
  try {
    await navigator.clipboard.writeText(csv);
    // Show notification
    import("../utils/notifications").then(({ showSuccess }) => {
      showSuccess(`Copied ${filteredNodes.length} nodes to clipboard`);
    });
  } catch (err) {
    console.error("Failed to copy to clipboard:", err);
    import("../utils/notifications").then(({ showError }) => {
      showError("Failed to copy to clipboard");
    });
  }
}

/** Download CSV file */
function downloadCSV(): void {
  const csv = generateCSV();
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);

  const link = document.createElement("a");
  link.href = url;
  link.download = `nodes-${new Date().toISOString().slice(0, 10)}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);

  // Show notification
  import("../utils/notifications").then(({ showSuccess }) => {
    showSuccess(`Downloaded ${filteredNodes.length} nodes`);
  });
}

