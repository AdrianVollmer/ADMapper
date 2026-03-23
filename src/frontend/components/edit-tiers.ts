/**
 * Edit Tiers Component
 *
 * Modal for viewing and batch-editing node tier assignments.
 * Shows a paginated, filterable list of nodes with their type, name, and tier.
 * Supports filtering by group membership, OU containment, and visible graph nodes.
 */

import { escapeHtml } from "../utils/html";
import { api } from "../api/client";
import { showSuccess, showError, showConfirm } from "../utils/notifications";
import { getRenderer } from "./graph-view";
import { NODE_COLORS } from "../graph/theme";
import { getNodeIconPath } from "../graph/icons";
import type { ADNodeType } from "../graph/types";

/** A node entry in the tier editor list */
interface TierNodeEntry {
  id: string;
  name: string;
  type: string;
  tier: number;
}

/** Search suggestion for group/OU pickers */
interface SearchSuggestion {
  id: string;
  name: string;
  type: string;
}

/** Pagination state */
interface PaginationState {
  page: number;
  perPage: number;
  total: number;
}

/** Filter state */
interface FilterState {
  nodeType: string; // "" = all
  nameRegex: string;
  groupId: string; // selected group ID for membership filter
  groupName: string; // display name
  ouId: string; // selected OU ID for containment filter
  ouName: string; // display name
  visibleOnly: boolean; // show only nodes currently visible in graph
}

/** All fetched nodes (filtered in-memory for pagination) */
let allNodes: TierNodeEntry[] = [];
let filteredNodes: TierNodeEntry[] = [];
let pagination: PaginationState = { page: 1, perPage: 50, total: 0 };
let filters: FilterState = { nodeType: "", nameRegex: "", groupId: "", groupName: "", ouId: "", ouName: "", visibleOnly: false };
let availableTypes: string[] = [];
let isLoading = false;
let modalEl: HTMLElement | null = null;

/** Debounce timer for search inputs */
let groupSearchTimer: ReturnType<typeof setTimeout> | null = null;
let ouSearchTimer: ReturnType<typeof setTimeout> | null = null;

/** Search result dropdown containers (portaled to document.body like sidebar search) */
let groupResultsEl: HTMLElement | null = null;
let ouResultsEl: HTMLElement | null = null;

/** Pending onSelect callbacks for suggestion dropdowns */
let groupOnSelect: ((s: SearchSuggestion) => void) | null = null;
let ouOnSelect: ((s: SearchSuggestion) => void) | null = null;

/** Create a search-results container portaled to document.body */
function getOrCreateResultsContainer(id: string): HTMLElement {
  let container = document.getElementById(id);
  if (!container) {
    container = document.createElement("div");
    container.id = id;
    container.className = "search-results";
    container.hidden = true;
    document.body.appendChild(container);
  }
  return container;
}

/** Position a results popover below its input */
function positionPopover(input: HTMLInputElement, resultsEl: HTMLElement): void {
  const rect = input.getBoundingClientRect();
  resultsEl.style.top = `${rect.bottom}px`;
  resultsEl.style.left = `${rect.left}px`;
  resultsEl.style.minWidth = `${rect.width}px`;
}

/** Create the modal element and append to body */
function createModalElement(): void {
  if (modalEl) return;

  const modal = document.createElement("div");
  modal.id = "edit-tiers-modal";
  modal.className = "modal-overlay";
  modal.setAttribute("hidden", "");
  modal.innerHTML = `
    <div class="modal-content modal-xl">
      <div class="modal-header">
        <h2 class="modal-title">Edit Tiers</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body" id="edit-tiers-body">
        <!-- Content rendered dynamically -->
      </div>
      <div class="modal-footer" id="edit-tiers-footer">
        <button class="btn btn-secondary" data-action="close">Close</button>
      </div>
    </div>
  `;

  modal.addEventListener("click", handleModalClick);
  document.body.appendChild(modal);
  modalEl = modal;

  // Create search result containers (portaled to body, like sidebar search)
  groupResultsEl = getOrCreateResultsContainer("tier-group-results");
  ouResultsEl = getOrCreateResultsContainer("tier-ou-results");

  // Handle clicks on search results
  groupResultsEl.addEventListener("mousedown", (e) => {
    e.preventDefault();
    const item = (e.target as HTMLElement).closest(".search-result-item") as HTMLElement;
    if (item && groupOnSelect) {
      const id = item.dataset.nodeId ?? "";
      const name = item.dataset.nodeLabel ?? "";
      const type = item.dataset.nodeType ?? "";
      groupOnSelect({ id, name, type });
    }
  });
  ouResultsEl.addEventListener("mousedown", (e) => {
    e.preventDefault();
    const item = (e.target as HTMLElement).closest(".search-result-item") as HTMLElement;
    if (item && ouOnSelect) {
      const id = item.dataset.nodeId ?? "";
      const name = item.dataset.nodeLabel ?? "";
      const type = item.dataset.nodeType ?? "";
      ouOnSelect({ id, name, type });
    }
  });
}

/** Open the edit tiers modal */
export async function openEditTiers(): Promise<void> {
  createModalElement();
  if (!modalEl) return;

  filters = { nodeType: "", nameRegex: "", groupId: "", groupName: "", ouId: "", ouName: "", visibleOnly: false };
  pagination = { page: 1, perPage: 50, total: 0 };

  modalEl.removeAttribute("hidden");
  await loadNodes();
}

/** Close the modal */
function closeModal(): void {
  if (!modalEl) return;
  modalEl.setAttribute("hidden", "");
  // Hide any open dropdowns
  if (groupResultsEl) groupResultsEl.hidden = true;
  if (ouResultsEl) ouResultsEl.hidden = true;
}

/** Fetch all nodes from the backend */
async function loadNodes(): Promise<void> {
  isLoading = true;
  render();

  try {
    const nodes =
      await api.get<Array<{ id: string; name: string; type: string; properties?: Record<string, unknown> }>>(
        "/api/graph/nodes"
      );

    allNodes = nodes.map((n) => ({
      id: n.id,
      name: n.name,
      type: n.type,
      tier: typeof n.properties?.tier === "number" ? n.properties.tier : 3,
    }));

    // Extract unique types
    const typeSet = new Set(allNodes.map((n) => n.type));
    availableTypes = [...typeSet].sort();

    applyFilters();
  } catch (err) {
    console.error("Failed to load nodes:", err);
    showError("Failed to load nodes");
  } finally {
    isLoading = false;
    render();
  }
}

/** Get all currently visible node IDs from the graph renderer */
function getVisibleNodeIds(): Set<string> {
  const renderer = getRenderer();
  if (!renderer) return new Set();
  try {
    const graph = renderer.sigma.getGraph();
    return new Set(graph.nodes());
  } catch {
    return new Set();
  }
}

/** Apply filters and update pagination */
function applyFilters(): void {
  let regex: RegExp | null = null;
  if (filters.nameRegex) {
    try {
      regex = new RegExp(filters.nameRegex, "i");
    } catch {
      // Invalid regex — show no results
      filteredNodes = [];
      pagination.total = 0;
      pagination.page = 1;
      render();
      return;
    }
  }

  const visibleIds = filters.visibleOnly ? getVisibleNodeIds() : null;

  // Client-side filtering for type, regex, and visible-only
  filteredNodes = allNodes.filter((n) => {
    if (filters.nodeType && n.type.toLowerCase() !== filters.nodeType.toLowerCase()) {
      return false;
    }
    if (regex && !regex.test(n.name)) {
      return false;
    }
    if (visibleIds && !visibleIds.has(n.id)) {
      return false;
    }
    return true;
  });

  pagination.total = filteredNodes.length;
  pagination.page = 1;
  render();
}

/** Get the current page of nodes */
function getCurrentPage(): TierNodeEntry[] {
  const start = (pagination.page - 1) * pagination.perPage;
  return filteredNodes.slice(start, start + pagination.perPage);
}

/** Search for groups or OUs via the API */
async function searchEntities(query: string, type: string): Promise<SearchSuggestion[]> {
  if (query.length < 2) return [];
  try {
    const results = await api.get<Array<{ id: string; name: string; type?: string; labels?: string[] }>>(
      `/api/graph/search?q=${encodeURIComponent(query)}&limit=10`
    );
    return results
      .filter((r) => {
        const rType = (r.type || r.labels?.[0] || "").toLowerCase();
        return rType === type.toLowerCase();
      })
      .map((r) => ({
        id: r.id,
        name: r.name,
        type: r.type || r.labels?.[0] || type,
      }));
  } catch {
    return [];
  }
}

/** Show search suggestions in a search-results dropdown (same style as sidebar search) */
function showSuggestions(input: HTMLInputElement, resultsEl: HTMLElement, suggestions: SearchSuggestion[]): void {
  if (suggestions.length === 0) {
    resultsEl.hidden = true;
    return;
  }

  resultsEl.innerHTML = suggestions
    .map((s) => {
      const nodeType = s.type as ADNodeType;
      const color = NODE_COLORS[nodeType] || "#6c757d";
      const iconPath = getNodeIconPath(nodeType);
      return `
        <div class="search-result-item" data-node-id="${escapeHtml(s.id)}" data-node-label="${escapeHtml(s.name)}" data-node-type="${escapeHtml(s.type)}">
          <span class="node-type-icon" style="background-color: ${color}" title="${escapeHtml(s.type)}">
            <svg viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${iconPath}</svg>
          </span>
          <span class="node-name">${escapeHtml(s.name)}</span>
        </div>
      `;
    })
    .join("");

  positionPopover(input, resultsEl);
  resultsEl.hidden = false;
}

/** Render the modal body */
function render(): void {
  const body = document.getElementById("edit-tiers-body");
  if (!body) return;

  if (isLoading) {
    body.innerHTML = `<div class="flex items-center justify-center py-8"><span class="spinner"></span></div>`;
    return;
  }

  const totalPages = Math.max(1, Math.ceil(pagination.total / pagination.perPage));
  const page = getCurrentPage();
  const hasVisibleNodes = getVisibleNodeIds().size > 0;

  body.innerHTML = `
    <div class="space-y-4">
      <!-- Row 1: Node Type + Name Regex + Filter button -->
      <div class="flex gap-3 items-end">
        <div class="flex flex-col gap-1" style="min-width: 140px">
          <label class="text-xs text-gray-400 uppercase tracking-wide">Node Type</label>
          <select id="tier-filter-type" class="form-select">
            <option value="">All types</option>
            ${availableTypes.map((t) => `<option value="${escapeHtml(t)}" ${filters.nodeType === t ? "selected" : ""}>${escapeHtml(t)}</option>`).join("")}
          </select>
        </div>
        <div class="flex flex-col gap-1 flex-1">
          <label class="text-xs text-gray-400 uppercase tracking-wide">Name (regex)</label>
          <input id="tier-filter-regex" type="text" class="form-input" placeholder="e.g. ADMIN|SERVER" value="${escapeHtml(filters.nameRegex)}" />
        </div>
        <button class="btn btn-sm btn-secondary" data-action="apply-filters">Filter</button>
      </div>

      <!-- Row 2: Group + OU + Load Visible -->
      <div class="flex gap-3 items-end">
        <div class="flex flex-col gap-1 flex-1">
          <label class="text-xs text-gray-400 uppercase tracking-wide">Group Membership</label>
          <div class="flex gap-2 items-center">
            <input id="tier-filter-group" type="text" class="form-input flex-1" placeholder="Search group..."
              value="${escapeHtml(filters.groupName)}" autocomplete="off" />
            ${filters.groupId ? `<button class="btn btn-sm btn-secondary" data-action="clear-group" title="Clear group filter" style="padding: 4px 8px">&times;</button>` : ""}
          </div>
        </div>
        <div class="flex flex-col gap-1 flex-1">
          <label class="text-xs text-gray-400 uppercase tracking-wide">OU Containment</label>
          <div class="flex gap-2 items-center">
            <input id="tier-filter-ou" type="text" class="form-input flex-1" placeholder="Search OU..."
              value="${escapeHtml(filters.ouName)}" autocomplete="off" />
            ${filters.ouId ? `<button class="btn btn-sm btn-secondary" data-action="clear-ou" title="Clear OU filter" style="padding: 4px 8px">&times;</button>` : ""}
          </div>
        </div>
        <button class="btn btn-sm ${filters.visibleOnly ? "btn-primary" : "btn-secondary"}" data-action="load-visible" title="Filter to nodes currently visible in the graph" ${!hasVisibleNodes ? "disabled" : ""}>
          ${filters.visibleOnly ? "Showing visible" : "Load Visible Nodes"}
        </button>
      </div>

      ${filters.groupId ? `<div class="text-xs text-blue-400">Group filter: ${escapeHtml(filters.groupName)}</div>` : ""}
      ${filters.ouId ? `<div class="text-xs text-blue-400">OU filter: ${escapeHtml(filters.ouName)}</div>` : ""}

      <!-- Results summary -->
      <div class="text-sm text-gray-400">
        ${pagination.total.toLocaleString()} node${pagination.total === 1 ? "" : "s"} matching filters
      </div>

      <!-- Table -->
      <div class="list-view-table-container" style="max-height: 400px">
        <table class="list-view-table">
          <thead>
            <tr>
              <th style="width: 100px">Type</th>
              <th>Name</th>
              <th style="width: 70px">Tier</th>
            </tr>
          </thead>
          <tbody>
            ${
              page.length === 0
                ? `<tr><td colspan="3" class="text-center text-gray-500 py-4">No nodes found</td></tr>`
                : page
                    .map(
                      (n) => `
              <tr>
                <td><span class="text-xs px-1.5 py-0.5 rounded bg-gray-700 text-gray-300">${escapeHtml(n.type)}</span></td>
                <td class="truncate" style="max-width: 400px" title="${escapeHtml(n.name)}">${escapeHtml(n.name)}</td>
                <td class="font-mono">${n.tier}</td>
              </tr>
            `
                    )
                    .join("")
            }
          </tbody>
        </table>
      </div>

      <!-- Pagination -->
      ${
        totalPages > 1
          ? `
        <div class="flex items-center justify-center gap-4 pt-2">
          <button class="btn btn-sm btn-secondary" data-action="prev-page" ${pagination.page <= 1 ? "disabled" : ""}>Previous</button>
          <span class="text-sm text-gray-400">Page ${pagination.page} of ${totalPages}</span>
          <button class="btn btn-sm btn-secondary" data-action="next-page" ${pagination.page >= totalPages ? "disabled" : ""}>Next</button>
        </div>
      `
          : ""
      }

      <!-- Batch assign -->
      <div class="border-t border-gray-700 pt-4 mt-4">
        <h3 class="text-sm font-semibold text-gray-300 mb-2">Assign Tier to Filtered Nodes</h3>
        <div class="flex gap-3 items-end">
          <div class="flex flex-col gap-1">
            <label class="text-xs text-gray-400 uppercase tracking-wide">Tier</label>
            <select id="tier-batch-value" class="form-select" style="width: 80px">
              <option value="0">0</option>
              <option value="1">1</option>
              <option value="2">2</option>
              <option value="3" selected>3</option>
            </select>
          </div>
          <button class="btn btn-sm btn-primary" data-action="batch-assign" ${pagination.total === 0 ? "disabled" : ""}>
            Assign to ${pagination.total.toLocaleString()} node${pagination.total === 1 ? "" : "s"}
          </button>
        </div>
      </div>
    </div>
  `;

  // Attach filter listeners (Enter key on inputs)
  const regexInput = document.getElementById("tier-filter-regex") as HTMLInputElement;
  if (regexInput) {
    regexInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") applyFiltersFromUI();
    });
  }
  const typeSelect = document.getElementById("tier-filter-type") as HTMLSelectElement;
  if (typeSelect) {
    typeSelect.addEventListener("change", () => applyFiltersFromUI());
  }

  // Group search with debounce
  const groupInput = document.getElementById("tier-filter-group") as HTMLInputElement;
  if (groupInput && groupResultsEl) {
    groupOnSelect = (s) => {
      filters.groupId = s.id;
      filters.groupName = s.name;
      if (groupResultsEl) groupResultsEl.hidden = true;
      render();
    };
    groupInput.addEventListener("input", () => {
      if (groupSearchTimer) clearTimeout(groupSearchTimer);
      groupSearchTimer = setTimeout(async () => {
        const suggestions = await searchEntities(groupInput.value, "Group");
        showSuggestions(groupInput, groupResultsEl!, suggestions);
      }, 300);
    });
    groupInput.addEventListener("focus", () => {
      // Re-search if there's already text
      if (groupInput.value.length >= 2) {
        searchEntities(groupInput.value, "Group").then((suggestions) => {
          showSuggestions(groupInput, groupResultsEl!, suggestions);
        });
      }
    });
    groupInput.addEventListener("blur", () => {
      setTimeout(() => { if (groupResultsEl) groupResultsEl.hidden = true; }, 150);
    });
  }

  // OU search with debounce
  const ouInput = document.getElementById("tier-filter-ou") as HTMLInputElement;
  if (ouInput && ouResultsEl) {
    ouOnSelect = (s) => {
      filters.ouId = s.id;
      filters.ouName = s.name;
      if (ouResultsEl) ouResultsEl.hidden = true;
      render();
    };
    ouInput.addEventListener("input", () => {
      if (ouSearchTimer) clearTimeout(ouSearchTimer);
      ouSearchTimer = setTimeout(async () => {
        const suggestions = await searchEntities(ouInput.value, "OU");
        showSuggestions(ouInput, ouResultsEl!, suggestions);
      }, 300);
    });
    ouInput.addEventListener("focus", () => {
      if (ouInput.value.length >= 2) {
        searchEntities(ouInput.value, "OU").then((suggestions) => {
          showSuggestions(ouInput, ouResultsEl!, suggestions);
        });
      }
    });
    ouInput.addEventListener("blur", () => {
      setTimeout(() => { if (ouResultsEl) ouResultsEl.hidden = true; }, 150);
    });
  }
}

/** Read filter values from the UI and apply */
function applyFiltersFromUI(): void {
  const typeEl = document.getElementById("tier-filter-type") as HTMLSelectElement;
  const regexEl = document.getElementById("tier-filter-regex") as HTMLInputElement;
  if (typeEl) filters.nodeType = typeEl.value;
  if (regexEl) filters.nameRegex = regexEl.value;
  applyFilters();
}

/** Handle batch tier assignment */
async function batchAssignTier(): Promise<void> {
  const selectEl = document.getElementById("tier-batch-value") as HTMLSelectElement;
  if (!selectEl) return;
  const tier = parseInt(selectEl.value, 10);

  const count = filteredNodes.length;
  if (count === 0) return;

  const confirmed = await showConfirm(
    `This will set tier ${tier} on ${count.toLocaleString()} node${count === 1 ? "" : "s"} matching the current filters.\n\nThis is a batch operation and cannot easily be undone.\n\nContinue?`
  );
  if (!confirmed) return;

  try {
    // If we're showing visible-only nodes, send the explicit IDs
    const payload: Record<string, unknown> = {
      tier,
      node_type: filters.nodeType || null,
      name_regex: filters.nameRegex || null,
      group_id: filters.groupId || null,
      ou_id: filters.ouId || null,
    };
    if (filters.visibleOnly) {
      payload.node_ids = filteredNodes.map((n) => n.id);
    }

    const result = await api.post<{ updated: number }>("/api/graph/batch-set-tier", payload);

    showSuccess(`Updated tier to ${tier} on ${result.updated.toLocaleString()} node${result.updated === 1 ? "" : "s"}`);

    // Refresh the list to show updated tiers
    await loadNodes();
  } catch (err) {
    console.error("Batch tier update failed:", err);
    showError("Failed to update tiers");
  }
}

/** Handle clicks within the modal */
function handleModalClick(e: Event): void {
  const target = e.target as HTMLElement;

  // Close on overlay click
  if (target === modalEl) {
    closeModal();
    return;
  }

  const actionEl = target.closest<HTMLElement>("[data-action]");
  if (!actionEl) return;

  const action = actionEl.dataset.action;
  switch (action) {
    case "close":
      closeModal();
      break;
    case "apply-filters":
      applyFiltersFromUI();
      break;
    case "prev-page":
      if (pagination.page > 1) {
        pagination.page--;
        render();
      }
      break;
    case "next-page": {
      const totalPages = Math.ceil(pagination.total / pagination.perPage);
      if (pagination.page < totalPages) {
        pagination.page++;
        render();
      }
      break;
    }
    case "batch-assign":
      batchAssignTier();
      break;
    case "load-visible":
      filters.visibleOnly = !filters.visibleOnly;
      applyFilters();
      break;
    case "clear-group":
      filters.groupId = "";
      filters.groupName = "";
      render();
      break;
    case "clear-ou":
      filters.ouId = "";
      filters.ouName = "";
      render();
      break;
  }
}
