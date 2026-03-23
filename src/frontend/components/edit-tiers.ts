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
}

/** All fetched nodes (filtered in-memory for pagination) */
let allNodes: TierNodeEntry[] = [];
let filteredNodes: TierNodeEntry[] = [];
let pagination: PaginationState = { page: 1, perPage: 50, total: 0 };
let filters: FilterState = { nodeType: "", nameRegex: "", groupId: "", groupName: "", ouId: "", ouName: "" };
let availableTypes: string[] = [];
let isLoading = false;
let modalEl: HTMLElement | null = null;

/** Debounce timer for search inputs */
let groupSearchTimer: ReturnType<typeof setTimeout> | null = null;
let ouSearchTimer: ReturnType<typeof setTimeout> | null = null;

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
}

/** Open the edit tiers modal */
export async function openEditTiers(): Promise<void> {
  createModalElement();
  if (!modalEl) return;

  filters = { nodeType: "", nameRegex: "", groupId: "", groupName: "", ouId: "", ouName: "" };
  pagination = { page: 1, perPage: 50, total: 0 };

  modalEl.removeAttribute("hidden");
  await loadNodes();
}

/** Close the modal */
function closeModal(): void {
  if (!modalEl) return;
  modalEl.setAttribute("hidden", "");
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

  // Client-side filtering for type and regex (group/OU filtering is server-side)
  filteredNodes = allNodes.filter((n) => {
    if (filters.nodeType && n.type.toLowerCase() !== filters.nodeType.toLowerCase()) {
      return false;
    }
    if (regex && !regex.test(n.name)) {
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

/** Search for groups or OUs */
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

/** Get all currently visible node IDs from the graph renderer */
function getVisibleNodeIds(): string[] {
  const renderer = getRenderer();
  if (!renderer) return [];
  try {
    const graph = renderer.sigma.getGraph();
    return graph.nodes();
  } catch {
    return [];
  }
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
  const hasVisibleNodes = getVisibleNodeIds().length > 0;

  body.innerHTML = `
    <div class="space-y-4">
      <!-- Filters -->
      <div class="flex gap-3 items-end flex-wrap">
        <div class="flex flex-col gap-1">
          <label class="text-xs text-gray-400 uppercase tracking-wide">Node Type</label>
          <select id="tier-filter-type" class="form-select" style="min-width: 140px">
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

      <!-- Group / OU / Visible Nodes filters -->
      <div class="flex gap-3 items-end flex-wrap">
        <div class="flex flex-col gap-1" style="min-width: 220px; position: relative;">
          <label class="text-xs text-gray-400 uppercase tracking-wide">Group Membership</label>
          <input id="tier-filter-group" type="text" class="form-input" placeholder="Search group..."
            value="${escapeHtml(filters.groupName)}" autocomplete="off" />
          <div id="tier-group-suggestions" class="search-suggestions" style="display: none; position: absolute; top: 100%; left: 0; right: 0; z-index: 10; background: var(--bg-secondary); border: 1px solid var(--border-color); border-radius: 4px; max-height: 200px; overflow-y: auto;"></div>
          ${filters.groupId ? `<button class="text-xs text-gray-500 hover:text-gray-300" data-action="clear-group" style="align-self: flex-start;">&times; Clear</button>` : ""}
        </div>
        <div class="flex flex-col gap-1" style="min-width: 220px; position: relative;">
          <label class="text-xs text-gray-400 uppercase tracking-wide">OU Containment</label>
          <input id="tier-filter-ou" type="text" class="form-input" placeholder="Search OU..."
            value="${escapeHtml(filters.ouName)}" autocomplete="off" />
          <div id="tier-ou-suggestions" class="search-suggestions" style="display: none; position: absolute; top: 100%; left: 0; right: 0; z-index: 10; background: var(--bg-secondary); border: 1px solid var(--border-color); border-radius: 4px; max-height: 200px; overflow-y: auto;"></div>
          ${filters.ouId ? `<button class="text-xs text-gray-500 hover:text-gray-300" data-action="clear-ou" style="align-self: flex-start;">&times; Clear</button>` : ""}
        </div>
        ${hasVisibleNodes ? `<button class="btn btn-sm btn-secondary" data-action="tag-visible" title="Use currently visible graph nodes">Tag Visible Nodes</button>` : ""}
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
  if (groupInput) {
    groupInput.addEventListener("input", () => {
      if (groupSearchTimer) clearTimeout(groupSearchTimer);
      groupSearchTimer = setTimeout(async () => {
        const suggestions = await searchEntities(groupInput.value, "Group");
        showSuggestions("tier-group-suggestions", suggestions, (s) => {
          filters.groupId = s.id;
          filters.groupName = s.name;
          render();
        });
      }, 300);
    });
    groupInput.addEventListener("blur", () => {
      // Delay hiding to allow click on suggestion
      setTimeout(() => hideSuggestions("tier-group-suggestions"), 200);
    });
  }

  // OU search with debounce
  const ouInput = document.getElementById("tier-filter-ou") as HTMLInputElement;
  if (ouInput) {
    ouInput.addEventListener("input", () => {
      if (ouSearchTimer) clearTimeout(ouSearchTimer);
      ouSearchTimer = setTimeout(async () => {
        const suggestions = await searchEntities(ouInput.value, "OU");
        showSuggestions("tier-ou-suggestions", suggestions, (s) => {
          filters.ouId = s.id;
          filters.ouName = s.name;
          render();
        });
      }, 300);
    });
    ouInput.addEventListener("blur", () => {
      setTimeout(() => hideSuggestions("tier-ou-suggestions"), 200);
    });
  }
}

/** Show search suggestions in a dropdown */
function showSuggestions(containerId: string, suggestions: SearchSuggestion[], onSelect: (s: SearchSuggestion) => void): void {
  const container = document.getElementById(containerId);
  if (!container) return;

  if (suggestions.length === 0) {
    container.style.display = "none";
    return;
  }

  container.innerHTML = suggestions
    .map(
      (s, i) =>
        `<div class="search-suggestion-item" data-index="${i}" style="padding: 6px 10px; cursor: pointer; font-size: 0.85rem; border-bottom: 1px solid var(--border-color);">
          <span class="text-xs px-1 py-0.5 rounded bg-gray-700 text-gray-400">${escapeHtml(s.type)}</span>
          ${escapeHtml(s.name)}
        </div>`
    )
    .join("");
  container.style.display = "block";

  // Attach click handlers
  container.querySelectorAll(".search-suggestion-item").forEach((el, i) => {
    el.addEventListener("mousedown", (e) => {
      e.preventDefault(); // Prevent blur from firing first
      const suggestion = suggestions[i];
      if (suggestion) onSelect(suggestion);
    });
  });
}

/** Hide suggestion dropdown */
function hideSuggestions(containerId: string): void {
  const container = document.getElementById(containerId);
  if (container) container.style.display = "none";
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
    const result = await api.post<{ updated: number }>("/api/graph/batch-set-tier", {
      tier,
      node_type: filters.nodeType || null,
      name_regex: filters.nameRegex || null,
      group_id: filters.groupId || null,
      ou_id: filters.ouId || null,
    });

    showSuccess(`Updated tier to ${tier} on ${result.updated.toLocaleString()} node${result.updated === 1 ? "" : "s"}`);

    // Refresh the list to show updated tiers
    await loadNodes();
  } catch (err) {
    console.error("Batch tier update failed:", err);
    showError("Failed to update tiers");
  }
}

/** Tag visible nodes: sends all visible graph node IDs for batch tier assignment */
async function tagVisibleNodes(): Promise<void> {
  const selectEl = document.getElementById("tier-batch-value") as HTMLSelectElement;
  if (!selectEl) return;
  const tier = parseInt(selectEl.value, 10);

  const visibleIds = getVisibleNodeIds();
  if (visibleIds.length === 0) {
    showError("No visible nodes in graph");
    return;
  }

  const confirmed = await showConfirm(
    `This will set tier ${tier} on ${visibleIds.length.toLocaleString()} currently visible graph node${visibleIds.length === 1 ? "" : "s"}.\n\nContinue?`
  );
  if (!confirmed) return;

  try {
    const result = await api.post<{ updated: number }>("/api/graph/batch-set-tier", {
      tier,
      node_ids: visibleIds,
    });

    showSuccess(`Updated tier to ${tier} on ${result.updated.toLocaleString()} node${result.updated === 1 ? "" : "s"}`);
    await loadNodes();
  } catch (err) {
    console.error("Tag visible nodes failed:", err);
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
    case "tag-visible":
      tagVisibleNodes();
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
