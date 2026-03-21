/**
 * Edit Tiers Component
 *
 * Modal for viewing and batch-editing node tier assignments.
 * Shows a paginated, filterable list of nodes with their type, name, and tier.
 */

import { escapeHtml } from "../utils/html";
import { api } from "../api/client";
import { showSuccess, showError, showConfirm } from "../utils/notifications";

/** A node entry in the tier editor list */
interface TierNodeEntry {
  id: string;
  name: string;
  type: string;
  tier: number;
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
}

/** All fetched nodes (filtered in-memory for pagination) */
let allNodes: TierNodeEntry[] = [];
let filteredNodes: TierNodeEntry[] = [];
let pagination: PaginationState = { page: 1, perPage: 50, total: 0 };
let filters: FilterState = { nodeType: "", nameRegex: "" };
let availableTypes: string[] = [];
let isLoading = false;
let modalEl: HTMLElement | null = null;

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

  filters = { nodeType: "", nameRegex: "" };
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
    });

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
  }
}
