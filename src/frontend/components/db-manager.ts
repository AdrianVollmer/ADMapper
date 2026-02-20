/**
 * Database Manager Component
 *
 * Modal for viewing database statistics and clearing data.
 */

import { api } from "../api/client";
import { escapeHtml } from "../utils/html";
import { showConfirm } from "../utils/notifications";

/** Detailed stats response from API */
interface DetailedStats {
  total_nodes: number;
  total_edges: number;
  users: number;
  computers: number;
  groups: number;
  domains: number;
  ous: number;
  gpos: number;
}

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Initialize the database manager modal */
export function initDbManager(): void {
  // Modal is created on demand
}

/** Open the database manager modal */
export async function openDbManager(): Promise<void> {
  if (!modalEl) {
    createModal();
  }
  modalEl!.hidden = false;
  await loadStats();
}

/** Close the modal */
function closeModal(): void {
  if (modalEl) {
    modalEl.hidden = true;
  }
}

/** Create the modal element */
function createModal(): void {
  modalEl = document.createElement("div");
  modalEl.id = "db-manager-modal";
  modalEl.className = "modal-overlay";
  modalEl.innerHTML = `
    <div class="modal-content">
      <div class="modal-header">
        <h2 class="modal-title">Database Manager</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body" id="db-manager-body">
        <div class="flex items-center justify-center py-8">
          <div class="spinner"></div>
          <span class="ml-3 text-gray-400">Loading stats...</span>
        </div>
      </div>
      <div class="modal-footer" id="db-manager-footer">
        <button class="btn btn-secondary" data-action="close">Close</button>
      </div>
    </div>
  `;

  modalEl.addEventListener("click", handleClick);
  document.body.appendChild(modalEl);

  // Close on Escape
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && modalEl && !modalEl.hidden) {
      closeModal();
    }
  });
}

/** Load and display stats */
async function loadStats(): Promise<void> {
  const body = document.getElementById("db-manager-body");
  const footer = document.getElementById("db-manager-footer");
  if (!body || !footer) return;

  // Show loading spinner
  body.innerHTML = `
    <div class="flex items-center justify-center py-8">
      <div class="spinner"></div>
      <span class="ml-3 text-gray-400">Loading stats...</span>
    </div>
  `;
  footer.innerHTML = `<button class="btn btn-secondary" data-action="close">Close</button>`;

  try {
    const stats = await api.get<DetailedStats>("/api/graph/detailed-stats");
    renderStats(body, footer, stats);
  } catch (err) {
    body.innerHTML = `
      <div class="text-red-400 p-4">
        Failed to load database stats: ${escapeHtml(err instanceof Error ? err.message : String(err))}
      </div>
    `;
    footer.innerHTML = `
      <button class="btn btn-secondary" data-action="refresh">Retry</button>
      <button class="btn btn-primary" data-action="close">Close</button>
    `;
  }
}

/** Render stats in the modal */
function renderStats(body: HTMLElement, footer: HTMLElement, stats: DetailedStats): void {
  const isEmpty = stats.total_nodes === 0 && stats.total_edges === 0;

  body.innerHTML = `
    <div class="db-stats-grid">
      <div class="db-stat-card db-stat-primary">
        <div class="db-stat-value">${stats.total_nodes.toLocaleString()}</div>
        <div class="db-stat-label">Total Nodes</div>
      </div>
      <div class="db-stat-card db-stat-primary">
        <div class="db-stat-value">${stats.total_edges.toLocaleString()}</div>
        <div class="db-stat-label">Total Edges</div>
      </div>
    </div>

    <div class="db-stats-breakdown">
      <h3 class="db-stats-section-title">Node Breakdown</h3>
      <div class="db-stats-grid-small">
        <div class="db-stat-item">
          <span class="db-stat-item-value">${stats.users.toLocaleString()}</span>
          <span class="db-stat-item-label">Users</span>
        </div>
        <div class="db-stat-item">
          <span class="db-stat-item-value">${stats.computers.toLocaleString()}</span>
          <span class="db-stat-item-label">Computers</span>
        </div>
        <div class="db-stat-item">
          <span class="db-stat-item-value">${stats.groups.toLocaleString()}</span>
          <span class="db-stat-item-label">Groups</span>
        </div>
        <div class="db-stat-item">
          <span class="db-stat-item-value">${stats.domains.toLocaleString()}</span>
          <span class="db-stat-item-label">Domains</span>
        </div>
        <div class="db-stat-item">
          <span class="db-stat-item-value">${stats.ous.toLocaleString()}</span>
          <span class="db-stat-item-label">OUs</span>
        </div>
        <div class="db-stat-item">
          <span class="db-stat-item-value">${stats.gpos.toLocaleString()}</span>
          <span class="db-stat-item-label">GPOs</span>
        </div>
      </div>
    </div>

    ${
      !isEmpty
        ? `
    <div class="db-danger-zone">
      <h3 class="db-danger-title">Danger Zone</h3>
      <p class="db-danger-text">Clearing the database will permanently delete all nodes and edges. This action cannot be undone.</p>
      <button class="btn btn-danger" data-action="clear-db">Clear Database</button>
    </div>
    `
        : ""
    }
  `;

  footer.innerHTML = `
    <button class="btn btn-secondary" data-action="refresh">Refresh</button>
    <button class="btn btn-primary" data-action="close">Close</button>
  `;
}

/** Handle click events */
function handleClick(e: Event): void {
  const target = e.target as HTMLElement;

  // Close on backdrop click
  if (target.classList.contains("modal-overlay")) {
    closeModal();
    return;
  }

  const actionEl = target.closest("[data-action]") as HTMLElement;
  if (!actionEl) return;

  const action = actionEl.getAttribute("data-action");

  switch (action) {
    case "close":
      closeModal();
      break;

    case "refresh":
      loadStats();
      break;

    case "clear-db":
      clearDatabase();
      break;
  }
}

/** Clear the database */
async function clearDatabase(): Promise<void> {
  const confirmed = await showConfirm(
    "Are you sure you want to clear all data from the database? This cannot be undone.",
    { title: "Clear Database", confirmText: "Clear All Data", danger: true }
  );
  if (!confirmed) {
    return;
  }

  const body = document.getElementById("db-manager-body");
  if (body) {
    body.innerHTML = `
      <div class="flex items-center justify-center py-8">
        <div class="spinner"></div>
        <span class="ml-3 text-gray-400">Clearing database...</span>
      </div>
    `;
  }

  try {
    await api.postNoContent("/api/graph/clear");
    // Reload stats to show empty state
    await loadStats();
    // Refresh the page to reset the graph view
    window.location.reload();
  } catch (err) {
    if (body) {
      body.innerHTML = `
        <div class="text-red-400 p-4">
          Failed to clear database: ${escapeHtml(err instanceof Error ? err.message : String(err))}
        </div>
      `;
    }
  }
}
