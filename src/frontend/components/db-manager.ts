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
  }
}

/** Clear the database */
export async function clearDatabase(): Promise<void> {
  const confirmed = await showConfirm(
    "Are you sure you want to clear all data from the database? This cannot be undone.",
    { title: "Clear Database", confirmText: "Clear All Data", danger: true }
  );
  if (!confirmed) {
    return;
  }

  try {
    await api.postNoContent("/api/graph/clear");
    // Refresh the page to reset the graph view
    window.location.reload();
  } catch (err) {
    await showConfirm(`Failed to clear database: ${err instanceof Error ? err.message : String(err)}`, {
      title: "Error",
      confirmText: "OK",
      danger: false,
    });
  }
}

/** Clear disabled objects from the database */
export async function clearDisabledObjects(): Promise<void> {
  const confirmed = await showConfirm(
    "This will delete all disabled objects (users, computers, etc. with enabled=false) from the database. Continue?",
    { title: "Clear Disabled Objects", confirmText: "Clear Disabled", danger: true }
  );
  if (!confirmed) {
    return;
  }

  try {
    await api.postNoContent("/api/graph/clear-disabled");
    // Refresh the page to update the graph view
    window.location.reload();
  } catch (err) {
    await showConfirm(`Failed to clear disabled objects: ${err instanceof Error ? err.message : String(err)}`, {
      title: "Error",
      confirmText: "OK",
      danger: false,
    });
  }
}

/** Cache stats response from API */
interface CacheStats {
  supported: boolean;
  entry_count: number | null;
  size_bytes: number | null;
}

/** Format bytes as human-readable size */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

/** Clear the query cache (CrustDB only) */
export async function clearCache(): Promise<void> {
  try {
    // First get cache stats to show size
    const stats = await api.get<CacheStats>("/api/cache/stats");

    if (!stats.supported) {
      await showConfirm("Query caching is only supported by CrustDB. The current database does not have a cache.", {
        title: "Cache Not Supported",
        confirmText: "OK",
        danger: false,
      });
      return;
    }

    const entryCount = stats.entry_count ?? 0;
    const sizeBytes = stats.size_bytes ?? 0;

    if (entryCount === 0) {
      await showConfirm("The query cache is already empty.", {
        title: "Cache Empty",
        confirmText: "OK",
        danger: false,
      });
      return;
    }

    const sizeStr = formatBytes(sizeBytes);
    const confirmed = await showConfirm(
      `Clear ${entryCount} cached ${entryCount === 1 ? "query" : "queries"} (${sizeStr})?`,
      { title: "Clear Query Cache", confirmText: "Clear Cache", danger: true }
    );
    if (!confirmed) {
      return;
    }

    await api.postNoContent("/api/cache/clear");
    await showConfirm("Query cache cleared successfully.", {
      title: "Cache Cleared",
      confirmText: "OK",
      danger: false,
    });
  } catch (err) {
    await showConfirm(`Failed to clear cache: ${err instanceof Error ? err.message : String(err)}`, {
      title: "Error",
      confirmText: "OK",
      danger: false,
    });
  }
}
