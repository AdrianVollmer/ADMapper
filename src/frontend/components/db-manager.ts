/**
 * Database Manager Component
 *
 * Modal for viewing database statistics and clearing data.
 */

import { api } from "../api/client";
import { escapeHtml } from "../utils/html";
import { createModal, type ModalHandle } from "../utils/modal";
import { showConfirm } from "../utils/notifications";
import { loadGraphData } from "./graph-view";
import type { RawADGraph } from "../graph/types";

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
  database_size_bytes?: number;
  cache_entries?: number;
  cache_size_bytes?: number;
}

/** Modal handle */
let modal: ModalHandle | null = null;

/** Initialize the database manager modal */
export function initDbManager(): void {
  // Modal is created on demand
}

/** Open the database manager modal */
export async function openDbManager(): Promise<void> {
  if (!modal) {
    modal = createModal({
      id: "db-manager-modal",
      title: "Database Manager",
      buttons: [{ label: "Close", action: "close", className: "btn btn-secondary" }],
      onClick: (action) => {
        if (action === "refresh") {
          loadStats();
        }
      },
    });
  }
  modal.open();
  await loadStats();
}

/** Load and display stats */
async function loadStats(): Promise<void> {
  if (!modal) return;
  const { body, footer } = modal;

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
  // Build storage section HTML (only for CrustDB)
  let storageSection = "";
  if (stats.database_size_bytes !== undefined) {
    storageSection = `
    <div class="db-stats-breakdown">
      <h3 class="db-stats-section-title">Storage</h3>
      <div class="db-stats-grid-small">
        <div class="db-stat-item">
          <span class="db-stat-item-value">${formatBytes(stats.database_size_bytes)}</span>
          <span class="db-stat-item-label">Database Size</span>
        </div>
        <div class="db-stat-item">
          <span class="db-stat-item-value">${stats.cache_entries?.toLocaleString() ?? 0}</span>
          <span class="db-stat-item-label">Cached Queries</span>
        </div>
        <div class="db-stat-item">
          <span class="db-stat-item-value">${formatBytes(stats.cache_size_bytes ?? 0)}</span>
          <span class="db-stat-item-label">Cache Size</span>
        </div>
      </div>
    </div>
    `;
  }

  body.innerHTML = `
    <div class="db-stats-grid">
      <div class="db-stat-card db-stat-primary">
        <div class="db-stat-value">${stats.total_nodes.toLocaleString()}</div>
        <div class="db-stat-label">Total Nodes</div>
      </div>
      <div class="db-stat-card db-stat-primary">
        <div class="db-stat-value">${stats.total_edges.toLocaleString()}</div>
        <div class="db-stat-label">Total Relationships</div>
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

    ${storageSection}
  `;

  footer.innerHTML = `
    <button class="btn btn-secondary" data-action="refresh">Refresh</button>
    <button class="btn btn-primary" data-action="close">Close</button>
  `;
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
    // Refresh the graph programmatically
    const graphData = await api.get<RawADGraph>("/api/graph/all");
    await loadGraphData(graphData);
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
    // Refresh the graph programmatically
    const graphData = await api.get<RawADGraph>("/api/graph/all");
    await loadGraphData(graphData);
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
