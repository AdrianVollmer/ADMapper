/**
 * Paths to Domain Admins Component
 *
 * Built-in feature to find all users with attack paths to Domain Admins.
 * Supports edge type filtering to exclude certain relationship types.
 */

import { api } from "../api/client";
import { escapeHtml } from "../utils/html";
import { addToHistory } from "./query-history";
import { getRenderer } from "./graph-view";

/** Common edge types in BloodHound data */
const EDGE_TYPES = [
  { id: "MemberOf", label: "MemberOf", description: "Group membership" },
  { id: "HasSession", label: "HasSession", description: "Active sessions" },
  { id: "AdminTo", label: "AdminTo", description: "Local admin rights" },
  { id: "CanRDP", label: "CanRDP", description: "RDP access" },
  { id: "CanPSRemote", label: "CanPSRemote", description: "PSRemote access" },
  { id: "ExecuteDCOM", label: "ExecuteDCOM", description: "DCOM execution" },
  { id: "SQLAdmin", label: "SQLAdmin", description: "SQL Server admin" },
  { id: "AllowedToDelegate", label: "AllowedToDelegate", description: "Constrained delegation" },
  { id: "AllowedToAct", label: "AllowedToAct", description: "RBCD" },
  { id: "GenericAll", label: "GenericAll", description: "Full control" },
  { id: "GenericWrite", label: "GenericWrite", description: "Write properties" },
  { id: "WriteDacl", label: "WriteDacl", description: "Modify permissions" },
  { id: "WriteOwner", label: "WriteOwner", description: "Change owner" },
  { id: "Owns", label: "Owns", description: "Object owner" },
  { id: "ForceChangePassword", label: "ForceChangePassword", description: "Reset password" },
  { id: "AddMember", label: "AddMember", description: "Add group members" },
  { id: "AddSelf", label: "AddSelf", description: "Add self to group" },
  { id: "ReadLAPSPassword", label: "ReadLAPSPassword", description: "Read LAPS" },
  { id: "ReadGMSAPassword", label: "ReadGMSAPassword", description: "Read GMSA" },
  { id: "DCSync", label: "DCSync", description: "DCSync rights" },
  { id: "GetChanges", label: "GetChanges", description: "Replication rights" },
  { id: "GetChangesAll", label: "GetChangesAll", description: "Full replication" },
];

/** Response type for paths-to-da API */
interface PathsToDaResponse {
  count: number;
  entries: Array<{
    id: string;
    type: string;
    label: string;
    hops: number;
  }>;
}

/** Currently excluded edge types */
const excludedEdgeTypes = new Set<string>();

/** DOM elements */
let findBtn: HTMLButtonElement | null = null;
let resultsEl: HTMLElement | null = null;
let edgeTypesEl: HTMLElement | null = null;

/** Initialize the paths-to-da component */
export function initPathsToDa(): void {
  findBtn = document.getElementById("paths-to-da-btn") as HTMLButtonElement;
  resultsEl = document.getElementById("paths-to-da-results");
  edgeTypesEl = document.getElementById("paths-to-da-edge-types");

  if (!findBtn || !resultsEl || !edgeTypesEl) return;

  // Render edge type checkboxes
  renderEdgeTypeFilters();

  // Set up button click handler
  findBtn.addEventListener("click", handleFindPaths);

  // Set up edge type checkbox handlers
  edgeTypesEl.addEventListener("change", handleEdgeTypeChange);
}

/** Render edge type filter checkboxes */
function renderEdgeTypeFilters(): void {
  if (!edgeTypesEl) return;

  let html = "";
  for (const edge of EDGE_TYPES) {
    html += `
      <label class="edge-type-filter" title="${escapeHtml(edge.description)}">
        <input type="checkbox" value="${escapeHtml(edge.id)}" checked />
        <span>${escapeHtml(edge.label)}</span>
      </label>
    `;
  }

  edgeTypesEl.innerHTML = html;
}

/** Handle edge type checkbox change */
function handleEdgeTypeChange(e: Event): void {
  const target = e.target as HTMLInputElement;
  if (target.type !== "checkbox") return;

  const edgeType = target.value;
  if (target.checked) {
    excludedEdgeTypes.delete(edgeType);
  } else {
    excludedEdgeTypes.add(edgeType);
  }
}

/** Set button loading state */
function setButtonLoading(loading: boolean): void {
  if (!findBtn) return;

  if (loading) {
    findBtn.setAttribute("disabled", "true");
    findBtn.classList.add("flex", "items-center", "justify-center", "gap-2");
    findBtn.innerHTML = '<span class="spinner-sm"></span>Finding...';
  } else {
    findBtn.removeAttribute("disabled");
    findBtn.classList.remove("flex", "items-center", "justify-center", "gap-2");
    findBtn.textContent = "Find Users with Paths";
  }
}

/** Handle find paths button click */
async function handleFindPaths(): Promise<void> {
  if (!resultsEl) return;

  setButtonLoading(true);
  resultsEl.hidden = true;

  try {
    // Build exclude parameter
    const excludeParam = Array.from(excludedEdgeTypes).join(",");
    const url = excludeParam
      ? `/api/graph/paths-to-da?exclude=${encodeURIComponent(excludeParam)}`
      : "/api/graph/paths-to-da";

    const response = await api.get<PathsToDaResponse>(url);

    // Add to query history
    const queryName =
      excludedEdgeTypes.size > 0
        ? `Paths to DA (excluding ${excludedEdgeTypes.size} edge types)`
        : "Paths to Domain Admins";
    addToHistory(queryName, `GET ${url}`, response.count);

    // Render results
    renderResults(response);
  } catch (err) {
    console.error("Paths to DA query failed:", err);
    resultsEl.innerHTML = `
      <div class="text-red-400 text-sm p-2">
        Query failed: ${escapeHtml(err instanceof Error ? err.message : String(err))}
      </div>
    `;
    resultsEl.hidden = false;
  } finally {
    setButtonLoading(false);
  }
}

/** Render query results */
function renderResults(response: PathsToDaResponse): void {
  if (!resultsEl) return;

  if (response.count === 0) {
    resultsEl.innerHTML = `
      <div class="text-gray-400 text-sm p-2">
        No users found with paths to Domain Admins
      </div>
    `;
    resultsEl.hidden = false;
    return;
  }

  // Group by hop count
  const byHops = new Map<number, typeof response.entries>();
  for (const entry of response.entries) {
    const existing = byHops.get(entry.hops) || [];
    existing.push(entry);
    byHops.set(entry.hops, existing);
  }

  // Sort by hop count
  const sortedHops = Array.from(byHops.keys()).sort((a, b) => a - b);

  let html = `
    <div class="paths-to-da-summary">
      <span class="text-sm font-medium text-green-400">${response.count} users</span>
      <span class="text-xs text-gray-400">with paths to Domain Admins</span>
    </div>
    <div class="paths-to-da-list">
  `;

  for (const hops of sortedHops) {
    const entries = byHops.get(hops)!;
    html += `
      <div class="paths-hop-group">
        <div class="paths-hop-header">
          <span class="paths-hop-count">${hops} hop${hops !== 1 ? "s" : ""}</span>
          <span class="paths-hop-badge">${entries.length}</span>
        </div>
        <div class="paths-hop-entries">
    `;

    for (const entry of entries) {
      html += `
        <div
          class="paths-to-da-entry"
          data-node-id="${escapeHtml(entry.id)}"
          data-action="select-node"
          title="Click to select, double-click to find path"
        >
          <span class="node-badge ${entry.type.toLowerCase()}">${escapeHtml(entry.type)}</span>
          <span class="paths-entry-label">${escapeHtml(entry.label)}</span>
        </div>
      `;
    }

    html += `
        </div>
      </div>
    `;
  }

  html += "</div>";
  resultsEl.innerHTML = html;
  resultsEl.hidden = false;

  // Add click handlers for entries
  resultsEl.querySelectorAll("[data-action='select-node']").forEach((el) => {
    el.addEventListener("click", handleEntryClick);
    el.addEventListener("dblclick", handleEntryDoubleClick);
  });
}

/** Handle click on a result entry (select node) */
function handleEntryClick(e: Event): void {
  const target = e.currentTarget as HTMLElement;
  const nodeId = target.getAttribute("data-node-id");
  if (!nodeId) return;

  const renderer = getRenderer();
  if (renderer) {
    renderer.selectNode(nodeId);
    renderer.focusNode(nodeId);
  }
}

/** Handle double-click on a result entry (find path to DA) */
async function handleEntryDoubleClick(e: Event): Promise<void> {
  e.preventDefault();
  const target = e.currentTarget as HTMLElement;
  const nodeId = target.getAttribute("data-node-id");
  if (!nodeId) return;

  // Fill in path start and trigger path finding
  const pathStartInput = document.getElementById("path-start") as HTMLInputElement;
  const pathEndInput = document.getElementById("path-end") as HTMLInputElement;
  const findPathBtn = document.getElementById("find-path-btn") as HTMLButtonElement;

  if (pathStartInput && pathEndInput && findPathBtn) {
    pathStartInput.value = nodeId;
    pathEndInput.value = "Domain Admins";
    findPathBtn.click();
  }
}
