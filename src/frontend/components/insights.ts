/**
 * Security Insights Component
 *
 * Modal for viewing security insights with three tabs:
 * - Domain Admin Analysis
 * - Reachability
 * - Stale Objects
 *
 * Each tab loads independently with parallel query execution.
 * Clicking on count values opens a graph visualization.
 */

import { escapeHtml } from "../utils/html";
import { executeQuery, getQueryErrorMessage } from "../utils/query";
import { loadGraphData } from "./graph-view";
import type { RawADGraph } from "../graph/types";

/** Tab identifiers */
type TabId = "da-analysis" | "reachability" | "stale-objects" | "choke-points";

/** Domain Admin Analysis data */
interface DAAnalysisData {
  effectiveCount: number;
  realCount: number;
  ratio: number;
}

/** Reachability data for a principal */
interface ReachabilityData {
  principalName: string;
  principalSid: string;
  count: number;
}

/** Stale Objects data */
interface StaleObjectsData {
  users: number;
  computers: number;
  thresholdDays: number;
}

/** Choke Point data */
interface ChokePointData {
  source_id: string;
  source_name: string;
  source_label: string;
  target_id: string;
  target_name: string;
  target_label: string;
  edge_type: string;
  betweenness: number;
}

/** Choke Points response */
interface ChokePointsData {
  choke_points: ChokePointData[];
  total_edges: number;
  total_nodes: number;
}

/** Tab state */
interface TabState<T> {
  loading: boolean;
  error: string | null;
  data: T | null;
}

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Active tab */
let activeTab: TabId = "da-analysis";

/** Tab states */
let daState: TabState<DAAnalysisData> = { loading: false, error: null, data: null };
let reachabilityState: TabState<ReachabilityData[]> = { loading: false, error: null, data: null };
let staleState: TabState<StaleObjectsData> = { loading: false, error: null, data: null };
let chokePointsState: TabState<ChokePointsData> = { loading: false, error: null, data: null };

/** Stale threshold in days */
let staleThresholdDays = 90;

/** Initialize the insights modal */
export function initInsights(): void {
  // Modal is created on demand
}

/** Open the insights modal */
export async function openInsights(): Promise<void> {
  if (!modalEl) {
    createModal();
  }

  // Reset states
  daState = { loading: true, error: null, data: null };
  reachabilityState = { loading: true, error: null, data: null };
  staleState = { loading: true, error: null, data: null };
  chokePointsState = { loading: true, error: null, data: null };
  activeTab = "da-analysis";

  modalEl!.hidden = false;
  renderModal();

  // Load all tabs in parallel
  loadDAAnalysis();
  loadReachability();
  loadStaleObjects();
  loadChokePoints();
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
  modalEl.id = "insights-modal";
  modalEl.className = "modal-overlay";
  modalEl.innerHTML = `
    <div class="modal-content modal-lg">
      <div class="modal-header">
        <h2 class="modal-title">Security Insights</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body" id="insights-body"></div>
      <div class="modal-footer">
        <button class="btn btn-secondary" data-action="refresh">Refresh</button>
        <button class="btn btn-primary" data-action="close">Close</button>
      </div>
    </div>
  `;

  modalEl.addEventListener("click", handleClick);
  modalEl.addEventListener("change", handleChange);
  document.body.appendChild(modalEl);

  // Close on Escape
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && modalEl && !modalEl.hidden) {
      closeModal();
    }
  });
}

/** Render the modal content */
function renderModal(): void {
  const body = document.getElementById("insights-body");
  if (!body) return;

  body.innerHTML = `
    <div class="db-type-tabs">
      <button class="db-type-tab ${activeTab === "da-analysis" ? "active" : ""}" data-tab="da-analysis">
        Domain Admin Analysis
      </button>
      <button class="db-type-tab ${activeTab === "reachability" ? "active" : ""}" data-tab="reachability">
        Reachability
      </button>
      <button class="db-type-tab ${activeTab === "stale-objects" ? "active" : ""}" data-tab="stale-objects">
        Stale Objects
      </button>
      <button class="db-type-tab ${activeTab === "choke-points" ? "active" : ""}" data-tab="choke-points">
        Choke Points
      </button>
    </div>
    <div class="insight-tab-content" ${activeTab !== "da-analysis" ? "hidden" : ""} id="tab-da-analysis">
      ${renderDAAnalysisTab()}
    </div>
    <div class="insight-tab-content" ${activeTab !== "reachability" ? "hidden" : ""} id="tab-reachability">
      ${renderReachabilityTab()}
    </div>
    <div class="insight-tab-content" ${activeTab !== "stale-objects" ? "hidden" : ""} id="tab-stale-objects">
      ${renderStaleObjectsTab()}
    </div>
    <div class="insight-tab-content" ${activeTab !== "choke-points" ? "hidden" : ""} id="tab-choke-points">
      ${renderChokePointsTab()}
    </div>
  `;
}

/** Render Domain Admin Analysis tab */
function renderDAAnalysisTab(): string {
  if (daState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Analyzing domain admins...</span></div>`;
  }
  if (daState.error) {
    return `<div class="insight-error">${escapeHtml(daState.error)}</div>`;
  }
  if (!daState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const { effectiveCount, realCount, ratio } = daState.data;

  return `
    <div class="insights-container">
      <div class="insight-section">
        <h3 class="insight-section-title">Domain Admin Analysis</h3>
        <p class="insight-desc">Compare users with any path to Domain Admins vs direct/transitive group members.</p>
        <div class="insight-cards">
          <div class="insight-card insight-card-primary">
            <div class="insight-card-value clickable" data-query="effective-das" title="Click to view graph">${effectiveCount.toLocaleString()}</div>
            <div class="insight-card-label">Effective Domain Admins</div>
            <div class="insight-card-desc">Users with any path to DA</div>
          </div>
          <div class="insight-card insight-card-secondary">
            <div class="insight-card-value clickable" data-query="real-das" title="Click to view graph">${realCount.toLocaleString()}</div>
            <div class="insight-card-label">Real Domain Admins</div>
            <div class="insight-card-desc">Direct/transitive members</div>
          </div>
          <div class="insight-card">
            <div class="insight-card-value">${ratio.toFixed(1)}x</div>
            <div class="insight-card-label">Privilege Expansion</div>
            <div class="insight-card-desc">Effective vs Real ratio</div>
          </div>
        </div>
        <p class="text-xs text-gray-500 mt-2">Click on a number to visualize the graph</p>
      </div>
    </div>
  `;
}

/** Render Reachability tab */
function renderReachabilityTab(): string {
  if (reachabilityState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Analyzing reachability...</span></div>`;
  }
  if (reachabilityState.error) {
    return `<div class="insight-error">${escapeHtml(reachabilityState.error)}</div>`;
  }
  if (!reachabilityState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const principals = reachabilityState.data;
  if (principals.length === 0) {
    return `<div class="insights-container"><div class="insight-section">
      <h3 class="insight-section-title">Reachability from Well-Known Principals</h3>
      <p class="text-gray-500">No well-known principals found in the graph.</p>
    </div></div>`;
  }

  let rowsHtml = "";
  for (const p of principals) {
    const hasData = p.count >= 0;
    rowsHtml += `
      <div class="insight-row">
        <span class="insight-label">${escapeHtml(p.principalName)}</span>
        <span class="insight-value ${hasData && p.count > 0 ? "clickable" : ""} ${!hasData ? "text-gray-500" : ""}"
              ${hasData && p.count > 0 ? `data-query="reachability" data-sid="${escapeHtml(p.principalSid)}" title="Click to view graph"` : ""}>
          ${hasData ? p.count.toLocaleString() : "Not found"}
        </span>
      </div>
    `;
  }

  return `
    <div class="insights-container">
      <div class="insight-section">
        <h3 class="insight-section-title">Reachability from Well-Known Principals</h3>
        <p class="insight-desc">Objects reachable via non-MemberOf edges (control paths, permissions, etc.):</p>
        <div class="insight-stats">
          ${rowsHtml}
        </div>
        <p class="text-xs text-gray-500 mt-3">Click on a count to view the reachable objects</p>
      </div>
    </div>
  `;
}

/** Render Stale Objects tab */
function renderStaleObjectsTab(): string {
  if (staleState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Finding stale objects...</span></div>`;
  }
  if (staleState.error) {
    return `<div class="insight-error">${escapeHtml(staleState.error)}</div>`;
  }
  if (!staleState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const { users, computers, thresholdDays } = staleState.data;

  return `
    <div class="insights-container">
      <div class="insight-section">
        <h3 class="insight-section-title">Stale Objects</h3>
        <div class="insight-desc" style="display: flex; align-items: center; gap: 8px;">
          <span>Enabled objects with lastlogon older than</span>
          <select class="stale-threshold-select" data-action="change-threshold">
            <option value="30" ${thresholdDays === 30 ? "selected" : ""}>30 days</option>
            <option value="60" ${thresholdDays === 60 ? "selected" : ""}>60 days</option>
            <option value="90" ${thresholdDays === 90 ? "selected" : ""}>90 days</option>
            <option value="180" ${thresholdDays === 180 ? "selected" : ""}>180 days</option>
          </select>
        </div>
        <div class="insight-stats" style="margin-top: 1rem;">
          <div class="insight-row">
            <span class="insight-label">Stale Users</span>
            <span class="insight-value ${users > 0 ? "clickable" : ""}"
                  ${users > 0 ? 'data-query="stale-users" title="Click to view graph"' : ""}>
              ${users.toLocaleString()}
            </span>
          </div>
          <div class="insight-row">
            <span class="insight-label">Stale Computers</span>
            <span class="insight-value ${computers > 0 ? "clickable" : ""}"
                  ${computers > 0 ? 'data-query="stale-computers" title="Click to view graph"' : ""}>
              ${computers.toLocaleString()}
            </span>
          </div>
        </div>
        <p class="text-xs text-gray-500 mt-3">Click on a count to view the objects in the graph</p>
      </div>
    </div>
  `;
}

/** Render Choke Points tab */
function renderChokePointsTab(): string {
  if (chokePointsState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Analyzing choke points...</span></div>`;
  }
  if (chokePointsState.error) {
    return `<div class="insight-error">${escapeHtml(chokePointsState.error)}</div>`;
  }
  if (!chokePointsState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const { choke_points, total_edges, total_nodes } = chokePointsState.data;

  if (choke_points.length === 0) {
    return `
      <div class="insights-container">
        <div class="insight-section">
          <h3 class="insight-section-title">Choke Points</h3>
          <p class="text-gray-500">No choke points found. The graph may be too small or disconnected.</p>
        </div>
      </div>
    `;
  }

  // Find max betweenness for normalization
  const maxBetweenness = Math.max(...choke_points.map((cp) => cp.betweenness));

  let rowsHtml = "";
  for (const [i, cp] of choke_points.entries()) {
    const normalizedScore = maxBetweenness > 0 ? (cp.betweenness / maxBetweenness) * 100 : 0;
    const barWidth = Math.max(normalizedScore, 5); // Minimum 5% for visibility

    rowsHtml += `
      <div class="choke-point-row" data-query="choke-point" data-index="${i}" title="Click to view in graph">
        <div class="choke-point-rank">#${i + 1}</div>
        <div class="choke-point-details">
          <div class="choke-point-path">
            <span class="choke-point-node">${escapeHtml(cp.source_name)}</span>
            <span class="choke-point-edge">&rarr; ${escapeHtml(cp.edge_type)} &rarr;</span>
            <span class="choke-point-node">${escapeHtml(cp.target_name)}</span>
          </div>
          <div class="choke-point-meta">
            <span class="choke-point-labels">${escapeHtml(cp.source_label)} to ${escapeHtml(cp.target_label)}</span>
          </div>
        </div>
        <div class="choke-point-score">
          <div class="choke-point-bar" style="width: ${barWidth}%"></div>
          <span class="choke-point-value">${cp.betweenness.toFixed(1)}</span>
        </div>
      </div>
    `;
  }

  return `
    <div class="insights-container">
      <div class="insight-section">
        <h3 class="insight-section-title">Choke Points</h3>
        <p class="insight-desc">
          Edges with the highest betweenness centrality - removing these would disrupt the most attack paths.
          Analyzed ${total_nodes.toLocaleString()} nodes and ${total_edges.toLocaleString()} edges.
        </p>
        <div class="choke-points-list">
          ${rowsHtml}
        </div>
        <p class="text-xs text-gray-500 mt-3">Click on a row to view the edge in the graph. Higher scores indicate more paths pass through this edge.</p>
      </div>
    </div>
  `;
}

/** Load Domain Admin Analysis data */
async function loadDAAnalysis(): Promise<void> {
  daState = { loading: true, error: null, data: null };
  renderModal();

  try {
    // Run both queries in parallel - return distinct users to get accurate count
    const [effectiveResult, realResult] = await Promise.all([
      executeQuery(`MATCH (u:User)-[r*1..10]->(g:Group) WHERE g.object_id ENDS WITH '-512' RETURN DISTINCT u`, {
        extractGraph: false,
        background: true,
      }),
      executeQuery(`MATCH (u:User)-[:MemberOf*1..10]->(g:Group) WHERE g.object_id ENDS WITH '-512' RETURN DISTINCT u`, {
        extractGraph: false,
        background: true,
      }),
    ]);

    const effectiveCount = effectiveResult.resultCount;
    const realCount = realResult.resultCount;
    const ratio = realCount > 0 ? effectiveCount / realCount : effectiveCount > 0 ? Infinity : 1;

    daState = {
      loading: false,
      error: null,
      data: { effectiveCount, realCount, ratio },
    };
  } catch (err) {
    daState = { loading: false, error: getQueryErrorMessage(err), data: null };
  }

  renderModal();
}

/** Load Reachability data */
async function loadReachability(): Promise<void> {
  reachabilityState = { loading: true, error: null, data: null };
  renderModal();

  // Well-known principal SIDs (relative IDs)
  const principals = [
    { name: "Domain Users", sid: "-513" },
    { name: "Domain Computers", sid: "-515" },
    { name: "Authenticated Users", sid: "-S-1-5-11" },
    { name: "Everyone", sid: "-S-1-1-0" },
  ];

  try {
    const results: ReachabilityData[] = [];

    // Run all reachability queries in parallel
    const queries = principals.map(async (p) => {
      try {
        // Use NONE() to exclude MemberOf edges from the path
        const query = `
          MATCH (g:Group)-[r*1..5]->(target)
          WHERE g.object_id ENDS WITH '${p.sid}'
          AND NONE(rel IN r WHERE type(rel) = 'MemberOf')
          RETURN DISTINCT target
        `;
        const result = await executeQuery(query, { extractGraph: false, background: true });
        return { principalName: p.name, principalSid: p.sid, count: result.resultCount };
      } catch {
        // Principal might not exist in the graph
        return { principalName: p.name, principalSid: p.sid, count: -1 };
      }
    });

    const allResults = await Promise.all(queries);
    results.push(...allResults);

    reachabilityState = { loading: false, error: null, data: results };
  } catch (err) {
    reachabilityState = { loading: false, error: getQueryErrorMessage(err), data: null };
  }

  renderModal();
}

/** Convert days to Windows FileTime threshold */
function daysToWindowsFileTime(days: number): number {
  const now = Date.now();
  const thresholdMs = now - days * 24 * 60 * 60 * 1000;
  // Windows FileTime is 100-nanosecond intervals since Jan 1, 1601
  // Unix epoch is Jan 1, 1970 - difference is 11644473600 seconds
  const FILETIME_UNIX_DIFF = 116444736000000000n;
  const fileTime = BigInt(thresholdMs) * 10000n + FILETIME_UNIX_DIFF;
  return Number(fileTime);
}

/** Load Stale Objects data */
async function loadStaleObjects(): Promise<void> {
  staleState = { loading: true, error: null, data: null };
  renderModal();

  try {
    const threshold = daysToWindowsFileTime(staleThresholdDays);

    // Run both queries in parallel
    const [usersResult, computersResult] = await Promise.all([
      executeQuery(`MATCH (u:User) WHERE u.enabled = true AND u.lastlogon < ${threshold} RETURN u`, {
        extractGraph: false,
        background: true,
      }),
      executeQuery(`MATCH (c:Computer) WHERE c.enabled = true AND c.lastlogon < ${threshold} RETURN c`, {
        extractGraph: false,
        background: true,
      }),
    ]);

    staleState = {
      loading: false,
      error: null,
      data: {
        users: usersResult.resultCount,
        computers: computersResult.resultCount,
        thresholdDays: staleThresholdDays,
      },
    };
  } catch (err) {
    staleState = { loading: false, error: getQueryErrorMessage(err), data: null };
  }

  renderModal();
}

/** Load Choke Points data */
async function loadChokePoints(): Promise<void> {
  chokePointsState = { loading: true, error: null, data: null };
  renderModal();

  try {
    const response = await fetch("/api/graph/choke-points");
    if (!response.ok) {
      const errorData = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(errorData.message || `HTTP ${response.status}`);
    }
    const data = (await response.json()) as ChokePointsData;
    chokePointsState = { loading: false, error: null, data };
  } catch (err) {
    const message = err instanceof Error ? err.message : "Failed to load choke points";
    chokePointsState = { loading: false, error: message, data: null };
  }

  renderModal();
}

/** Execute a graph query and render the result */
async function executeGraphQuery(queryType: string, extraData?: string): Promise<void> {
  let query: string;

  switch (queryType) {
    case "effective-das":
      query = `MATCH p=(u:User)-[r*1..10]->(g:Group) WHERE g.object_id ENDS WITH '-512' RETURN p LIMIT 500`;
      break;
    case "real-das":
      query = `MATCH p=(u:User)-[:MemberOf*1..10]->(g:Group) WHERE g.object_id ENDS WITH '-512' RETURN p LIMIT 500`;
      break;
    case "reachability":
      query = `
        MATCH p=(g:Group)-[r*1..5]->(target)
        WHERE g.object_id ENDS WITH '${extraData}'
        AND NONE(rel IN r WHERE type(rel) = 'MemberOf')
        RETURN p LIMIT 500
      `;
      break;
    case "stale-users": {
      const threshold = daysToWindowsFileTime(staleThresholdDays);
      query = `MATCH (u:User) WHERE u.enabled = true AND u.lastlogon < ${threshold} RETURN u LIMIT 500`;
      break;
    }
    case "stale-computers": {
      const threshold = daysToWindowsFileTime(staleThresholdDays);
      query = `MATCH (c:Computer) WHERE c.enabled = true AND c.lastlogon < ${threshold} RETURN c LIMIT 500`;
      break;
    }
    case "choke-point": {
      // extraData contains the index into choke_points array
      const index = parseInt(extraData ?? "0", 10);
      const cp = chokePointsState.data?.choke_points[index];
      if (!cp) return;
      // Query for the edge and its connected nodes
      query = `MATCH p=(a)-[r]->(b) WHERE a.object_id = '${cp.source_id}' AND b.object_id = '${cp.target_id}' AND type(r) = '${cp.edge_type}' RETURN p`;
      break;
    }
    default:
      return;
  }

  closeModal();

  try {
    const result = await executeQuery(query, { extractGraph: true });
    if (result.graph && result.graph.nodes.length > 0) {
      loadGraphData(result.graph as unknown as RawADGraph);
    } else {
      // For single-node queries like stale objects, build a simple graph
      // The backend should have extracted the nodes from the query result
      const emptyGraph: RawADGraph = { nodes: [], edges: [] };
      loadGraphData(emptyGraph);
    }
  } catch (err) {
    console.error("Failed to execute graph query:", err);
  }
}

/** Handle click events */
function handleClick(e: Event): void {
  const target = e.target as HTMLElement;

  // Close on backdrop click
  if (target.classList.contains("modal-overlay")) {
    closeModal();
    return;
  }

  // Tab switching
  const tabBtn = target.closest("[data-tab]") as HTMLElement;
  if (tabBtn) {
    const tabId = tabBtn.getAttribute("data-tab") as TabId;
    if (tabId && tabId !== activeTab) {
      activeTab = tabId;
      renderModal();
    }
    return;
  }

  // Clickable values (graph queries)
  const clickableValue = target.closest("[data-query]") as HTMLElement;
  if (clickableValue) {
    const queryType = clickableValue.getAttribute("data-query");
    const sid = clickableValue.getAttribute("data-sid");
    const index = clickableValue.getAttribute("data-index");
    if (queryType) {
      executeGraphQuery(queryType, sid ?? index ?? undefined);
    }
    return;
  }

  // Action buttons
  const actionEl = target.closest("[data-action]") as HTMLElement;
  if (!actionEl) return;

  const action = actionEl.getAttribute("data-action");

  switch (action) {
    case "close":
      closeModal();
      break;
    case "refresh":
      // Reload all tabs
      loadDAAnalysis();
      loadReachability();
      loadStaleObjects();
      loadChokePoints();
      break;
  }
}

/** Handle change events (for select elements) */
function handleChange(e: Event): void {
  const target = e.target as HTMLElement;

  // Stale threshold change
  const thresholdSelect = target.closest("[data-action='change-threshold']") as HTMLSelectElement;
  if (thresholdSelect) {
    const newThreshold = parseInt(thresholdSelect.value, 10);
    if (newThreshold !== staleThresholdDays) {
      staleThresholdDays = newThreshold;
      loadStaleObjects();
    }
  }
}
