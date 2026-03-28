/**
 * Security Insights Component
 *
 * Modal for viewing security insights with tabs:
 * - Domain Admin Analysis
 * - Reachability
 * - Stale Objects
 * - Account Exposure
 * - Choke Points / Unexpected Choke Points
 *
 * Each tab loads independently with parallel query execution.
 * Clicking on count values opens a graph visualization.
 */

import { escapeHtml } from "../utils/html";
import { executeQuery, getQueryErrorMessage, QueryAbortedError } from "../utils/query";
import { loadGraphData } from "./graph-view";
import { api } from "../api/client";
import type { RawADGraph } from "../graph/types";

/** Tab identifiers */
type TabId =
  | "da-analysis"
  | "reachability"
  | "stale-objects"
  | "account-exposure"
  | "choke-points"
  | "unexpected-choke-points"
  | "tier-violations";

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

/** Account Exposure data */
interface AccountExposureData {
  kerberoastable: number;
  asrepRoastable: number;
  unconstrainedDelegation: number;
  protectedUsers: number;
}

/** Stale Objects data */
interface StaleObjectsData {
  users: number;
  computers: number;
  thresholdDays: number;
}

/** A single tier violation edge */
interface TierViolationEdge {
  source_id: string;
  target_id: string;
  rel_type: string;
}

/** A single tier violation category */
interface TierViolationCategory {
  source_zone: number;
  target_zone: number;
  count: number;
  edges: TierViolationEdge[];
}

/** Tier Violations response from API */
interface TierViolationsData {
  violations: TierViolationCategory[];
  total_nodes: number;
  total_edges: number;
}

/** Choke Point data */
interface ChokePointData {
  source_id: string;
  source_name: string;
  source_label: string;
  target_id: string;
  target_name: string;
  target_label: string;
  rel_type: string;
  betweenness: number;
  source_tier: number;
}

/** Choke Points response */
interface ChokePointsData {
  choke_points: ChokePointData[];
  unexpected_choke_points: ChokePointData[];
  total_edges: number;
  total_nodes: number;
}

/** Tab state */
interface TabState<T> {
  loading: boolean;
  error: string | null;
  data: T | null;
}

/** Choke points pagination */
const CHOKE_POINTS_PAGE_SIZE = 10;

/** Encapsulated mutable state for the insights modal */
interface InsightsState {
  chokePointsPage: number;
  unexpectedChokePointsPage: number;
  modalExpanded: boolean;
  activeTab: TabId;
  daState: TabState<DAAnalysisData>;
  reachabilityState: TabState<ReachabilityData[]>;
  staleState: TabState<StaleObjectsData>;
  accountExposureState: TabState<AccountExposureData>;
  chokePointsState: TabState<ChokePointsData>;
  tierViolationsState: TabState<TierViolationsData>;
  staleThresholdDays: number;
  computingEffectiveTiers: boolean;
  effectiveTiersResult: { computed: number; violations: number } | null;
}

function createInitialInsightsState(): InsightsState {
  return {
    chokePointsPage: 0,
    unexpectedChokePointsPage: 0,
    modalExpanded: false,
    activeTab: "da-analysis",
    daState: { loading: false, error: null, data: null },
    reachabilityState: { loading: false, error: null, data: null },
    staleState: { loading: false, error: null, data: null },
    accountExposureState: { loading: false, error: null, data: null },
    chokePointsState: { loading: false, error: null, data: null },
    tierViolationsState: { loading: false, error: null, data: null },
    staleThresholdDays: 90,
    computingEffectiveTiers: false,
    effectiveTiersResult: null,
  };
}

let state = createInitialInsightsState();

/** Reset all mutable state (called on modal close) */
function resetState(): void {
  state = createInitialInsightsState();
}

/** Modal element (DOM reference, not reset with state) */
let modalEl: HTMLElement | null = null;

/** Open the insights modal */
export async function openInsights(): Promise<void> {
  if (!modalEl) {
    createModal();
  }

  // Reset states
  state.daState = { loading: true, error: null, data: null };
  state.reachabilityState = { loading: true, error: null, data: null };
  state.staleState = { loading: true, error: null, data: null };
  state.accountExposureState = { loading: true, error: null, data: null };
  state.chokePointsState = { loading: true, error: null, data: null };
  state.tierViolationsState = { loading: true, error: null, data: null };
  state.chokePointsPage = 0;
  state.unexpectedChokePointsPage = 0;
  state.modalExpanded = false;
  state.activeTab = "da-analysis";

  modalEl!.hidden = false;
  updateModalExpanded();
  renderModal();

  // Load all tabs in parallel
  loadDAAnalysis();
  loadReachability();
  loadStaleObjects();
  loadAccountExposure();
  loadChokePoints();
  loadTierViolations();
}

/** Close the modal */
function closeModal(): void {
  if (modalEl) {
    modalEl.hidden = true;
  }
  resetState();
}

/** Update modal expanded/collapsed state */
function updateModalExpanded(): void {
  if (!modalEl) return;
  const content = modalEl.querySelector(".modal-content") as HTMLElement;
  if (!content) return;

  content.classList.toggle("modal-expanded", state.modalExpanded);

  const expandIcon = modalEl.querySelector(".expand-icon") as HTMLElement;
  const collapseIcon = modalEl.querySelector(".collapse-icon") as HTMLElement;
  if (expandIcon && collapseIcon) {
    expandIcon.style.display = state.modalExpanded ? "none" : "";
    collapseIcon.style.display = state.modalExpanded ? "" : "none";
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
        <div class="modal-header-actions">
          <button class="modal-close" data-action="toggle-expand" aria-label="Expand" id="insights-expand-btn">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="expand-icon">
              <path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7"/>
            </svg>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="collapse-icon" style="display:none">
              <path d="M4 14h6v6M20 10h-6V4M14 10l7-7M3 21l7-7"/>
            </svg>
          </button>
          <button class="modal-close" data-action="close" aria-label="Close">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M18 6L6 18M6 6l12 12"/>
            </svg>
          </button>
        </div>
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
}

/** Render the modal content */
function renderModal(): void {
  const body = document.getElementById("insights-body");
  if (!body) return;

  body.innerHTML = `
    <div class="db-type-tabs">
      <button class="db-type-tab ${state.activeTab === "da-analysis" ? "active" : ""}" data-tab="da-analysis">
        Domain Admin Analysis
      </button>
      <button class="db-type-tab ${state.activeTab === "reachability" ? "active" : ""}" data-tab="reachability">
        Reachability
      </button>
      <button class="db-type-tab ${state.activeTab === "stale-objects" ? "active" : ""}" data-tab="stale-objects">
        Stale Objects
      </button>
      <button class="db-type-tab ${state.activeTab === "account-exposure" ? "active" : ""}" data-tab="account-exposure">
        Account Exposure
      </button>
      <button class="db-type-tab ${state.activeTab === "choke-points" ? "active" : ""}" data-tab="choke-points">
        Choke Points
      </button>
      <button class="db-type-tab ${state.activeTab === "unexpected-choke-points" ? "active" : ""}" data-tab="unexpected-choke-points">
        Unexpected Choke Points
      </button>
      <button class="db-type-tab ${state.activeTab === "tier-violations" ? "active" : ""}" data-tab="tier-violations">
        Tier Violations
      </button>
    </div>
    <div class="insight-tab-content" ${state.activeTab !== "da-analysis" ? "hidden" : ""} id="tab-da-analysis">
      ${renderDAAnalysisTab()}
    </div>
    <div class="insight-tab-content" ${state.activeTab !== "reachability" ? "hidden" : ""} id="tab-reachability">
      ${renderReachabilityTab()}
    </div>
    <div class="insight-tab-content" ${state.activeTab !== "stale-objects" ? "hidden" : ""} id="tab-stale-objects">
      ${renderStaleObjectsTab()}
    </div>
    <div class="insight-tab-content" ${state.activeTab !== "account-exposure" ? "hidden" : ""} id="tab-account-exposure">
      ${renderAccountExposureTab()}
    </div>
    <div class="insight-tab-content" ${state.activeTab !== "choke-points" ? "hidden" : ""} id="tab-choke-points">
      ${renderChokePointsTab()}
    </div>
    <div class="insight-tab-content" ${state.activeTab !== "unexpected-choke-points" ? "hidden" : ""} id="tab-unexpected-choke-points">
      ${renderUnexpectedChokePointsTab()}
    </div>
    <div class="insight-tab-content" ${state.activeTab !== "tier-violations" ? "hidden" : ""} id="tab-tier-violations">
      ${renderTierViolationsTab()}
    </div>
  `;
}

/** Render Domain Admin Analysis tab */
function renderDAAnalysisTab(): string {
  if (state.daState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Analyzing domain admins...</span></div>`;
  }
  if (state.daState.error) {
    return `<div class="insight-error">${escapeHtml(state.daState.error)}</div>`;
  }
  if (!state.daState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const { effectiveCount, realCount, ratio } = state.daState.data;

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
  if (state.reachabilityState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Analyzing reachability...</span></div>`;
  }
  if (state.reachabilityState.error) {
    return `<div class="insight-error">${escapeHtml(state.reachabilityState.error)}</div>`;
  }
  if (!state.reachabilityState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const principals = state.reachabilityState.data;
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
        <p class="insight-desc">Objects with direct non-MemberOf relationships (permissions, sessions, delegation, etc.):</p>
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
  if (state.staleState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Finding stale objects...</span></div>`;
  }
  if (state.staleState.error) {
    return `<div class="insight-error">${escapeHtml(state.staleState.error)}</div>`;
  }
  if (!state.staleState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const { users, computers, thresholdDays } = state.staleState.data;

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

/** Render Account Exposure tab */
function renderAccountExposureTab(): string {
  if (state.accountExposureState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Analyzing account exposure...</span></div>`;
  }
  if (state.accountExposureState.error) {
    return `<div class="insight-error">${escapeHtml(state.accountExposureState.error)}</div>`;
  }
  if (!state.accountExposureState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const { kerberoastable, asrepRoastable, unconstrainedDelegation, protectedUsers } = state.accountExposureState.data;

  function row(label: string, count: number, queryType: string): string {
    return `
      <div class="insight-row">
        <span class="insight-label">${escapeHtml(label)}</span>
        <span class="insight-value ${count > 0 ? "clickable" : ""}"
              ${count > 0 ? `data-query="${queryType}" title="Click to view graph"` : ""}>
          ${count.toLocaleString()}
        </span>
      </div>
    `;
  }

  return `
    <div class="insights-container">
      <div class="insight-section">
        <h3 class="insight-section-title">Account Exposure</h3>
        <p class="insight-desc">Enabled accounts and computers with risky configurations.</p>
        <div class="insight-stats" style="margin-top: 1rem;">
          ${row("Kerberoastable Users", kerberoastable, "kerberoastable")}
          ${row("AS-REP Roastable Users", asrepRoastable, "asrep-roastable")}
          ${row("Unconstrained Delegation", unconstrainedDelegation, "unconstrained-delegation")}
          ${row("Protected Users Members", protectedUsers, "protected-users")}
        </div>
        <p class="text-xs text-gray-500 mt-3">Click on a count to view the objects in the graph</p>
      </div>
    </div>
  `;
}

/** Render a paginated choke points table */
function renderChokePointsTable(opts: {
  items: ChokePointData[];
  page: number;
  prevAction: string;
  nextAction: string;
}): string {
  const { items, page, prevAction, nextAction } = opts;
  const totalPages = Math.ceil(items.length / CHOKE_POINTS_PAGE_SIZE);
  const clampedPage = Math.min(page, Math.max(totalPages - 1, 0));
  const startIdx = clampedPage * CHOKE_POINTS_PAGE_SIZE;
  const pageItems = items.slice(startIdx, startIdx + CHOKE_POINTS_PAGE_SIZE);

  const maxBetweenness = Math.max(...items.map((cp) => cp.betweenness), 0);

  let rowsHtml = "";
  for (const [pageIdx, cp] of pageItems.entries()) {
    const displayRank = startIdx + pageIdx + 1;
    const normalizedScore = maxBetweenness > 0 ? (cp.betweenness / maxBetweenness) * 100 : 0;
    const barWidth = Math.max(normalizedScore, 5);

    rowsHtml += `
      <tr class="choke-point-tr" data-query="choke-point"
          data-source-id="${escapeHtml(cp.source_id)}"
          data-target-id="${escapeHtml(cp.target_id)}"
          data-rel-type="${escapeHtml(cp.rel_type)}"
          title="Click to view in graph">
        <td class="choke-point-cell-rank">${displayRank}</td>
        <td class="choke-point-cell-score">
          <div class="choke-point-bar-container">
            <div class="choke-point-bar" style="width: ${barWidth}%"></div>
          </div>
          <span class="choke-point-value">${cp.betweenness.toFixed(1)}</span>
        </td>
        <td class="choke-point-cell-source">${escapeHtml(cp.source_name)}<span class="choke-point-type-label">${escapeHtml(cp.source_label)}</span></td>
        <td class="choke-point-cell-rel">${escapeHtml(cp.rel_type)}</td>
        <td class="choke-point-cell-target">${escapeHtml(cp.target_name)}<span class="choke-point-type-label">${escapeHtml(cp.target_label)}</span></td>
      </tr>
    `;
  }

  let paginationHtml = "";
  if (totalPages > 1) {
    paginationHtml = `
      <div class="choke-points-pagination">
        <button class="btn btn-sm btn-secondary" data-action="${prevAction}" ${clampedPage === 0 ? "disabled" : ""}>Prev</button>
        <span class="choke-points-page-info">Page ${clampedPage + 1} of ${totalPages}</span>
        <button class="btn btn-sm btn-secondary" data-action="${nextAction}" ${clampedPage >= totalPages - 1 ? "disabled" : ""}>Next</button>
      </div>
    `;
  }

  return `
    <div class="choke-points-table-wrap">
      <table class="choke-points-table">
        <thead>
          <tr>
            <th class="choke-th-rank">#</th>
            <th class="choke-th-score">Score</th>
            <th class="choke-th-source">Source</th>
            <th class="choke-th-rel">Relationship</th>
            <th class="choke-th-target">Target</th>
          </tr>
        </thead>
        <tbody>
          ${rowsHtml}
        </tbody>
      </table>
    </div>
    ${paginationHtml}
    <p class="text-xs text-gray-500 mt-2">Click a row to view the relationship in the graph.</p>
  `;
}

/** Render Choke Points tab */
function renderChokePointsTab(): string {
  if (state.chokePointsState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Analyzing choke points...</span></div>`;
  }
  if (state.chokePointsState.error) {
    return `<div class="insight-error">${escapeHtml(state.chokePointsState.error)}</div>`;
  }
  if (!state.chokePointsState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const { choke_points, total_edges, total_nodes } = state.chokePointsState.data;

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

  return `
    <div class="insights-container">
      <div class="insight-section">
        <h3 class="insight-section-title">Choke Points</h3>
        <p class="insight-desc">
          Relationships with the highest betweenness centrality &mdash; removing these would disrupt the most attack paths.
          Analyzed ${total_nodes.toLocaleString()} nodes and ${total_edges.toLocaleString()} relationships.
        </p>
        ${renderChokePointsTable({
          items: choke_points,
          page: state.chokePointsPage,
          prevAction: "choke-page-prev",
          nextAction: "choke-page-next",
        })}
      </div>
    </div>
  `;
}

/** Render Unexpected Choke Points tab */
function renderUnexpectedChokePointsTab(): string {
  if (state.chokePointsState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Analyzing choke points...</span></div>`;
  }
  if (state.chokePointsState.error) {
    return `<div class="insight-error">${escapeHtml(state.chokePointsState.error)}</div>`;
  }
  if (!state.chokePointsState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const { unexpected_choke_points, total_edges, total_nodes } = state.chokePointsState.data;

  if (unexpected_choke_points.length === 0) {
    return `
      <div class="insights-container">
        <div class="insight-section">
          <h3 class="insight-section-title">Unexpected Choke Points</h3>
          <p class="text-gray-500">No unexpected choke points found. All high-centrality relationships originate from tier-0 or domain objects.</p>
        </div>
      </div>
    `;
  }

  return `
    <div class="insights-container">
      <div class="insight-section">
        <h3 class="insight-section-title">Unexpected Choke Points</h3>
        <p class="insight-desc">
          Choke points where the source is neither a tier-0 target nor a domain object &mdash;
          these represent surprising attack paths from low-privilege entities.
          ${unexpected_choke_points.length} results
          (${total_nodes.toLocaleString()} nodes, ${total_edges.toLocaleString()} relationships analyzed).
        </p>
        ${renderChokePointsTable({
          items: unexpected_choke_points,
          page: state.unexpectedChokePointsPage,
          prevAction: "unexpected-choke-page-prev",
          nextAction: "unexpected-choke-page-next",
        })}
      </div>
    </div>
  `;
}

/** Render Tier Violations tab */
function renderTierViolationsTab(): string {
  if (state.tierViolationsState.loading) {
    return `<div class="insight-loading"><div class="spinner"></div><span>Analyzing tier violations...</span></div>`;
  }
  if (state.tierViolationsState.error) {
    return `<div class="insight-error">${escapeHtml(state.tierViolationsState.error)}</div>`;
  }
  if (!state.tierViolationsState.data) {
    return `<div class="insight-error">No data available</div>`;
  }

  const { violations, total_nodes, total_edges } = state.tierViolationsState.data;

  // Find each violation category (may be absent if backend returns fewer)
  const v1to0 = violations.find((v) => v.source_zone === 1 && v.target_zone === 0);
  const v2to1 = violations.find((v) => v.source_zone === 2 && v.target_zone === 1);
  const v3to2 = violations.find((v) => v.source_zone === 3 && v.target_zone === 2);

  const total = violations.reduce((sum, v) => sum + v.count, 0);

  const descriptions: Record<string, string> = {
    "1-0": "Server admin zone reaching domain admin zone",
    "2-1": "Workstation zone reaching server admin zone",
    "3-2": "Default zone reaching workstation zone",
  };

  const renderCard = (
    v: TierViolationCategory | undefined,
    srcZone: number,
    tgtZone: number,
    cardClass: string
  ): string => {
    const count = v?.count ?? 0;
    const key = `${srcZone}-${tgtZone}`;
    return `
      <div class="insight-card ${cardClass}">
        <div class="insight-card-value ${count > 0 ? "clickable" : ""}" ${count > 0 ? `data-query="tier-violation" data-sid="${key}" title="Click to view graph"` : ""}>${count.toLocaleString()}</div>
        <div class="insight-card-label">Zone ${srcZone} &rarr; Zone ${tgtZone}</div>
        <div class="insight-card-desc">${descriptions[key]}</div>
      </div>
    `;
  };

  const computeButton = state.computingEffectiveTiers
    ? `<button class="btn btn-sm btn-secondary" disabled><span class="spinner spinner-sm"></span> Computing...</button>`
    : `<button class="btn btn-sm btn-primary" data-action="compute-effective-tiers">Analyze Tier Violations</button>`;

  const computeResult = state.effectiveTiersResult
    ? `<div class="text-sm text-green-400 mt-2">Computed effective tiers for ${state.effectiveTiersResult.computed.toLocaleString()} nodes. Found ${state.effectiveTiersResult.violations.toLocaleString()} violation${state.effectiveTiersResult.violations === 1 ? "" : "s"}.</div>`
    : "";

  return `
    <div class="insights-container">
      <div class="insight-section">
        <h3 class="insight-section-title">Tier Violations</h3>
        <p class="insight-desc">
          Relationships crossing tier zone boundaries. Nodes are assigned to the
          most privileged zone they can reach (zone 0 = can reach tier-0 nodes, etc.).
          Edges from a lower-privilege zone to a higher-privilege zone are violations.
          ${total.toLocaleString()} total violation${total === 1 ? "" : "s"}
          (${total_nodes.toLocaleString()} nodes, ${total_edges.toLocaleString()} relationships analyzed).
        </p>
        <div class="insight-cards">
          ${renderCard(v1to0, 1, 0, "insight-card-primary")}
          ${renderCard(v2to1, 2, 1, "insight-card-secondary")}
          ${renderCard(v3to2, 3, 2, "")}
        </div>
        <div class="flex items-center gap-3 mt-3">
          ${computeButton}
          <span class="text-xs text-gray-500">Compute effective tiers via reverse BFS to update violation analysis</span>
        </div>
        ${computeResult}
        <p class="text-xs text-gray-500 mt-2">Click on a number to visualize the graph</p>
      </div>
    </div>
  `;
}

/** Compute effective tiers and reload violations */
async function computeEffectiveTiers(): Promise<void> {
  state.computingEffectiveTiers = true;
  state.effectiveTiersResult = null;
  renderModal();

  try {
    const result = await api.post<{ computed: number; violations: number }>("/api/graph/compute-effective-tiers", {});
    state.effectiveTiersResult = result;
    state.computingEffectiveTiers = false;
    renderModal();

    // Reload tier violations to reflect updated effective tiers
    await loadTierViolations();
  } catch (err) {
    state.computingEffectiveTiers = false;
    const message = err instanceof Error ? err.message : "Failed to compute effective tiers";
    state.effectiveTiersResult = null;
    state.tierViolationsState = { loading: false, error: message, data: state.tierViolationsState.data };
    renderModal();
  }
}

/** Load Domain Admin Analysis data */
async function loadDAAnalysis(): Promise<void> {
  state.daState = { loading: true, error: null, data: null };
  renderModal();

  try {
    // Run both queries in parallel using shortestPath to avoid combinatorial explosion
    const [effectiveResult, realResult] = await Promise.all([
      executeQuery(
        `MATCH (u:User), (g:Group), p = shortestPath((u)-[*1..10]->(g)) WHERE g.objectid ENDS WITH '-512' RETURN DISTINCT u`,
        { extractGraph: false, background: true }
      ),
      executeQuery(
        `MATCH (u:User), (g:Group), p = shortestPath((u)-[:MemberOf*1..10]->(g)) WHERE g.objectid ENDS WITH '-512' RETURN DISTINCT u`,
        { extractGraph: false, background: true }
      ),
    ]);

    const effectiveCount = effectiveResult.resultCount;
    const realCount = realResult.resultCount;
    const ratio = realCount > 0 ? effectiveCount / realCount : effectiveCount > 0 ? Infinity : 1;

    state.daState = {
      loading: false,
      error: null,
      data: { effectiveCount, realCount, ratio },
    };
  } catch (err) {
    // If query was aborted, just ignore silently
    if (err instanceof QueryAbortedError) {
      return;
    }
    state.daState = { loading: false, error: getQueryErrorMessage(err), data: null };
  }

  renderModal();
}

/** Load Reachability data */
async function loadReachability(): Promise<void> {
  state.reachabilityState = { loading: true, error: null, data: null };
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

    // Run all reachability queries in parallel - single hop, exclude MemberOf
    const queries = principals.map(async (p) => {
      try {
        const query = `
          MATCH (g:Group)-[r]->(target)
          WHERE g.objectid ENDS WITH '${p.sid}'
          AND type(r) <> 'MemberOf'
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

    state.reachabilityState = { loading: false, error: null, data: results };
  } catch (err) {
    if (err instanceof QueryAbortedError) {
      return;
    }
    state.reachabilityState = { loading: false, error: getQueryErrorMessage(err), data: null };
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
  state.staleState = { loading: true, error: null, data: null };
  renderModal();

  try {
    const threshold = daysToWindowsFileTime(state.staleThresholdDays);

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

    state.staleState = {
      loading: false,
      error: null,
      data: {
        users: usersResult.resultCount,
        computers: computersResult.resultCount,
        thresholdDays: state.staleThresholdDays,
      },
    };
  } catch (err) {
    if (err instanceof QueryAbortedError) {
      return;
    }
    state.staleState = { loading: false, error: getQueryErrorMessage(err), data: null };
  }

  renderModal();
}

/** Load Account Exposure data */
async function loadAccountExposure(): Promise<void> {
  state.accountExposureState = { loading: true, error: null, data: null };
  renderModal();

  try {
    const [kerbResult, asrepResult, delegationResult, protectedResult] = await Promise.all([
      executeQuery(`MATCH (u:User) WHERE u.hasspn = true AND u.enabled = true RETURN u`, {
        extractGraph: false,
        background: true,
      }),
      executeQuery(`MATCH (u:User) WHERE u.dontreqpreauth = true AND u.enabled = true RETURN u`, {
        extractGraph: false,
        background: true,
      }),
      executeQuery(`MATCH (c:Computer) WHERE c.unconstraineddelegation = true AND c.enabled = true RETURN c`, {
        extractGraph: false,
        background: true,
      }),
      executeQuery(
        `MATCH (u:User), (g:Group), p = shortestPath((u)-[:MemberOf*1..]->(g)) WHERE g.objectid ENDS WITH '-525' RETURN DISTINCT u`,
        { extractGraph: false, background: true }
      ),
    ]);

    state.accountExposureState = {
      loading: false,
      error: null,
      data: {
        kerberoastable: kerbResult.resultCount,
        asrepRoastable: asrepResult.resultCount,
        unconstrainedDelegation: delegationResult.resultCount,
        protectedUsers: protectedResult.resultCount,
      },
    };
  } catch (err) {
    if (err instanceof QueryAbortedError) {
      return;
    }
    state.accountExposureState = { loading: false, error: getQueryErrorMessage(err), data: null };
  }

  renderModal();
}

/** Load Choke Points data */
async function loadChokePoints(): Promise<void> {
  state.chokePointsState = { loading: true, error: null, data: null };
  renderModal();

  try {
    const data = await api.get<ChokePointsData>("/api/graph/choke-points");
    state.chokePointsState = { loading: false, error: null, data };
  } catch (err) {
    const message = err instanceof Error ? err.message : "Failed to load choke points";
    state.chokePointsState = { loading: false, error: message, data: null };
  }

  renderModal();
}

/** Load Tier Violations data */
async function loadTierViolations(): Promise<void> {
  state.tierViolationsState = { loading: true, error: null, data: null };
  renderModal();

  try {
    const data = await api.get<TierViolationsData>("/api/graph/tier-violations");
    state.tierViolationsState = { loading: false, error: null, data };
  } catch (err) {
    const message = err instanceof Error ? err.message : "Failed to load tier violations";
    state.tierViolationsState = { loading: false, error: message, data: null };
  }

  renderModal();
}

/** Execute a choke point graph query using direct IDs */
async function executeChokePointQuery(sourceId: string, targetId: string, relType: string): Promise<void> {
  const query = `MATCH p=(a)-[r]->(b) WHERE a.objectid = '${sourceId}' AND b.objectid = '${targetId}' AND type(r) = '${relType}' RETURN p`;
  try {
    const result = await executeQuery(query, { extractGraph: true });
    if (result.graph && result.graph.nodes.length > 0) {
      loadGraphData(result.graph as unknown as RawADGraph);
    }
  } catch (err) {
    if (!(err instanceof QueryAbortedError)) {
      console.error("Failed to execute choke point query:", err);
    }
  }
}

/** Show tier violation edges as a graph using pre-fetched edge data */
async function executeTierViolationGraph(sid: string): Promise<void> {
  const data = state.tierViolationsState.data;
  if (!data) return;

  const parts = sid.split("-");
  const srcZone = parseInt(parts[0] ?? "0", 10);
  const tgtZone = parseInt(parts[1] ?? "0", 10);

  const violation = data.violations.find((v) => v.source_zone === srcZone && v.target_zone === tgtZone);
  if (!violation || violation.edges.length === 0) return;

  // Build a query using the actual violating edge IDs
  const pairs = violation.edges.slice(0, 500);
  const conditions = pairs
    .map(
      (e) =>
        `(a.objectid = '${e.source_id.replace(/'/g, "\\'")}' AND b.objectid = '${e.target_id.replace(/'/g, "\\'")}' AND type(r) = '${e.rel_type}')`
    )
    .join(" OR ");

  const query = `MATCH p=(a)-[r]->(b) WHERE ${conditions} RETURN p`;

  closeModal();

  try {
    const result = await executeQuery(query, { extractGraph: true });
    if (result.graph && result.graph.nodes.length > 0) {
      loadGraphData(result.graph as unknown as RawADGraph);
    }
  } catch (err) {
    if (!(err instanceof QueryAbortedError)) {
      console.error("Failed to load tier violation graph:", err);
    }
  }
}

/** Execute a graph query and render the result */
async function executeGraphQuery(queryType: string, extraData?: string): Promise<void> {
  let query: string;

  switch (queryType) {
    case "effective-das":
      query = `MATCH (u:User), (g:Group), p = shortestPath((u)-[*1..10]->(g)) WHERE g.objectid ENDS WITH '-512' RETURN p LIMIT 500`;
      break;
    case "real-das":
      query = `MATCH (u:User), (g:Group), p = shortestPath((u)-[:MemberOf*1..10]->(g)) WHERE g.objectid ENDS WITH '-512' RETURN p LIMIT 500`;
      break;
    case "reachability":
      query = `
        MATCH p=(g:Group)-[r]->(target)
        WHERE g.objectid ENDS WITH '${extraData}'
        AND type(r) <> 'MemberOf'
        RETURN p LIMIT 500
      `;
      break;
    case "kerberoastable":
      query = `MATCH (u:User) WHERE u.hasspn = true AND u.enabled = true RETURN u LIMIT 500`;
      break;
    case "asrep-roastable":
      query = `MATCH (u:User) WHERE u.dontreqpreauth = true AND u.enabled = true RETURN u LIMIT 500`;
      break;
    case "unconstrained-delegation":
      query = `MATCH (c:Computer) WHERE c.unconstraineddelegation = true AND c.enabled = true RETURN c LIMIT 500`;
      break;
    case "protected-users":
      query = `MATCH (u:User), (g:Group), p = shortestPath((u)-[:MemberOf*1..]->(g)) WHERE g.objectid ENDS WITH '-525' RETURN DISTINCT u LIMIT 500`;
      break;
    case "stale-users": {
      const threshold = daysToWindowsFileTime(state.staleThresholdDays);
      query = `MATCH (u:User) WHERE u.enabled = true AND u.lastlogon < ${threshold} RETURN u LIMIT 500`;
      break;
    }
    case "stale-computers": {
      const threshold = daysToWindowsFileTime(state.staleThresholdDays);
      query = `MATCH (c:Computer) WHERE c.enabled = true AND c.lastlogon < ${threshold} RETURN c LIMIT 500`;
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
      const emptyGraph: RawADGraph = { nodes: [], relationships: [] };
      loadGraphData(emptyGraph);
    }
  } catch (err) {
    // Silently ignore aborted queries
    if (err instanceof QueryAbortedError) {
      return;
    }
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
    if (tabId && tabId !== state.activeTab) {
      state.activeTab = tabId;
      renderModal();
    }
    return;
  }

  // Clickable values (graph queries)
  const clickableValue = target.closest("[data-query]") as HTMLElement;
  if (clickableValue) {
    const queryType = clickableValue.getAttribute("data-query");
    if (queryType === "tier-violation") {
      const sid = clickableValue.getAttribute("data-sid");
      if (sid) {
        executeTierViolationGraph(sid);
      }
      return;
    }
    if (queryType === "choke-point") {
      const sourceId = clickableValue.getAttribute("data-source-id");
      const targetId = clickableValue.getAttribute("data-target-id");
      const relType = clickableValue.getAttribute("data-rel-type");
      if (sourceId && targetId && relType) {
        closeModal();
        executeChokePointQuery(sourceId, targetId, relType);
      }
      return;
    }
    const sid = clickableValue.getAttribute("data-sid");
    if (queryType) {
      executeGraphQuery(queryType, sid ?? undefined);
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
    case "toggle-expand":
      state.modalExpanded = !state.modalExpanded;
      updateModalExpanded();
      break;
    case "refresh":
      // Reload all tabs
      state.chokePointsPage = 0;
      state.unexpectedChokePointsPage = 0;
      loadDAAnalysis();
      loadReachability();
      loadStaleObjects();
      loadAccountExposure();
      loadChokePoints();
      loadTierViolations();
      break;
    case "choke-page-prev":
      if (state.chokePointsPage > 0) {
        state.chokePointsPage--;
        renderModal();
      }
      break;
    case "choke-page-next": {
      const total = state.chokePointsState.data?.choke_points.length ?? 0;
      const maxPage = Math.ceil(total / CHOKE_POINTS_PAGE_SIZE) - 1;
      if (state.chokePointsPage < maxPage) {
        state.chokePointsPage++;
        renderModal();
      }
      break;
    }
    case "unexpected-choke-page-prev":
      if (state.unexpectedChokePointsPage > 0) {
        state.unexpectedChokePointsPage--;
        renderModal();
      }
      break;
    case "unexpected-choke-page-next": {
      const unexpectedCount = state.chokePointsState.data?.unexpected_choke_points.length ?? 0;
      const maxUnexpectedPage = Math.ceil(unexpectedCount / CHOKE_POINTS_PAGE_SIZE) - 1;
      if (state.unexpectedChokePointsPage < maxUnexpectedPage) {
        state.unexpectedChokePointsPage++;
        renderModal();
      }
      break;
    }
    case "compute-effective-tiers":
      computeEffectiveTiers();
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
    if (newThreshold !== state.staleThresholdDays) {
      state.staleThresholdDays = newThreshold;
      loadStaleObjects();
    }
  }
}
