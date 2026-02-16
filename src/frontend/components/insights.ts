/**
 * Insights Component
 *
 * Modal for viewing security insights and findings from the AD graph.
 */

import { api } from "../api/client";
import { escapeHtml } from "../utils/html";

/** Reachability insight from API */
interface ReachabilityInsight {
  principal_name: string;
  principal_id: string | null;
  reachable_count: number;
}

/** Security insights response from API */
interface SecurityInsights {
  effective_da_count: number;
  real_da_count: number;
  da_ratio: number;
  total_users: number;
  effective_da_percentage: number;
  reachability: ReachabilityInsight[];
  effective_das: [string, string, number][]; // [id, label, hops]
  real_das: [string, string][]; // [id, label]
}

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Current insights data */
let currentInsights: SecurityInsights | null = null;

/** Initialize the insights modal */
export function initInsights(): void {
  // Modal is created on demand
}

/** Open the insights modal */
export async function openInsights(): Promise<void> {
  if (!modalEl) {
    createModal();
  }
  modalEl!.hidden = false;
  await loadInsights();
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
      <div class="modal-body" id="insights-body">
        <div class="flex items-center justify-center py-8">
          <div class="spinner"></div>
          <span class="ml-3 text-gray-400">Analyzing graph...</span>
        </div>
      </div>
      <div class="modal-footer" id="insights-footer">
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

/** Load and display insights */
async function loadInsights(): Promise<void> {
  const body = document.getElementById("insights-body");
  const footer = document.getElementById("insights-footer");
  if (!body || !footer) return;

  try {
    currentInsights = await api.get<SecurityInsights>("/api/graph/insights");
    renderInsights(body, footer, currentInsights);
  } catch (err) {
    body.innerHTML = `
      <div class="text-red-400 p-4">
        Failed to compute insights: ${escapeHtml(err instanceof Error ? err.message : String(err))}
      </div>
    `;
  }
}

/** Render insights in the modal */
function renderInsights(body: HTMLElement, footer: HTMLElement, insights: SecurityInsights): void {
  const hasData = insights.total_users > 0;

  // Build reachability HTML
  let reachabilityHtml = "";
  for (const r of insights.reachability) {
    const hasData = r.principal_id !== null;
    reachabilityHtml += `
      <div class="insight-row">
        <span class="insight-label">${escapeHtml(r.principal_name)}</span>
        <span class="insight-value ${!hasData ? "text-gray-500" : ""}">${
          hasData ? r.reachable_count.toLocaleString() : "Not found"
        }</span>
      </div>
    `;
  }

  body.innerHTML = `
    <div class="insights-container">
      <div class="insight-section">
        <h3 class="insight-section-title">Domain Admin Analysis</h3>
        <div class="insight-cards">
          <div class="insight-card insight-card-primary">
            <div class="insight-card-value">${insights.effective_da_count.toLocaleString()}</div>
            <div class="insight-card-label">Effective Domain Admins</div>
            <div class="insight-card-desc">Users with any path to DA</div>
          </div>
          <div class="insight-card insight-card-secondary">
            <div class="insight-card-value">${insights.real_da_count.toLocaleString()}</div>
            <div class="insight-card-label">Real Domain Admins</div>
            <div class="insight-card-desc">Direct/transitive members</div>
          </div>
          <div class="insight-card">
            <div class="insight-card-value">${insights.da_ratio.toFixed(1)}x</div>
            <div class="insight-card-label">Privilege Expansion</div>
            <div class="insight-card-desc">Effective vs Real ratio</div>
          </div>
        </div>
        <div class="insight-stats">
          <div class="insight-row">
            <span class="insight-label">Total Users</span>
            <span class="insight-value">${insights.total_users.toLocaleString()}</span>
          </div>
          <div class="insight-row">
            <span class="insight-label">Users with DA Path</span>
            <span class="insight-value ${insights.effective_da_percentage > 10 ? "text-red-400" : ""}">${insights.effective_da_percentage.toFixed(1)}%</span>
          </div>
        </div>
      </div>

      <div class="insight-section">
        <h3 class="insight-section-title">Reachability from Well-Known Principals</h3>
        <p class="insight-desc">Objects reachable through any edge type from common groups:</p>
        <div class="insight-stats">
          ${reachabilityHtml || '<p class="text-gray-500">No well-known principals found</p>'}
        </div>
      </div>
    </div>
  `;

  footer.innerHTML = `
    <div class="flex gap-2">
      <button class="btn btn-secondary" data-action="export-csv" ${!hasData ? "disabled" : ""}>Export CSV</button>
      <button class="btn btn-secondary" data-action="export-json" ${!hasData ? "disabled" : ""}>Export JSON</button>
    </div>
    <div class="flex gap-2">
      <button class="btn btn-secondary" data-action="refresh">Refresh</button>
      <button class="btn btn-primary" data-action="close">Close</button>
    </div>
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
      loadInsights();
      break;

    case "export-csv":
      exportCSV();
      break;

    case "export-json":
      exportJSON();
      break;
  }
}

/** Export insights as CSV */
function exportCSV(): void {
  if (!currentInsights) return;

  const lines: string[] = [];

  // Header section
  lines.push("Security Insights Report");
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push("");

  // Summary section
  lines.push("Summary");
  lines.push("Metric,Value");
  lines.push(`Total Users,${currentInsights.total_users}`);
  lines.push(`Effective Domain Admins,${currentInsights.effective_da_count}`);
  lines.push(`Real Domain Admins,${currentInsights.real_da_count}`);
  lines.push(`Privilege Expansion Ratio,${currentInsights.da_ratio.toFixed(2)}`);
  lines.push(`Effective DA Percentage,${currentInsights.effective_da_percentage.toFixed(2)}%`);
  lines.push("");

  // Reachability section
  lines.push("Reachability from Well-Known Principals");
  lines.push("Principal,Reachable Objects");
  for (const r of currentInsights.reachability) {
    lines.push(`"${r.principal_name}",${r.reachable_count}`);
  }
  lines.push("");

  // Effective DAs section
  lines.push("Effective Domain Admins (Users with path to DA)");
  lines.push("Object ID,Label,Hops to DA");
  for (const [id, label, hops] of currentInsights.effective_das) {
    lines.push(`"${id}","${label}",${hops}`);
  }
  lines.push("");

  // Real DAs section
  lines.push("Real Domain Admins (Direct/Transitive Members)");
  lines.push("Object ID,Label");
  for (const [id, label] of currentInsights.real_das) {
    lines.push(`"${id}","${label}"`);
  }

  const csv = lines.join("\n");
  downloadBlob(new Blob([csv], { type: "text/csv" }), "admapper-insights.csv");
}

/** Export insights as JSON */
function exportJSON(): void {
  if (!currentInsights) return;

  const data = {
    generated_at: new Date().toISOString(),
    summary: {
      total_users: currentInsights.total_users,
      effective_da_count: currentInsights.effective_da_count,
      real_da_count: currentInsights.real_da_count,
      da_ratio: currentInsights.da_ratio,
      effective_da_percentage: currentInsights.effective_da_percentage,
    },
    reachability: currentInsights.reachability,
    effective_das: currentInsights.effective_das.map(([id, label, hops]) => ({
      id,
      label,
      hops_to_da: hops,
    })),
    real_das: currentInsights.real_das.map(([id, label]) => ({
      id,
      label,
    })),
  };

  const json = JSON.stringify(data, null, 2);
  downloadBlob(new Blob([json], { type: "application/json" }), "admapper-insights.json");
}

/** Download a blob as a file */
function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
