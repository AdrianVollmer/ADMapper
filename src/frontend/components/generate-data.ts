/**
 * Generate Sample Data Component
 *
 * Modal for generating sample Active Directory data for testing and demos.
 */

import { api } from "../api/client";
import { escapeHtml } from "../utils/html";
import { loadGraphData } from "./graph-view";
import type { RawADGraph } from "../graph/types";

/** Data size presets */
type DataSize = "small" | "medium" | "large";

/** Size descriptions */
const SIZE_INFO: Record<DataSize, { nodes: string; description: string }> = {
  small: {
    nodes: "~100 nodes",
    description: "Single domain with basic structure. Good for quick testing.",
  },
  medium: {
    nodes: "~500 nodes",
    description: "Single forest with multiple domains and realistic group structure.",
  },
  large: {
    nodes: "~2000 nodes",
    description: "Multiple forests with foreign trust, three tiers, and common vulnerabilities.",
  },
};

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Initialize the generate data modal */
export function initGenerateData(): void {
  // Modal is created on demand
}

/** Open the generate data modal */
export async function openGenerateData(): Promise<void> {
  // First check if database is empty
  try {
    const stats = await api.get<{ total_nodes: number; total_edges: number }>("/api/graph/detailed-stats");
    if (stats.total_nodes > 0 || stats.total_edges > 0) {
      showDatabaseNotEmpty(stats.total_nodes, stats.total_edges);
      return;
    }
  } catch (err) {
    console.error("Failed to check database stats:", err);
  }

  if (!modalEl) {
    createModal();
  }
  showSizeSelection();
  modalEl!.hidden = false;
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
  modalEl.id = "generate-data-modal";
  modalEl.className = "modal-overlay";
  modalEl.innerHTML = `
    <div class="modal-content" style="max-width: 480px;">
      <div class="modal-header">
        <h2 class="modal-title">Generate Sample Data</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body" id="generate-data-body">
      </div>
      <div class="modal-footer" id="generate-data-footer">
        <button class="btn btn-secondary" data-action="close">Cancel</button>
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

/** Show database not empty message */
function showDatabaseNotEmpty(nodes: number, relationships: number): void {
  if (!modalEl) {
    createModal();
  }

  const body = document.getElementById("generate-data-body");
  const footer = document.getElementById("generate-data-footer");
  if (!body || !footer) return;

  body.innerHTML = `
    <div class="text-center py-4">
      <svg class="w-12 h-12 mx-auto mb-4 text-yellow-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
      </svg>
      <h3 class="text-lg font-semibold mb-2">Database Not Empty</h3>
      <p class="text-gray-400 mb-4">
        The database already contains <strong>${nodes.toLocaleString()}</strong> nodes
        and <strong>${relationships.toLocaleString()}</strong> relationships.
      </p>
      <p class="text-gray-400">
        Sample data generation is only available for empty databases.
        Clear the database first if you want to generate sample data.
      </p>
    </div>
  `;

  footer.innerHTML = `
    <button class="btn btn-primary" data-action="close">OK</button>
  `;

  modalEl!.hidden = false;
}

/** Show size selection UI */
function showSizeSelection(): void {
  const body = document.getElementById("generate-data-body");
  const footer = document.getElementById("generate-data-footer");
  if (!body || !footer) return;

  body.innerHTML = `
    <p class="text-gray-300 mb-4">
      Generate a sample Active Directory environment with realistic structure,
      group memberships, and common vulnerabilities for testing.
    </p>
    <div class="space-y-3">
      ${(["small", "medium", "large"] as DataSize[])
        .map(
          (size) => `
        <label class="generate-size-option">
          <input type="radio" name="data-size" value="${size}" ${size === "medium" ? "checked" : ""}>
          <div class="generate-size-content">
            <div class="generate-size-header">
              <span class="generate-size-name">${size.charAt(0).toUpperCase() + size.slice(1)}</span>
              <span class="generate-size-nodes">${SIZE_INFO[size].nodes}</span>
            </div>
            <p class="generate-size-desc">${SIZE_INFO[size].description}</p>
          </div>
        </label>
      `
        )
        .join("")}
    </div>
  `;

  footer.innerHTML = `
    <button class="btn btn-secondary" data-action="close">Cancel</button>
    <button class="btn btn-primary" data-action="generate">Generate</button>
  `;
}

/** Show generating progress */
function showProgress(): void {
  const body = document.getElementById("generate-data-body");
  const footer = document.getElementById("generate-data-footer");
  if (!body || !footer) return;

  body.innerHTML = `
    <div class="flex items-center justify-center py-8">
      <div class="spinner"></div>
      <span class="ml-3 text-gray-400">Generating sample data...</span>
    </div>
    <p class="text-center text-gray-500 text-sm">This may take a moment for larger datasets.</p>
  `;

  footer.innerHTML = ``;
}

/** Show completion */
function showComplete(nodes: number, relationships: number): void {
  const body = document.getElementById("generate-data-body");
  const footer = document.getElementById("generate-data-footer");
  if (!body || !footer) return;

  body.innerHTML = `
    <div class="text-center py-4">
      <svg class="w-12 h-12 mx-auto mb-4 text-green-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
        <polyline points="22,4 12,14.01 9,11.01"/>
      </svg>
      <h3 class="text-lg font-semibold mb-2">Generation Complete</h3>
      <p class="text-gray-400">
        Created <strong>${nodes.toLocaleString()}</strong> nodes
        and <strong>${relationships.toLocaleString()}</strong> relationships.
      </p>
    </div>
  `;

  footer.innerHTML = `
    <button class="btn btn-primary" data-action="done">Done</button>
  `;
}

/** Show error */
function showError(message: string): void {
  const body = document.getElementById("generate-data-body");
  const footer = document.getElementById("generate-data-footer");
  if (!body || !footer) return;

  body.innerHTML = `
    <div class="text-center py-4">
      <svg class="w-12 h-12 mx-auto mb-4 text-red-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <circle cx="12" cy="12" r="10"/>
        <line x1="15" y1="9" x2="9" y2="15"/>
        <line x1="9" y1="9" x2="15" y2="15"/>
      </svg>
      <h3 class="text-lg font-semibold mb-2">Generation Failed</h3>
      <p class="text-red-400">${escapeHtml(message)}</p>
    </div>
  `;

  footer.innerHTML = `
    <button class="btn btn-secondary" data-action="retry">Try Again</button>
    <button class="btn btn-primary" data-action="close">Close</button>
  `;
}

/** Handle click events */
async function handleClick(e: Event): Promise<void> {
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

    case "generate":
      startGeneration();
      break;

    case "done":
      closeModal();
      // Refresh the graph programmatically to show the generated data
      try {
        const graphData = await api.get<RawADGraph>("/api/graph/all");
        await loadGraphData(graphData);
      } catch (err) {
        console.error("Failed to reload graph data:", err);
      }
      break;

    case "retry":
      showSizeSelection();
      break;
  }
}

/** Start the generation process */
async function startGeneration(): Promise<void> {
  const sizeInput = document.querySelector('input[name="data-size"]:checked') as HTMLInputElement;
  if (!sizeInput) return;

  const size = sizeInput.value as DataSize;
  showProgress();

  try {
    const result = await api.post<{ nodes: number; relationships: number }>("/api/graph/generate", { size });
    showComplete(result.nodes, result.relationships);
  } catch (err) {
    showError(err instanceof Error ? err.message : String(err));
  }
}
