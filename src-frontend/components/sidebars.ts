/**
 * Sidebar Components
 *
 * Handles collapsible navigation and detail sidebars.
 */

import { appState } from "../main";
import type { ADNodeAttributes } from "../graph/types";
import { NODE_COLORS } from "../graph/theme";

const NAV_SIDEBAR_WIDTH = "240px";
const DETAIL_SIDEBAR_WIDTH = "300px";

/** Initialize sidebars */
export function initSidebars(): void {
  // Set up toggle buttons
  document.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;
    const button = target.closest("[data-action]") as HTMLElement;
    if (!button) return;

    const action = button.getAttribute("data-action");
    if (action === "toggle-nav-sidebar") {
      toggleNavSidebar();
    } else if (action === "toggle-detail-sidebar") {
      toggleDetailSidebar();
    }
  });
}

/** Toggle the navigation sidebar */
export function toggleNavSidebar(): void {
  const sidebar = document.getElementById("nav-sidebar");
  const expandBtn = document.getElementById("nav-sidebar-expand");
  if (!sidebar || !expandBtn) return;

  appState.navSidebarCollapsed = !appState.navSidebarCollapsed;

  if (appState.navSidebarCollapsed) {
    sidebar.setAttribute("data-collapsed", "true");
    sidebar.style.width = "0";
    expandBtn.classList.remove("hidden");
  } else {
    sidebar.setAttribute("data-collapsed", "false");
    sidebar.style.width = NAV_SIDEBAR_WIDTH;
    expandBtn.classList.add("hidden");
  }
}

/** Toggle the detail sidebar */
export function toggleDetailSidebar(): void {
  const sidebar = document.getElementById("detail-sidebar");
  const expandBtn = document.getElementById("detail-sidebar-expand");
  if (!sidebar || !expandBtn) return;

  appState.detailSidebarCollapsed = !appState.detailSidebarCollapsed;

  if (appState.detailSidebarCollapsed) {
    sidebar.setAttribute("data-collapsed", "true");
    sidebar.style.width = "0";
    expandBtn.classList.remove("hidden");
  } else {
    sidebar.setAttribute("data-collapsed", "false");
    sidebar.style.width = DETAIL_SIDEBAR_WIDTH;
    expandBtn.classList.add("hidden");
  }
}

/** Update the detail sidebar with node information */
export function updateDetailPanel(nodeId: string | null, attrs: ADNodeAttributes | null): void {
  const content = document.getElementById("detail-content");
  if (!content) return;

  appState.selectedNodeId = nodeId;

  if (!nodeId || !attrs) {
    content.innerHTML = `
      <div class="empty-state">
        <svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
          <circle cx="12" cy="12" r="10"/>
          <path d="M12 16v-4M12 8h.01"/>
        </svg>
        <p>Select a node to view details</p>
      </div>
    `;
    return;
  }

  const typeColor = NODE_COLORS[attrs.nodeType] || "#6c757d";
  const typeLower = attrs.nodeType.toLowerCase();

  // Build properties list
  let propsHtml = "";
  if (attrs.properties) {
    for (const [key, value] of Object.entries(attrs.properties)) {
      if (value !== null && value !== undefined && value !== "") {
        propsHtml += `
          <div class="detail-prop">
            <span class="detail-prop-label">${escapeHtml(key)}</span>
            <span class="detail-prop-value">${escapeHtml(String(value))}</span>
          </div>
        `;
      }
    }
  }

  content.innerHTML = `
    <div class="detail-header">
      <span class="detail-node-type node-badge ${typeLower}" style="background-color: ${typeColor}">
        ${escapeHtml(attrs.nodeType)}
      </span>
      <h2 class="detail-node-name">${escapeHtml(attrs.label)}</h2>
    </div>

    ${
      propsHtml
        ? `
    <div class="detail-section">
      <h3 class="detail-section-title">Properties</h3>
      <div class="detail-props">
        ${propsHtml}
      </div>
    </div>
    `
        : ""
    }

    <div class="detail-section">
      <h3 class="detail-section-title">Actions</h3>
      <div class="space-y-1">
        <button class="w-full px-3 py-1.5 text-sm text-left rounded bg-gray-700 hover:bg-gray-600 text-gray-200 transition-colors" data-action="expand-node" data-node-id="${escapeHtml(nodeId)}">
          Expand Connections
        </button>
        <button class="w-full px-3 py-1.5 text-sm text-left rounded bg-gray-700 hover:bg-gray-600 text-gray-200 transition-colors" data-action="find-path-from" data-node-id="${escapeHtml(nodeId)}">
          Find Path From Here
        </button>
        <button class="w-full px-3 py-1.5 text-sm text-left rounded bg-gray-700 hover:bg-gray-600 text-gray-200 transition-colors" data-action="find-path-to" data-node-id="${escapeHtml(nodeId)}">
          Find Path To Here
        </button>
      </div>
    </div>
  `;
}

/** Escape HTML special characters */
function escapeHtml(str: string): string {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}
