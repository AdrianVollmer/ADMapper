/**
 * Sidebar Components
 *
 * Handles collapsible navigation and detail sidebars.
 */

import { appState } from "../main";
import type { ADNodeAttributes, ADNodeType, ADEdgeAttributes, RawADGraph, ADEdgeType } from "../graph/types";
import { NODE_COLORS, EDGE_COLORS } from "../graph/theme";
import { escapeHtml } from "../utils/html";
import { api } from "../api/client";
import { setPathStart, setPathEnd } from "./search";
import { getRenderer, loadGraphData } from "./graph-view";
import { showError, showConfirm } from "../utils/notifications";
import { executeQuery, QueryAbortedError } from "../utils/query";
import { HIGH_VALUE_RIDS, ridWhereClause } from "./queries/builtin-queries";
import { openEditNode, openEditEdge } from "./add-node-edge";

const NAV_SIDEBAR_WIDTH = "240px";
const DETAIL_SIDEBAR_WIDTH = "300px";

/**
 * Priority order for properties in the detail panel.
 * Lower numbers appear first. Properties not listed get a default priority of 100.
 */
const PROPERTY_PRIORITY: Record<string, number> = {
  // Core identity (top priority)
  name: 1,
  displayname: 2,
  samaccountname: 3,
  userprincipalname: 4,
  cn: 5,

  // Domain & location
  domain: 10,
  distinguishedname: 11,

  // Identifiers
  objectsid: 20,
  objectid: 21,
  domainsid: 22,

  // Contact
  email: 30,
  mail: 30,

  // Description
  description: 40,

  // Account status
  enabled: 50,
  admincount: 51,
  tier: 52,
  effective_tier: 53,
  sensitive: 54,

  // Computer info
  operatingsystem: 60,
  operatingsystemversion: 61,

  // Group info
  grouptype: 70,
  membercount: 71,

  // Timestamps (lower priority)
  whencreated: 80,
  whenchanged: 81,
  lastlogon: 82,
  lastlogontimestamp: 83,
  pwdlastset: 84,
};

/** Pretty labels for common AD properties */
const PROPERTY_LABELS: Record<string, string> = {
  // Identity
  objectid: "Object SID",
  distinguishedname: "Distinguished Name",
  samaccountname: "SAM Account Name",
  userprincipalname: "User Principal Name",
  displayname: "Display Name",
  name: "Name",
  cn: "Common Name",
  description: "Description",

  // Domain
  domain: "Domain",
  domainsid: "Domain SID",
  functionallevel: "Functional Level",

  // Account status
  enabled: "Enabled",
  pwdneverexpires: "Password Never Expires",
  pwdlastset: "Password Last Set",
  lastlogon: "Last Logon",
  lastlogontimestamp: "Last Logon Timestamp",
  whencreated: "Created",
  whenchanged: "Changed",
  admincount: "Admin Count",
  tier: "Tier (Assigned)",
  effective_tier: "Tier (Effective)",
  sensitive: "Sensitive",

  // Computer
  operatingsystem: "Operating System",
  operatingsystemversion: "OS Version",
  serviceprincipalname: "Service Principal Name",
  unconstraineddelegation: "Unconstrained Delegation",

  // Group
  grouptype: "Group Type",
  membercount: "Member Count",

  // OU/GPO
  gpopath: "GPO Path",
  blocksinheritance: "Blocks Inheritance",

  // Trust
  trusttype: "Trust Type",
  trustdirection: "Trust Direction",
  trustattributes: "Trust Attributes",
  sidfilteringenabled: "SID Filtering Enabled",

  // Certificate
  certificatetemplatename: "Template Name",
  enrollmentflag: "Enrollment Flag",
  certificatenameflags: "Name Flags",

  // Email
  email: "Email",
  mail: "Email",

  // Misc
  hasspn: "Has SPN",
  serviceprincipalnames: "SPNs",
  owned: "Owned",
  notes: "Notes",

  // Common timestamp variants
  created_at: "Created",
  createdat: "Created",
  updated_at: "Updated",
  updatedat: "Updated",
  accountexpires: "Account Expires",
  badpasswordtime: "Bad Password Time",
  lockouttime: "Lockout Time",
};

/** Main toolbar action definitions */
const MAIN_ACTIONS = [
  {
    id: "show-incoming",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="12" cy="12" r="4"/>
      <path d="M12 2v4m0 12v4M2 12h4m12 0h4"/>
      <path d="M12 2l-2 2m2-2l2 2M12 22l-2-2m2 2l2-2M2 12l2-2m-2 2l2 2M22 12l-2-2m2 2l-2 2"/>
    </svg>`,
    tooltip: "Incoming",
    countKey: "incoming",
    nodeTypes: null, // all types
  },
  {
    id: "show-outgoing",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="12" cy="12" r="4"/>
      <path d="M12 2v4m0 12v4M2 12h4m12 0h4"/>
      <path d="M12 6l-2-2m2 2l2-2M12 18l-2 2m2-2l2 2M6 12l-2-2m2 2l-2 2M18 12l2-2m-2 2l2 2"/>
    </svg>`,
    tooltip: "Outgoing",
    countKey: "outgoing",
    nodeTypes: null,
  },
  {
    id: "show-admin-to",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <path d="M12 2L4 6v6c0 5.5 3.4 10 8 11 4.6-1 8-5.5 8-11V6l-8-4z"/>
      <path d="M8 12h8M12 8v8"/>
    </svg>`,
    tooltip: "Admin Permissions",
    countKey: "adminTo",
    nodeTypes: ["User", "Computer", "Group"],
  },
  {
    id: "show-memberof",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="10" cy="8" r="4"/>
      <path d="M2 20v-1c0-2.5 3.6-4.5 8-4.5"/>
      <path d="M16 16l4 4m0-4l-4 4"/>
      <circle cx="18" cy="18" r="4"/>
    </svg>`,
    tooltip: "Member Of",
    countKey: "memberOf",
    nodeTypes: ["User", "Computer", "Group"],
  },
  {
    id: "show-members",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="9" cy="7" r="3"/>
      <circle cx="17" cy="7" r="3"/>
      <path d="M3 18c0-2.2 2.7-4 6-4s6 1.8 6 4"/>
      <path d="M15 14c1.4-.6 2.8-1 4-1 2.2 0 4 1.3 4 3"/>
    </svg>`,
    tooltip: "Members",
    countKey: "members",
    nodeTypes: ["Group"],
  },
];

/** Overflow menu actions (three-dot menu) */
const OVERFLOW_ACTIONS = [
  {
    id: "toggle-owned",
    label: "Mark Owned",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
      <polyline points="22 4 12 14.01 9 11.01"/>
    </svg>`,
  },
  {
    id: "set-start-node",
    label: "Set as Start Node",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="5" cy="12" r="3"/>
      <path d="M8 12h13"/>
    </svg>`,
  },
  {
    id: "set-end-node",
    label: "Set as End Node",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="19" cy="12" r="3"/>
      <path d="M3 12h13"/>
      <path d="M12 8l4 4-4 4"/>
    </svg>`,
  },
  {
    id: "edit-node",
    label: "Edit Properties",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <path d="M17 3a2.83 2.83 0 114 4L7.5 20.5 2 22l1.5-5.5L17 3z"/>
    </svg>`,
  },
  {
    id: "delete-node",
    label: "Delete Node",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <path d="M3 6h18M8 6V4h8v2M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6"/>
      <path d="M10 11v6M14 11v6"/>
    </svg>`,
    danger: true,
  },
];
/** User status indicator icons */
const USER_INDICATORS = {
  owned: {
    icon: `<svg viewBox="0 0 24 24" fill="currentColor" class="w-4 h-4 text-red-500">
      <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/>
    </svg>`,
    tooltip: "Owned",
  },
  enterpriseAdmin: {
    icon: `<svg viewBox="0 0 24 24" fill="currentColor" class="w-4 h-4 text-purple-500">
      <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/>
    </svg>`,
    tooltip: "Enterprise Admin",
  },
  domainAdmin: {
    icon: `<svg viewBox="0 0 24 24" fill="currentColor" class="w-4 h-4 text-yellow-500">
      <path d="M5 16L3 5l5.5 5L12 4l3.5 6L21 5l-2 11H5z"/>
      <path d="M5 19h14v2H5z"/>
    </svg>`,
    tooltip: "Domain Admin",
  },
  tierZero: {
    icon: `<svg viewBox="0 0 24 24" fill="currentColor" class="w-4 h-4 text-orange-500">
      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>
    </svg>`,
    tooltip: "Tier 0",
  },
  hasPath: {
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="w-4 h-4 text-blue-500">
      <path d="M13 17l5-5-5-5M6 17l5-5-5-5"/>
    </svg>`,
    tooltip: "Has Path to Tier 0",
  },
  checking: {
    icon: `<span class="spinner-sm"></span>`,
    tooltip: "Checking paths...",
  },
  noPath: {
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="w-4 h-4 text-gray-500">
      <circle cx="12" cy="12" r="10"/>
      <path d="M8 12h8"/>
    </svg>`,
    tooltip: "No path to tier-0 targets",
  },
};

/** Render a user status indicator */
function renderIndicator(type: keyof typeof USER_INDICATORS): string {
  const indicator = USER_INDICATORS[type];
  return `<span class="user-indicator" title="${indicator.tooltip}">${indicator.icon}</span>`;
}

/** Initialize sidebars */
export function initSidebars(): void {
  // Sidebars are now initialized without document-level click handlers.
  // Click handling is consolidated in main.ts via handleSidebarClicks().
}

/**
 * Consolidated click handler for sidebar-related clicks.
 * Called from the central document click handler in main.ts.
 * Returns true if the click was handled and should stop propagation.
 */
export function handleSidebarClicks(e: MouseEvent): boolean {
  const target = e.target as HTMLElement;

  // Handle toggle sidebar actions
  const actionButton = target.closest("[data-action]") as HTMLElement;
  if (actionButton) {
    const action = actionButton.getAttribute("data-action");
    if (action === "toggle-nav-sidebar") {
      toggleNavSidebar();
      return true;
    } else if (action === "toggle-detail-sidebar") {
      toggleDetailSidebar();
      return true;
    }
  }

  // Handle click-to-copy for property values
  const valueEl = target.closest(".detail-prop-value") as HTMLElement;
  if (valueEl) {
    const text = valueEl.getAttribute("data-value") || valueEl.textContent || "";
    copyToClipboard(text, valueEl);
    return true;
  }

  // Handle overflow menu toggle
  const trigger = target.closest(".overflow-trigger") as HTMLElement;

  // Close any open overflow menus when clicking elsewhere
  const allDropdowns = document.querySelectorAll(".overflow-dropdown:not([hidden])");
  for (const dropdown of allDropdowns) {
    if (!trigger || !dropdown.previousElementSibling?.contains(trigger)) {
      dropdown.setAttribute("hidden", "");
      dropdown.previousElementSibling?.setAttribute("aria-expanded", "false");
    }
  }

  if (trigger) {
    const dropdown = trigger.nextElementSibling as HTMLElement;
    if (dropdown?.classList.contains("overflow-dropdown")) {
      const isHidden = dropdown.hasAttribute("hidden");
      if (isHidden) {
        dropdown.removeAttribute("hidden");
        trigger.setAttribute("aria-expanded", "true");
      } else {
        dropdown.setAttribute("hidden", "");
        trigger.setAttribute("aria-expanded", "false");
      }
    }
    return true;
  }

  // Handle detail panel action buttons
  const button = target.closest(".detail-action-btn, .overflow-item") as HTMLElement;
  if (button && !button.classList.contains("overflow-trigger")) {
    const action = button.getAttribute("data-action");
    const nodeId = button.getAttribute("data-node-id");
    const edgeId = button.getAttribute("data-relationship-id");

    if (action && nodeId) {
      // Close overflow menu if clicking an overflow item
      const dropdown = button.closest(".overflow-dropdown");
      if (dropdown) {
        dropdown.setAttribute("hidden", "");
      }
      handleDetailAction(action, nodeId);
      return true;
    }

    if (action && edgeId) {
      // Handle relationship actions
      const sourceId = button.getAttribute("data-source-id") || "";
      const targetId = button.getAttribute("data-target-id") || "";
      const edgeType = button.getAttribute("data-relationship-type") || "";
      handleEdgeAction(action, edgeId, sourceId, targetId, edgeType);
      return true;
    }
  }

  return false;
}

/** Handle detail panel actions */
async function handleDetailAction(action: string, nodeId: string): Promise<void> {
  const renderer = getRenderer();
  const graph = renderer?.sigma.getGraph();
  const nodeLabel = graph?.getNodeAttribute(nodeId, "label") || nodeId;

  switch (action) {
    case "show-incoming":
      await loadConnections(nodeId, "incoming");
      break;

    case "show-outgoing":
      await loadConnections(nodeId, "outgoing");
      break;

    case "show-admin-to":
      await loadConnections(nodeId, "admin");
      break;

    case "show-memberof":
      await loadConnections(nodeId, "memberof");
      break;

    case "show-members":
      await loadConnections(nodeId, "members");
      break;

    case "set-start-node":
      setPathStart(nodeId, nodeLabel);
      break;

    case "set-end-node":
      setPathEnd(nodeId, nodeLabel);
      break;

    case "edit-node": {
      // Get node properties (from graph or fetch)
      const nodeProps = graph?.getNodeAttribute(nodeId, "properties") as Record<string, unknown> | undefined;
      if (nodeProps) {
        openEditNode(nodeId, nodeProps);
      } else {
        // Fetch properties if not in graph
        try {
          const node = await api.get<{ properties: Record<string, unknown> }>(
            `/api/graph/node/${encodeURIComponent(nodeId)}`
          );
          openEditNode(nodeId, node.properties || {});
        } catch (err) {
          console.error("Failed to fetch node for editing:", err);
          showError("Failed to load node properties");
        }
      }
      break;
    }

    case "delete-node": {
      const confirmed = await showConfirm(`Delete node "${nodeLabel}"?`, {
        title: "Delete Node",
        confirmText: "Delete",
        danger: true,
      });
      if (confirmed) {
        try {
          await api.delete(`/api/graph/nodes/${encodeURIComponent(nodeId)}`);
          // Remove from graph view
          if (graph?.hasNode(nodeId)) {
            graph.dropNode(nodeId);
          }
          // Clear detail panel
          updateDetailPanel(null, null);
        } catch (err) {
          console.error("Failed to delete node:", err);
          showError("Failed to delete node");
        }
      }
      break;
    }

    case "toggle-owned": {
      await toggleNodeOwned(nodeId);
      break;
    }

    default:
      console.log(`Unknown detail action: ${action}`);
  }
}

/** Handle relationship panel actions */
async function handleEdgeAction(
  action: string,
  edgeId: string,
  sourceId: string,
  targetId: string,
  edgeType: string
): Promise<void> {
  const renderer = getRenderer();
  const graph = renderer?.sigma.getGraph();

  switch (action) {
    case "edit-relationship": {
      // Get edge properties from graph or use empty object
      const edgeProps = graph?.getEdgeAttribute(edgeId, "properties") ?? {};
      openEditEdge(edgeId, sourceId, targetId, edgeType, edgeProps);
      break;
    }

    case "delete-relationship": {
      const confirmed = await showConfirm(`Delete relationship "${edgeType}" from "${sourceId}" to "${targetId}"?`, {
        title: "Delete Relationship",
        confirmText: "Delete",
        danger: true,
      });
      if (confirmed) {
        try {
          // Delete from backend
          await api.delete(
            `/api/graph/relationships/${encodeURIComponent(sourceId)}/${encodeURIComponent(targetId)}/${encodeURIComponent(edgeType)}`
          );

          if (graph?.hasEdge(edgeId)) {
            const attrs = graph.getEdgeAttributes(edgeId) as ADEdgeAttributes;
            const collapsed = attrs.collapsedTypes;

            if (collapsed && collapsed.length > 1) {
              // Remove this type from the collapsed group
              const remaining = collapsed.filter((t) => t !== edgeType);
              graph.setEdgeAttribute(edgeId, "collapsedTypes", remaining);

              if (remaining.length === 1) {
                // Revert to single edge
                graph.setEdgeAttribute(edgeId, "label", remaining[0]!);
                graph.setEdgeAttribute(edgeId, "edgeType", remaining[0]!);
              } else {
                graph.setEdgeAttribute(edgeId, "label", `${remaining[0]} +${remaining.length - 1}`);
              }

              // Refresh sidebar to show updated list
              const sourceLabel = graph.getNodeAttribute(sourceId, "label") || sourceId;
              const targetLabel = graph.getNodeAttribute(targetId, "label") || targetId;
              const updatedAttrs = graph.getEdgeAttributes(edgeId) as ADEdgeAttributes;
              updateDetailPanelForEdge(edgeId, updatedAttrs, sourceId, targetId, sourceLabel, targetLabel);
            } else {
              // Single edge or last type: drop entirely
              graph.dropEdge(edgeId);
              updateDetailPanel(null, null);
            }

            renderer?.refresh();
          } else {
            updateDetailPanel(null, null);
          }
        } catch (err) {
          console.error("Failed to delete relationship:", err);
          showError("Failed to delete relationship");
        }
      }
      break;
    }

    default:
      console.log(`Unknown relationship action: ${action}`);
  }
}

/** Toggle the owned status of a node */
async function toggleNodeOwned(nodeId: string): Promise<void> {
  try {
    // Get current status
    const status = await api.get<NodeStatusResponse>(`/api/graph/node/${encodeURIComponent(nodeId)}/status`);
    const newOwned = !status.owned;

    // Update the owned status
    await api.postNoContent(`/api/graph/node/${encodeURIComponent(nodeId)}/owned`, {
      owned: newOwned,
    });

    // Refresh the status indicators
    fetchNodeStatus(nodeId);

    // Update the overflow menu button text
    const toggleBtn = document.querySelector(`[data-action="toggle-owned"][data-node-id="${nodeId}"]`);
    if (toggleBtn) {
      const label = toggleBtn.querySelector("span");
      if (label) {
        label.textContent = newOwned ? "Unmark Owned" : "Mark Owned";
      }
    }
  } catch (err) {
    console.error("Failed to toggle owned status:", err);
    showError("Failed to update owned status");
  }
}

/** Load connections for a node */
async function loadConnections(nodeId: string, direction: string): Promise<void> {
  try {
    const response = await api.get<{
      nodes: Array<{ id: string; name: string; type: string; properties?: Record<string, unknown> }>;
      relationships: Array<{ source: string; target: string; type: string }>;
    }>(`/api/graph/node/${encodeURIComponent(nodeId)}/connections/${direction}`);

    if (!response.nodes || response.nodes.length === 0) {
      console.log(`No ${direction} connections found for node ${nodeId}`);
      return;
    }

    // Load the graph data
    loadGraphData({
      nodes: response.nodes.map((n) => ({
        id: n.id,
        name: n.name,
        type: n.type as ADNodeType,
        properties: n.properties,
      })),
      relationships: response.relationships.map((e) => ({
        source: e.source,
        target: e.target,
        type: e.type as import("../graph/types").ADEdgeType,
      })),
    });
  } catch (err) {
    console.error(`Failed to load ${direction} connections:`, err);
  }
}

/** Copy text to clipboard and show feedback */
async function copyToClipboard(text: string, element: HTMLElement): Promise<void> {
  try {
    await navigator.clipboard.writeText(text);

    // Show copied indicator
    element.classList.add("copied");
    setTimeout(() => {
      element.classList.remove("copied");
    }, 1500);
  } catch {
    // Fallback for older browsers
    const textArea = document.createElement("textarea");
    textArea.value = text;
    textArea.style.position = "fixed";
    textArea.style.left = "-9999px";
    document.body.appendChild(textArea);
    textArea.select();
    document.execCommand("copy");
    document.body.removeChild(textArea);

    element.classList.add("copied");
    setTimeout(() => {
      element.classList.remove("copied");
    }, 1500);
  }
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

/** Toggle both sidebars at once */
export function toggleSidebars(): void {
  // If either is visible, collapse both; otherwise expand both
  const shouldCollapse = !appState.navSidebarCollapsed || !appState.detailSidebarCollapsed;

  const navSidebar = document.getElementById("nav-sidebar");
  const navExpandBtn = document.getElementById("nav-sidebar-expand");
  const detailSidebar = document.getElementById("detail-sidebar");
  const detailExpandBtn = document.getElementById("detail-sidebar-expand");

  if (navSidebar && navExpandBtn) {
    appState.navSidebarCollapsed = shouldCollapse;
    if (shouldCollapse) {
      navSidebar.setAttribute("data-collapsed", "true");
      navSidebar.style.width = "0";
      navExpandBtn.classList.remove("hidden");
    } else {
      navSidebar.setAttribute("data-collapsed", "false");
      navSidebar.style.width = NAV_SIDEBAR_WIDTH;
      navExpandBtn.classList.add("hidden");
    }
  }

  if (detailSidebar && detailExpandBtn) {
    appState.detailSidebarCollapsed = shouldCollapse;
    if (shouldCollapse) {
      detailSidebar.setAttribute("data-collapsed", "true");
      detailSidebar.style.width = "0";
      detailExpandBtn.classList.remove("hidden");
    } else {
      detailSidebar.setAttribute("data-collapsed", "false");
      detailSidebar.style.width = DETAIL_SIDEBAR_WIDTH;
      detailExpandBtn.classList.add("hidden");
    }
  }
}

/** Get a pretty label for a property key */
function getPrettyLabel(key: string): string {
  const lower = key.toLowerCase();
  if (PROPERTY_LABELS[lower]) {
    return PROPERTY_LABELS[lower];
  }
  // Convert camelCase or snake_case to Title Case
  return key
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/** Field names that should be formatted as timestamps */
const TIMESTAMP_FIELDS = new Set([
  "created_at",
  "createdat",
  "updated_at",
  "updatedat",
  "whencreated",
  "whenchanged",
  "lastlogon",
  "lastlogontimestamp",
  "pwdlastset",
  "lastpasswordset",
  "accountexpires",
  "badpasswordtime",
  "lockouttime",
]);

/** Format a property value for display */
function formatValue(key: string, value: unknown): string {
  if (value === null || value === undefined) {
    return "—";
  }
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  if (typeof value === "number") {
    const keyLower = key.toLowerCase();

    // Check if this is a known timestamp field
    if (TIMESTAMP_FIELDS.has(keyLower)) {
      return formatTimestamp(value);
    }

    // Delegate heuristic timestamp detection to formatTimestamp as well,
    // which has the most complete range checks for FILETIME / Unix / JS ms.
    const tsResult = formatTimestamp(value);
    if (tsResult !== String(value)) {
      return tsResult;
    }

    // Regular number - use locale formatting for thousands separators
    return value.toLocaleString();
  }
  if (Array.isArray(value)) {
    return value.join(", ");
  }
  return String(value);
}

/** Format a Date to ISO format (YYYY-MM-DD HH:mm:ss) */
function formatDateISO(date: Date): string {
  const pad = (n: number) => n.toString().padStart(2, "0");
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
    `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
  );
}

/** Format a numeric timestamp to human-readable ISO string */
function formatTimestamp(value: number): string {
  // Handle special "never" values (0 or max int64)
  if (value === 0 || value > 9e18) {
    return "Never";
  }

  // Windows FILETIME (very large numbers, 100-nanosecond intervals since 1601)
  // Valid FILETIME range is roughly 1.3e17 to 2.5e17 for years 1970-2100
  if (value > 1e17 && value < 3e17) {
    const epoch = (value - 116444736000000000) / 10000;
    if (epoch > 0) {
      return formatDateISO(new Date(epoch));
    }
    return "Never";
  }

  // JS milliseconds timestamp (13 digits)
  if (value > 1000000000000) {
    return formatDateISO(new Date(value));
  }

  // Unix seconds timestamp (10 digits)
  if (value > 1000000000) {
    return formatDateISO(new Date(value * 1000));
  }

  // Small number - probably not a timestamp
  return String(value);
}

/** Render sorted property entries as detail-prop HTML */
function renderPropertyList(entries: [string, unknown][]): string {
  // Sort properties by priority, then alphabetically
  entries.sort((a, b) => {
    const aPriority = PROPERTY_PRIORITY[a[0].toLowerCase()] ?? 100;
    const bPriority = PROPERTY_PRIORITY[b[0].toLowerCase()] ?? 100;
    if (aPriority !== bPriority) return aPriority - bPriority;
    return a[0].localeCompare(b[0]);
  });

  let html = "";
  for (const [key, value] of entries) {
    const formatted = formatValue(key, value);
    const rawValue = value === null || value === undefined ? "" : String(value);
    html += `
      <div class="detail-prop">
        <span class="detail-prop-label">${escapeHtml(getPrettyLabel(key))}</span>
        <span class="detail-prop-value" data-value="${escapeHtml(rawValue)}" title="Click to copy">
          ${escapeHtml(formatted)}
        </span>
      </div>
    `;
  }
  return html;
}

/** Render the placeholder node warning banner HTML */
function renderPlaceholderBanner(): string {
  return `
    <div class="placeholder-warning">
      <svg class="placeholder-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <path d="M12 9v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
      </svg>
      <div class="placeholder-text">
        <span class="placeholder-title">Placeholder Node</span>
        <span class="placeholder-desc">
          This node was auto-created as a placeholder.
          <button class="placeholder-learn-more" data-action="show-placeholder-modal">Learn more</button>
        </span>
      </div>
    </div>
  `;
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

  // Build status indicators placeholder (fetched asynchronously from backend)
  const indicatorsHtml = `<span id="node-status-indicators" class="user-indicators">
    <span class="inline-flex items-center" title="Checking status...">${USER_INDICATORS.checking.icon}</span>
  </span>`;

  // Build main actions bar (filter by node type)
  const mainActionsHtml = MAIN_ACTIONS.filter(
    (action) => !action.nodeTypes || action.nodeTypes.includes(attrs.nodeType)
  )
    .map(
      (action) => `
    <button
      class="detail-action-btn"
      data-action="${action.id}"
      data-node-id="${escapeHtml(nodeId)}"
      title="${action.tooltip}"
      aria-label="${action.tooltip}"
    >
      ${action.icon}
      <span class="action-count" data-count-key="${action.countKey}" hidden></span>
    </button>
  `
    )
    .join("");

  // Build overflow menu
  const overflowMenuHtml = `
    <div class="detail-overflow-menu">
      <button class="detail-action-btn overflow-trigger" title="More actions" aria-label="More actions" aria-haspopup="true" aria-expanded="false">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="5" r="1.5"/>
          <circle cx="12" cy="12" r="1.5"/>
          <circle cx="12" cy="19" r="1.5"/>
        </svg>
      </button>
      <div class="overflow-dropdown" hidden>
        ${OVERFLOW_ACTIONS.map(
          (action) => `
          <button
            class="overflow-item${action.danger ? " danger" : ""}"
            data-action="${action.id}"
            data-node-id="${escapeHtml(nodeId)}"
          >
            ${action.icon}
            <span>${action.label}</span>
          </button>
        `
        ).join("")}
      </div>
    </div>
  `;

  const actionsHtml = mainActionsHtml + overflowMenuHtml;

  // Build properties list - show ALL properties
  let propsHtml = "";
  let needsFetch = false;
  if (attrs.properties) {
    propsHtml = renderPropertyList(Object.entries(attrs.properties));
  } else {
    // Properties not available - need to fetch them
    needsFetch = true;
    propsHtml = `
      <div class="flex items-center gap-2 text-sm text-gray-500">
        <svg class="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="10" stroke-opacity="0.25"/>
          <path d="M12 2a10 10 0 0 1 10 10" stroke-linecap="round"/>
        </svg>
        <span>Loading properties...</span>
      </div>
    `;
  }

  // Check if this is a placeholder node
  const isPlaceholder = attrs.properties?.placeholder === true;

  // Build placeholder warning banner if applicable
  const placeholderBanner = isPlaceholder ? renderPlaceholderBanner() : "";

  content.innerHTML = `
    <div class="detail-header">
      <div class="detail-header-top">
        <span class="detail-node-type node-badge" style="background-color: ${typeColor}">
          ${escapeHtml(attrs.nodeType)}
        </span>
        ${indicatorsHtml}
      </div>
      <h2 class="detail-node-name">${escapeHtml(attrs.label)}</h2>
      <div class="detail-actions">
        ${actionsHtml}
      </div>
    </div>

    <div id="placeholder-banner-container">${placeholderBanner}</div>

    <div class="detail-section">
      <h3 class="detail-section-title">Properties</h3>
      <div id="node-properties-content" class="detail-props">
        ${propsHtml}
      </div>
    </div>
  `;

  // Fetch node security status from backend
  fetchNodeStatus(nodeId);

  // Fetch and display connection counts
  fetchNodeCounts(nodeId);

  // Fetch properties if not already available
  if (needsFetch) {
    fetchNodeProperties(nodeId);
  }
}

/** Update the detail sidebar with relationship information */
export function updateDetailPanelForEdge(
  edgeId: string,
  attrs: ADEdgeAttributes,
  sourceId: string,
  targetId: string,
  sourceLabel: string,
  targetLabel: string
): void {
  const content = document.getElementById("detail-content");
  if (!content) return;

  // Clear any selected node state
  appState.selectedNodeId = null;
  appState.selectedEdgeId = edgeId;

  const types = attrs.collapsedTypes && attrs.collapsedTypes.length > 0 ? attrs.collapsedTypes : [attrs.edgeType];
  const isMulti = types.length > 1;

  // Header: show count for multi, type name for single
  const headerType = isMulti ? `${types.length} Relationships` : types[0] || "Unknown";
  const headerColor = isMulti ? "#6c757d" : EDGE_COLORS[types[0]!] || "#6c757d";
  const badgeLabel = isMulti ? "Relationships" : "Relationship";

  // Endpoints section
  const endpointsHtml = `
    <div class="detail-prop">
      <span class="detail-prop-label">Source</span>
      <span class="detail-prop-value" data-value="${escapeHtml(sourceId)}" title="Click to copy">
        ${escapeHtml(sourceLabel)}
      </span>
    </div>
    <div class="detail-prop">
      <span class="detail-prop-label">Target</span>
      <span class="detail-prop-value" data-value="${escapeHtml(targetId)}" title="Click to copy">
        ${escapeHtml(targetLabel)}
      </span>
    </div>
  `;

  // Relationship types list with edit and delete buttons
  const typesHtml = types
    .map((type) => {
      const color = EDGE_COLORS[type] || "#6c757d";
      return `
      <div class="detail-prop" style="flex-direction:row; align-items:center; justify-content:space-between; gap:8px">
        <span class="detail-node-type relationship-badge" style="background-color: ${color}; font-size: 0.75rem">
          ${escapeHtml(type)}
        </span>
        <div style="display:flex; gap:4px; flex-shrink:0">
          <button
            class="detail-action-btn"
            data-action="edit-relationship"
            data-relationship-id="${escapeHtml(edgeId)}"
            data-source-id="${escapeHtml(sourceId)}"
            data-target-id="${escapeHtml(targetId)}"
            data-relationship-type="${escapeHtml(type)}"
            title="Edit ${escapeHtml(type)}"
            aria-label="Edit ${escapeHtml(type)}"
            style="width:28px; height:28px; padding:4px"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M17 3a2.83 2.83 0 114 4L7.5 20.5 2 22l1.5-5.5L17 3z"/>
            </svg>
          </button>
          <button
            class="detail-action-btn danger"
            data-action="delete-relationship"
            data-relationship-id="${escapeHtml(edgeId)}"
            data-source-id="${escapeHtml(sourceId)}"
            data-target-id="${escapeHtml(targetId)}"
            data-relationship-type="${escapeHtml(type)}"
            title="Delete ${escapeHtml(type)}"
            aria-label="Delete ${escapeHtml(type)}"
            style="width:28px; height:28px; padding:4px"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M3 6h18M8 6V4h8v2M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6"/>
              <path d="M10 11v6M14 11v6"/>
            </svg>
          </button>
        </div>
      </div>
    `;
    })
    .join("");

  content.innerHTML = `
    <div class="detail-header">
      <div class="detail-header-top">
        <span class="detail-node-type relationship-badge" style="background-color: ${headerColor}">
          ${badgeLabel}
        </span>
      </div>
      <h2 class="detail-node-name">${escapeHtml(headerType)}</h2>
    </div>

    <div class="detail-section">
      <h3 class="detail-section-title">Endpoints</h3>
      <div class="detail-props">
        ${endpointsHtml}
      </div>
    </div>

    <div class="detail-section">
      <h3 class="detail-section-title">${isMulti ? "Relationship Types" : "Type"}</h3>
      <div class="detail-props">
        ${typesHtml}
      </div>
    </div>
  `;
}

/** Node response from API (with full properties) */
interface NodeResponse {
  id: string;
  name: string;
  type: string;
  properties: Record<string, unknown>;
}

/** Node counts response from API */
interface NodeCountsResponse {
  incoming: number;
  outgoing: number;
  adminTo: number;
  memberOf: number;
  members: number;
}

/** Node security status response from API */
interface NodeStatusResponse {
  owned: boolean;
  isEnterpriseAdmin: boolean;
  isDomainAdmin: boolean;
  tier: number;
  hasPathToHighTier: boolean;
  pathLength?: number;
}

/** Fetch and display node security status */
async function fetchNodeStatus(nodeId: string): Promise<void> {
  const container = document.getElementById("node-status-indicators");
  if (!container) return;

  try {
    const status = await api.get<NodeStatusResponse>(`/api/graph/node/${encodeURIComponent(nodeId)}/status`);

    // Check if we're still showing the same node
    if (appState.selectedNodeId !== nodeId) return;

    const indicators: string[] = [];

    // Owned indicator
    if (status.owned) {
      indicators.push(renderIndicator("owned"));
    }

    // Admin/tier indicators (mutually exclusive hierarchy)
    if (status.isEnterpriseAdmin) {
      indicators.push(renderIndicator("enterpriseAdmin"));
    } else if (status.isDomainAdmin) {
      indicators.push(renderIndicator("domainAdmin"));
    } else if (status.tier === 0) {
      indicators.push(renderIndicator("tierZero"));
    } else if (status.hasPathToHighTier) {
      const hops = status.pathLength ?? 0;
      indicators.push(
        `<span class="user-indicator cursor-pointer" data-action="show-path-to-hv" data-node-id="${nodeId}" title="${USER_INDICATORS.hasPath.tooltip} (${hops} hops) - click to show path">${USER_INDICATORS.hasPath.icon}</span>`
      );
    } else {
      indicators.push(
        `<span class="user-indicator" title="${USER_INDICATORS.noPath.tooltip}">${USER_INDICATORS.noPath.icon}</span>`
      );
    }

    container.innerHTML = indicators.join("");

    // Attach click handler for "path to tier 0" indicator
    const pathIndicator = container.querySelector('[data-action="show-path-to-hv"]');
    if (pathIndicator) {
      pathIndicator.addEventListener("click", () => showPathToTierZero(nodeId));
    }

    // Update the overflow menu button text based on owned status
    const toggleBtn = document.querySelector(`[data-action="toggle-owned"][data-node-id="${nodeId}"]`);
    if (toggleBtn) {
      const label = toggleBtn.querySelector("span");
      if (label) {
        label.textContent = status.owned ? "Unmark Owned" : "Mark Owned";
      }
    }
  } catch (err) {
    console.error("Failed to fetch node status:", err);
    if (appState.selectedNodeId === nodeId) {
      container.innerHTML = `<span class="user-indicator" title="Status check failed">${USER_INDICATORS.noPath.icon}</span>`;
    }
  }
}

/** Show path to tier-0 target when indicator is clicked */
async function showPathToTierZero(nodeId: string): Promise<void> {
  const escapedId = nodeId.replace(/'/g, "\\'");
  const query = `MATCH p = shortestPath((a)-[*1..]->(b)) WHERE a.objectid = '${escapedId}' AND (${ridWhereClause("b", HIGH_VALUE_RIDS)}) RETURN p`;

  try {
    const result = await executeQuery(query, { extractGraph: true });

    if (result.graph && result.graph.nodes.length > 0) {
      const pathGraph: RawADGraph = {
        nodes: result.graph.nodes.map((n) => ({
          id: n.id,
          name: n.name,
          type: n.type as ADNodeType,
          properties: n.properties,
        })),
        relationships: result.graph.relationships.map((e) => ({
          source: e.source,
          target: e.target,
          type: e.type as ADEdgeType,
        })),
      };
      loadGraphData(pathGraph);

      // Highlight the path after graph is loaded
      requestAnimationFrame(() => {
        const renderer = getRenderer();
        if (renderer) {
          const nodeIds = result.graph!.nodes.map((n) => n.id);
          renderer.highlightPath(nodeIds);
        }
      });
    }
  } catch (err) {
    // Don't show error if query was aborted (e.g., user started a new query)
    if (err instanceof QueryAbortedError) {
      console.debug("Path query was aborted:", err.message);
      return;
    }
    console.error("Failed to show path to tier-0 target:", err);
    showError("Failed to load path to tier-0 target");
  }
}

/** Fetch and display connection counts for a node */
async function fetchNodeCounts(nodeId: string): Promise<void> {
  try {
    const counts = await api.get<NodeCountsResponse>(`/api/graph/node/${encodeURIComponent(nodeId)}/counts`);

    // Check if we're still showing the same node
    if (appState.selectedNodeId !== nodeId) return;

    // Map API field names to countKey values
    const countMap: Record<string, number> = {
      incoming: counts.incoming,
      outgoing: counts.outgoing,
      adminTo: counts.adminTo,
      memberOf: counts.memberOf,
      members: counts.members,
    };

    // Update all count badges
    for (const [key, count] of Object.entries(countMap)) {
      const badge = document.querySelector<HTMLSpanElement>(`.action-count[data-count-key="${key}"]`);
      if (badge) {
        if (count > 0) {
          badge.textContent = count > 99 ? "99+" : String(count);
          badge.hidden = false;
        } else {
          badge.hidden = true;
        }
      }
    }
  } catch (err) {
    console.error("Failed to fetch node counts:", err);
  }
}

/** Fetch and display properties for a node */
async function fetchNodeProperties(nodeId: string): Promise<void> {
  const propsContainer = document.getElementById("node-properties-content");
  if (!propsContainer) return;

  try {
    const node = await api.get<NodeResponse>(`/api/graph/node/${encodeURIComponent(nodeId)}`);

    // Check if we're still showing the same node
    if (appState.selectedNodeId !== nodeId) return;

    // Check if this is a placeholder node and show banner if needed
    const bannerContainer = document.getElementById("placeholder-banner-container");
    if (bannerContainer && node.properties.placeholder === true) {
      bannerContainer.innerHTML = renderPlaceholderBanner();
    }

    // Build properties HTML
    const entries = Object.entries(node.properties);
    if (entries.length === 0) {
      propsContainer.innerHTML = `<p class="text-sm text-gray-500">No properties available</p>`;
      return;
    }

    propsContainer.innerHTML = renderPropertyList(entries);
  } catch (err) {
    console.error("Failed to fetch node properties:", err);
    if (appState.selectedNodeId === nodeId) {
      propsContainer.innerHTML = `<p class="text-sm text-gray-500">Failed to load properties</p>`;
    }
  }
}
