/**
 * Sidebar Components
 *
 * Handles collapsible navigation and detail sidebars.
 */

import { appState } from "../main";
import type { ADNodeAttributes } from "../graph/types";
import { NODE_COLORS } from "../graph/theme";
import { escapeHtml } from "../utils/html";

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
  highvalue: 52,
  sensitive: 53,

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
  objectid: "Object ID",
  objectsid: "Object SID",
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
  highvalue: "High Value",
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

/** Action definitions with icons */
const ACTIONS = [
  {
    id: "expand-node",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="12" cy="12" r="3"/>
      <path d="M12 5v2M12 17v2M5 12h2M17 12h2M7.05 7.05l1.41 1.41M15.54 15.54l1.41 1.41M7.05 16.95l1.41-1.41M15.54 8.46l1.41-1.41"/>
    </svg>`,
    tooltip: "Expand Connections",
  },
  {
    id: "find-path-from",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="5" cy="12" r="2"/>
      <circle cx="19" cy="12" r="2"/>
      <path d="M7 12h10"/>
      <path d="M14 8l4 4-4 4"/>
    </svg>`,
    tooltip: "Find Path From Here",
  },
  {
    id: "find-path-to",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <circle cx="5" cy="12" r="2"/>
      <circle cx="19" cy="12" r="2"/>
      <path d="M7 12h10"/>
      <path d="M10 8l-4 4 4 4"/>
    </svg>`,
    tooltip: "Find Path To Here",
  },
  {
    id: "set-start-node",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <path d="M3 12h4l3 9 4-18 3 9h4"/>
    </svg>`,
    tooltip: "Set as Start Node",
  },
  {
    id: "set-end-node",
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
      <path d="M4 15s1-1 4-1 5 2 8 2 4-1 4-1V3s-1 1-4 1-5-2-8-2-4 1-4 1z"/>
      <line x1="4" y1="22" x2="4" y2="15"/>
    </svg>`,
    tooltip: "Set as End Node",
  },
];

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

  // Set up click-to-copy for property values
  document.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;
    const valueEl = target.closest(".detail-prop-value") as HTMLElement;
    if (!valueEl) return;

    const text = valueEl.getAttribute("data-value") || valueEl.textContent || "";
    copyToClipboard(text, valueEl);
  });
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

    // Check if it's a timestamp by value heuristics
    // JS milliseconds timestamp (13 digits, 2001-2050 range)
    if (value > 1000000000000 && value < 2500000000000) {
      return new Date(value).toLocaleString();
    }
    // Unix seconds timestamp (10 digits, 2001-2050 range)
    if (value > 1000000000 && value < 2500000000) {
      return new Date(value * 1000).toLocaleString();
    }
    // Windows FILETIME (100-nanosecond intervals since 1601)
    if (value > 100000000000000000) {
      const epoch = (value - 116444736000000000) / 10000;
      if (epoch > 0) {
        return new Date(epoch).toLocaleString();
      }
    }
    return value.toLocaleString();
  }
  if (Array.isArray(value)) {
    return value.join(", ");
  }
  return String(value);
}

/** Format a numeric timestamp to human-readable string */
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
      return new Date(epoch).toLocaleString();
    }
    return "Never";
  }

  // JS milliseconds timestamp (13 digits)
  if (value > 1000000000000) {
    return new Date(value).toLocaleString();
  }

  // Unix seconds timestamp (10 digits)
  if (value > 1000000000) {
    return new Date(value * 1000).toLocaleString();
  }

  // Small number - probably not a timestamp
  return String(value);
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

  // Build actions bar
  const actionsHtml = ACTIONS.map(
    (action) => `
    <button
      class="detail-action-btn"
      data-action="${action.id}"
      data-node-id="${escapeHtml(nodeId)}"
      title="${action.tooltip}"
      aria-label="${action.tooltip}"
    >
      ${action.icon}
    </button>
  `
  ).join("");

  // Build properties list - show ALL properties
  let propsHtml = "";
  if (attrs.properties) {
    // Sort properties by priority, then alphabetically
    const entries = Object.entries(attrs.properties);
    entries.sort((a, b) => {
      const aPriority = PROPERTY_PRIORITY[a[0].toLowerCase()] ?? 100;
      const bPriority = PROPERTY_PRIORITY[b[0].toLowerCase()] ?? 100;
      if (aPriority !== bPriority) return aPriority - bPriority;
      return a[0].localeCompare(b[0]);
    });

    for (const [key, value] of entries) {
      const formatted = formatValue(key, value);
      const rawValue = value === null || value === undefined ? "" : String(value);
      propsHtml += `
        <div class="detail-prop">
          <span class="detail-prop-label">${escapeHtml(getPrettyLabel(key))}</span>
          <span class="detail-prop-value" data-value="${escapeHtml(rawValue)}" title="Click to copy">
            ${escapeHtml(formatted)}
          </span>
        </div>
      `;
    }
  }

  content.innerHTML = `
    <div class="detail-header">
      <div class="detail-header-top">
        <span class="detail-node-type node-badge ${typeLower}" style="background-color: ${typeColor}">
          ${escapeHtml(attrs.nodeType)}
        </span>
      </div>
      <h2 class="detail-node-name">${escapeHtml(attrs.label)}</h2>
      <div class="detail-actions">
        ${actionsHtml}
      </div>
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
        : `
    <div class="detail-section">
      <p class="text-sm text-gray-500">No properties available</p>
    </div>
    `
    }
  `;
}
