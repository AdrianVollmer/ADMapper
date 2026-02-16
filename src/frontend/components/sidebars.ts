/**
 * Sidebar Components
 *
 * Handles collapsible navigation and detail sidebars.
 */

import { appState } from "../main";
import type { ADNodeAttributes } from "../graph/types";
import { NODE_COLORS } from "../graph/theme";
import { escapeHtml } from "../utils/html";
import { api } from "../api/client";
import type { PathResponse } from "../api/types";

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

/** Well-known high-value RIDs */
const HIGH_VALUE_RIDS = new Set([
  "-500", // Built-in Administrator
  "-502", // KRBTGT
  "-512", // Domain Admins
  "-516", // Domain Controllers
  "-518", // Schema Admins
  "-519", // Enterprise Admins
  "-544", // Builtin Administrators
]);

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
  highValue: {
    icon: `<svg viewBox="0 0 24 24" fill="currentColor" class="w-4 h-4 text-orange-500">
      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>
    </svg>`,
    tooltip: "High Value Target",
  },
  hasPath: {
    icon: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="w-4 h-4 text-blue-500">
      <path d="M13 17l5-5-5-5M6 17l5-5-5-5"/>
    </svg>`,
    tooltip: "Has Path to High Value",
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
    tooltip: "No path to high value targets",
  },
};

/** Check if a SID ends with a high-value RID */
function hasHighValueRID(sid: string | undefined): boolean {
  if (!sid) return false;
  for (const rid of HIGH_VALUE_RIDS) {
    if (sid.endsWith(rid)) return true;
  }
  return false;
}

/** Check if user is in a specific admin group based on properties */
function isInAdminGroup(props: Record<string, unknown>, groupRid: string): boolean {
  // Check admincount property
  if (groupRid === "-512" || groupRid === "-519") {
    const adminCount = props.admincount ?? props.AdminCount;
    if (adminCount === true || adminCount === 1 || adminCount === "1") {
      return true;
    }
  }
  return false;
}

/** Get the domain SID from a user SID */
function getDomainSID(userSid: string): string | null {
  // User SID format: S-1-5-21-DOMAIN-RID
  // Domain SID is everything except the last part
  const parts = userSid.split("-");
  if (parts.length < 5) return null;
  return parts.slice(0, -1).join("-");
}

/** Render a user status indicator */
function renderIndicator(type: keyof typeof USER_INDICATORS): string {
  const indicator = USER_INDICATORS[type];
  return `<span class="user-indicator" title="${indicator.tooltip}">${indicator.icon}</span>`;
}

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

    // Check if it's a timestamp by value heuristics
    // JS milliseconds timestamp (13 digits, 2001-2050 range)
    if (value > 1000000000000 && value < 2500000000000) {
      return formatDateISO(new Date(value));
    }
    // Unix seconds timestamp (10 digits, 2001-2050 range)
    if (value > 1000000000 && value < 2500000000) {
      return formatDateISO(new Date(value * 1000));
    }
    // Windows FILETIME (100-nanosecond intervals since 1601)
    if (value > 100000000000000000) {
      const epoch = (value - 116444736000000000) / 10000;
      if (epoch > 0) {
        return formatDateISO(new Date(epoch));
      }
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

  // Build user status indicators (for User nodes only)
  let indicatorsHtml = "";
  let needsPathCheck = false;
  if (attrs.nodeType === "User" && attrs.properties) {
    const props = attrs.properties;
    const sid = (props.objectsid ?? props.objectSid ?? props.ObjectSid ?? "") as string;
    const owned = props.owned ?? props.Owned;
    const highValue = props.highvalue ?? props.HighValue ?? props.highValue;

    const indicators: string[] = [];

    // Check owned status
    if (owned === true || owned === "true" || owned === 1) {
      indicators.push(renderIndicator("owned"));
    }

    // Check if Enterprise Admin (RID -519)
    if (sid.endsWith("-519") || isInAdminGroup(props, "-519")) {
      indicators.push(renderIndicator("enterpriseAdmin"));
    }
    // Check if Domain Admin (RID -512)
    else if (sid.endsWith("-512") || isInAdminGroup(props, "-512")) {
      indicators.push(renderIndicator("domainAdmin"));
    }
    // Check high value based on RID or property
    else if (highValue === true || highValue === "true" || hasHighValueRID(sid)) {
      indicators.push(renderIndicator("highValue"));
    }
    // Need to check for path to high value
    else {
      needsPathCheck = true;
      indicators.push(
        `<span id="path-check-indicator" class="inline-flex items-center" title="Checking paths...">${USER_INDICATORS.checking.icon}</span>`
      );
    }

    if (indicators.length > 0) {
      indicatorsHtml = `<span class="user-indicators">${indicators.join("")}</span>`;
    }
  }

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
        ${indicatorsHtml}
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

  // Async path check for users without obvious high-value indicators
  if (needsPathCheck && attrs.properties) {
    checkPathToHighValue(nodeId, attrs.properties);
  }
}

/** Check if user has a path to high-value targets */
async function checkPathToHighValue(nodeId: string, props: Record<string, unknown>): Promise<void> {
  const indicator = document.getElementById("path-check-indicator");
  if (!indicator) return;

  const sid = (props.objectsid ?? props.objectSid ?? props.ObjectSid ?? "") as string;
  const domainSid = getDomainSID(sid);

  if (!domainSid) {
    indicator.innerHTML = USER_INDICATORS.noPath.icon;
    indicator.title = USER_INDICATORS.noPath.tooltip;
    return;
  }

  // High-value targets to check paths to
  const targets = [
    `${domainSid}-512`, // Domain Admins
    `${domainSid}-519`, // Enterprise Admins
    `${domainSid}-518`, // Schema Admins
  ];

  try {
    for (const target of targets) {
      const response = await api.get<PathResponse>(
        `/api/graph/path?from=${encodeURIComponent(nodeId)}&to=${encodeURIComponent(target)}`
      );

      // Check if we're still showing the same node
      if (appState.selectedNodeId !== nodeId) return;

      if (response.found && response.path.length > 1) {
        indicator.innerHTML = USER_INDICATORS.hasPath.icon;
        indicator.title = `${USER_INDICATORS.hasPath.tooltip} (${response.path.length - 1} hops)`;
        indicator.classList.add("cursor-pointer");
        return;
      }
    }

    // No path found to any target
    if (appState.selectedNodeId === nodeId) {
      indicator.innerHTML = USER_INDICATORS.noPath.icon;
      indicator.title = USER_INDICATORS.noPath.tooltip;
    }
  } catch (err) {
    console.error("Path check failed:", err);
    if (appState.selectedNodeId === nodeId) {
      indicator.innerHTML = USER_INDICATORS.noPath.icon;
      indicator.title = "Path check failed";
    }
  }
}
