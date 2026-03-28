/**
 * Detail Panel Formatter
 *
 * Property formatting logic for the detail sidebar panel.
 * Handles property display ordering, labeling, value formatting,
 * and rendering of property lists and placeholder banners.
 */

import { escapeHtml } from "../utils/html";

/**
 * Priority order for properties in the detail panel.
 * Lower numbers appear first. Properties not listed get a default priority of 100.
 */
export const PROPERTY_PRIORITY: Record<string, number> = {
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
export const PROPERTY_LABELS: Record<string, string> = {
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

/** Get a pretty label for a property key */
export function getPrettyLabel(key: string): string {
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
export const TIMESTAMP_FIELDS = new Set([
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
export function formatValue(key: string, value: unknown): string {
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
export function formatDateISO(date: Date): string {
  const pad = (n: number) => n.toString().padStart(2, "0");
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
    `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
  );
}

/** Format a numeric timestamp to human-readable ISO string */
export function formatTimestamp(value: number): string {
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
export function renderPropertyList(entries: [string, unknown][]): string {
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
export function renderPlaceholderBanner(): string {
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
