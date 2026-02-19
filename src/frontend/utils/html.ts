/**
 * HTML and string utility functions for safe manipulation.
 */

const escapeMap: Record<string, string> = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
};

/**
 * Escape HTML special characters to prevent XSS attacks.
 * Uses string replacement for efficiency (no DOM allocation).
 */
export function escapeHtml(str: string): string {
  return str.replace(/[&<>"']/g, (c) => escapeMap[c]!);
}

/**
 * Redact credentials from a database URL for safe logging/display.
 * Replaces password portion with asterisks while preserving URL structure.
 *
 * Examples:
 *   neo4j://user:password@host:7687 -> neo4j://user:****@host:7687
 *   falkordb://user:pass@host:6379  -> falkordb://user:****@host:6379
 */
export function redactUrlCredentials(url: string): string {
  // Match scheme://user:password@rest or scheme://user@rest
  return url.replace(/^([a-z][a-z0-9+.-]*:\/\/)([^:@]+):([^@]+)@/i, "$1$2:****@");
}
