/**
 * HTML utility functions for safe string manipulation.
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
  return str.replace(/[&<>"']/g, (c) => escapeMap[c]);
}
