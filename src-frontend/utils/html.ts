/**
 * HTML utility functions for safe string manipulation.
 */

/**
 * Escape HTML special characters to prevent XSS attacks.
 * Uses the browser's built-in text content handling for safe escaping.
 */
export function escapeHtml(str: string): string {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}
