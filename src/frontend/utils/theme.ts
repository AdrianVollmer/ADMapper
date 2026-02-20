/**
 * Theme Utility
 *
 * Manages application theme switching between dark and light modes.
 */

import type { Theme } from "../api/types";

/**
 * Apply a theme to the document.
 * Sets the data-theme attribute on body and manages the 'dark' class on html.
 * Dispatches a 'themechange' event for components that need to react.
 */
export function applyTheme(theme: Theme): void {
  document.body.setAttribute("data-theme", theme);

  if (theme === "dark") {
    document.documentElement.classList.add("dark");
  } else {
    document.documentElement.classList.remove("dark");
  }

  // Notify components that need to react to theme changes
  window.dispatchEvent(new CustomEvent("themechange", { detail: { theme } }));
}

/**
 * Get the currently applied theme.
 */
export function getCurrentTheme(): Theme {
  return document.body.getAttribute("data-theme") === "light" ? "light" : "dark";
}
