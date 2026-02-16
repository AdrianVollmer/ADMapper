/**
 * Keyboard Shortcuts
 *
 * Global keyboard shortcuts for the application.
 */

import { dispatchAction } from "./actions";

/** Keyboard shortcut definitions */
const shortcuts: Array<{
  key: string;
  ctrl?: boolean;
  shift?: boolean;
  alt?: boolean;
  action: string;
}> = [
  // File menu
  { key: "n", ctrl: true, action: "new-project" },
  { key: "o", ctrl: true, action: "open-file" },
  { key: "e", ctrl: true, action: "export" },
  { key: ",", ctrl: true, action: "settings" },
  { key: "q", ctrl: true, action: "quit" },

  // Edit menu
  { key: "a", ctrl: true, action: "select-all" },
  { key: "f", ctrl: true, action: "find" },

  // View menu
  { key: "\\", ctrl: true, action: "toggle-sidebars" },
  { key: "b", ctrl: true, action: "toggle-nav-sidebar" },
  { key: "d", ctrl: true, action: "toggle-detail-sidebar" },
  { key: "+", ctrl: true, action: "zoom-in" },
  { key: "=", ctrl: true, action: "zoom-in" }, // Also handle = for zoom in
  { key: "-", ctrl: true, action: "zoom-out" },
  { key: "0", ctrl: true, action: "zoom-reset" },
  { key: "1", ctrl: true, action: "fit-graph" },
  { key: "F11", action: "fullscreen" },

  // Tools menu
  { key: "Enter", ctrl: true, action: "run-query" },
  { key: "l", ctrl: true, action: "layout-graph" },

  // Help menu
  { key: "F1", action: "documentation" },
  { key: "?", ctrl: true, action: "keyboard-shortcuts" },
];

/** Initialize keyboard shortcuts */
export function initKeyboardShortcuts(): void {
  document.addEventListener("keydown", handleKeydown);
}

/** Handle keydown events */
function handleKeydown(e: KeyboardEvent): void {
  // Skip if in input field
  if (isInputFocused()) {
    // Allow Escape to blur input
    if (e.key === "Escape") {
      (document.activeElement as HTMLElement)?.blur();
    }
    return;
  }

  // Find matching shortcut
  for (const shortcut of shortcuts) {
    if (matchesShortcut(e, shortcut)) {
      e.preventDefault();
      dispatchAction(shortcut.action);
      return;
    }
  }
}

/** Check if an input element is focused */
function isInputFocused(): boolean {
  const active = document.activeElement;
  if (!active) return false;

  const tagName = active.tagName.toLowerCase();
  if (tagName === "input" || tagName === "textarea" || tagName === "select") {
    return true;
  }

  if (active.getAttribute("contenteditable") === "true") {
    return true;
  }

  return false;
}

/** Check if a keyboard event matches a shortcut definition */
function matchesShortcut(
  e: KeyboardEvent,
  shortcut: { key: string; ctrl?: boolean; shift?: boolean; alt?: boolean }
): boolean {
  // Check modifiers
  if (shortcut.ctrl && !e.ctrlKey && !e.metaKey) return false;
  if (!shortcut.ctrl && (e.ctrlKey || e.metaKey)) return false;
  if (shortcut.shift && !e.shiftKey) return false;
  if (!shortcut.shift && e.shiftKey) return false;
  if (shortcut.alt && !e.altKey) return false;
  if (!shortcut.alt && e.altKey) return false;

  // Check key
  return e.key.toLowerCase() === shortcut.key.toLowerCase();
}
