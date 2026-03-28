/**
 * Keyboard Shortcuts
 *
 * Global keyboard shortcuts for the application.
 */

import { dispatchAction, type StaticAction } from "./actions";
import { createModal, type ModalHandle } from "../utils/modal";

/** Shortcut definition with display info */
interface ShortcutDef {
  key: string;
  ctrl?: boolean;
  shift?: boolean;
  alt?: boolean;
  action: StaticAction;
  label?: string;
  category?: string;
}

/** Keyboard shortcut definitions */
const shortcuts: ShortcutDef[] = [
  // File menu
  { key: "d", ctrl: true, shift: true, action: "connect-db", label: "Connect to Database", category: "File" },
  { key: "e", ctrl: true, action: "export-png", label: "Export PNG", category: "File" },
  { key: ",", ctrl: true, action: "settings", label: "Settings", category: "File" },
  { key: "q", ctrl: true, action: "quit", label: "Quit", category: "File" },

  // View menu
  { key: "t", ctrl: true, action: "toggle-theme", label: "Toggle Theme", category: "View" },
  { key: "\\", ctrl: true, action: "toggle-sidebars", label: "Toggle Sidebars", category: "View" },
  { key: "b", ctrl: true, action: "toggle-nav-sidebar", label: "Toggle Navigation", category: "View" },
  { key: "d", ctrl: true, action: "toggle-detail-sidebar", label: "Toggle Details", category: "View" },
  { key: " ", ctrl: true, action: "toggle-label-visibility", label: "Toggle Labels", category: "View" },
  { key: "m", ctrl: true, action: "toggle-magnifier", label: "Toggle Magnifier", category: "View" },
  { key: "+", ctrl: true, action: "zoom-in", label: "Zoom In", category: "View" },
  { key: "=", ctrl: true, action: "zoom-in" }, // Also handle = for zoom in (no label, duplicate)
  { key: "-", ctrl: true, action: "zoom-out", label: "Zoom Out", category: "View" },
  { key: "0", ctrl: true, action: "zoom-reset", label: "Reset Zoom", category: "View" },
  { key: "1", ctrl: true, action: "fit-graph", label: "Fit Graph to View", category: "View" },
  { key: "F11", action: "fullscreen", label: "Toggle Fullscreen", category: "View" },

  // Tools menu
  { key: "Enter", ctrl: true, action: "run-query", label: "Run Query", category: "Tools" },
  { key: "i", ctrl: true, action: "insights", label: "Security Insights", category: "Tools" },
  { key: "l", ctrl: true, action: "layout-graph", label: "Re-layout Graph", category: "Tools" },
  { key: "l", ctrl: true, shift: true, action: "cycle-layout", label: "Cycle Layout", category: "Tools" },

  // Help menu
  { key: "?", ctrl: true, action: "keyboard-shortcuts", label: "Keyboard Shortcuts", category: "Help" },
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

/** Format a shortcut for display */
function formatShortcut(shortcut: ShortcutDef): string {
  const parts: string[] = [];
  if (shortcut.ctrl) parts.push("Ctrl");
  if (shortcut.shift) parts.push("Shift");
  if (shortcut.alt) parts.push("Alt");

  // Format key - capitalize letters, keep special keys as-is
  let key = shortcut.key;
  const specialKeys = ["\\", "+", "-", ",", "?", "Enter"];
  if (!specialKeys.includes(key) && !key.startsWith("F")) {
    key = key.toUpperCase();
  }

  parts.push(key);
  return parts.join("+");
}

/** Cached modal handle for keyboard shortcuts */
let shortcutsModal: ModalHandle | null = null;

/** Show keyboard shortcuts modal */
export function showKeyboardShortcuts(): void {
  if (shortcutsModal) {
    shortcutsModal.open();
    return;
  }

  // Group shortcuts by category
  const byCategory = new Map<string, ShortcutDef[]>();
  for (const shortcut of shortcuts) {
    if (!shortcut.label || !shortcut.category) continue; // Skip duplicates
    const existing = byCategory.get(shortcut.category) ?? [];
    existing.push(shortcut);
    byCategory.set(shortcut.category, existing);
  }

  // Build body HTML
  let categoriesHtml = "";
  for (const [category, categoryShortcuts] of byCategory) {
    let shortcutsHtml = "";
    for (const shortcut of categoryShortcuts) {
      shortcutsHtml += `
        <div class="shortcut-row">
          <span class="shortcut-label">${shortcut.label}</span>
          <kbd class="shortcut-key">${formatShortcut(shortcut)}</kbd>
        </div>
      `;
    }
    categoriesHtml += `
      <div class="shortcut-category">
        <h3 class="shortcut-category-title">${category}</h3>
        ${shortcutsHtml}
      </div>
    `;
  }

  shortcutsModal = createModal({
    id: "shortcuts-modal",
    title: "Keyboard Shortcuts",
    sizeClass: "modal-lg",
    buttons: [{ label: "Close", action: "close", className: "btn btn-primary" }],
  });

  shortcutsModal.body.innerHTML = `
    <div class="shortcuts-grid">
      ${categoriesHtml}
    </div>
  `;

  shortcutsModal.open();
}
