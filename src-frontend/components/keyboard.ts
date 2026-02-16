/**
 * Keyboard Shortcuts
 *
 * Global keyboard shortcuts for the application.
 */

import { dispatchAction } from "./actions";

/** Shortcut definition with display info */
interface ShortcutDef {
  key: string;
  ctrl?: boolean;
  shift?: boolean;
  alt?: boolean;
  action: string;
  label?: string;
  category?: string;
}

/** Keyboard shortcut definitions */
const shortcuts: ShortcutDef[] = [
  // File menu
  { key: "n", ctrl: true, action: "new-project", label: "New Project", category: "File" },
  { key: "o", ctrl: true, action: "open-file", label: "Open File", category: "File" },
  { key: "e", ctrl: true, action: "export-png", label: "Export PNG", category: "File" },
  { key: ",", ctrl: true, action: "settings", label: "Settings", category: "File" },
  { key: "q", ctrl: true, action: "quit", label: "Quit", category: "File" },

  // Edit menu
  { key: "a", ctrl: true, action: "select-all", label: "Select All", category: "Edit" },
  { key: "f", ctrl: true, action: "find", label: "Find", category: "Edit" },

  // View menu
  { key: "\\", ctrl: true, action: "toggle-sidebars", label: "Toggle Sidebars", category: "View" },
  { key: "b", ctrl: true, action: "toggle-nav-sidebar", label: "Toggle Navigation", category: "View" },
  { key: "d", ctrl: true, action: "toggle-detail-sidebar", label: "Toggle Details", category: "View" },
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

  // Help menu
  { key: "F1", action: "documentation", label: "Documentation", category: "Help" },
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

/** Show keyboard shortcuts modal */
export function showKeyboardShortcuts(): void {
  // Check if modal already exists
  let modal = document.getElementById("shortcuts-modal");
  if (modal) {
    modal.hidden = false;
    return;
  }

  // Group shortcuts by category
  const byCategory = new Map<string, ShortcutDef[]>();
  for (const shortcut of shortcuts) {
    if (!shortcut.label || !shortcut.category) continue; // Skip duplicates
    const existing = byCategory.get(shortcut.category) || [];
    existing.push(shortcut);
    byCategory.set(shortcut.category, existing);
  }

  // Build modal HTML
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

  modal = document.createElement("div");
  modal.id = "shortcuts-modal";
  modal.className = "modal-overlay";
  modal.innerHTML = `
    <div class="modal-content modal-lg">
      <div class="modal-header">
        <h2 class="modal-title">Keyboard Shortcuts</h2>
        <button class="modal-close" data-action="close-shortcuts-modal" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body">
        <div class="shortcuts-grid">
          ${categoriesHtml}
        </div>
      </div>
      <div class="modal-footer">
        <button class="btn btn-primary" data-action="close-shortcuts-modal">Close</button>
      </div>
    </div>
  `;

  document.body.appendChild(modal);

  // Handle close actions
  modal.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;
    if (target === modal || target.closest("[data-action='close-shortcuts-modal']")) {
      modal!.hidden = true;
    }
  });

  // Close on Escape
  const handleEscape = (e: KeyboardEvent) => {
    if (e.key === "Escape" && !modal!.hidden) {
      modal!.hidden = true;
      e.preventDefault();
    }
  };
  document.addEventListener("keydown", handleEscape);
}
