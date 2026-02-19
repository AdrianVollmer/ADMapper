/**
 * Settings Component
 *
 * Modal for configuring application settings:
 * - Theme (dark/light)
 * - Default graph layout (force/hierarchical)
 */

import { api } from "../api/client";
import type { Settings, Theme, GraphLayout } from "../api/types";
import { applyTheme } from "../utils/theme";
import { showSuccess, showError } from "../utils/notifications";

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Current settings (cached) */
let currentSettings: Settings = {
  theme: "dark",
  defaultGraphLayout: "force",
};

/** Initialize the settings modal */
export function initSettings(): void {
  // Modal is created on demand
}

/**
 * Apply initial settings on app startup.
 * Loads settings from API and applies the theme immediately.
 */
export async function applyInitialSettings(): Promise<void> {
  try {
    currentSettings = await api.get<Settings>("/api/settings");
    applyTheme(currentSettings.theme);
  } catch {
    // Use defaults if settings can't be loaded
    applyTheme("dark");
  }
}

/** Get the default graph layout from settings */
export function getDefaultLayout(): GraphLayout {
  return currentSettings.defaultGraphLayout;
}

/** Toggle between dark and light theme */
export async function toggleTheme(): Promise<void> {
  const newTheme = currentSettings.theme === "dark" ? "light" : "dark";

  const newSettings: Settings = {
    ...currentSettings,
    theme: newTheme,
  };

  try {
    currentSettings = await api.put<Settings>("/api/settings", newSettings);
    applyTheme(currentSettings.theme);
  } catch {
    // Apply locally even if save fails
    currentSettings.theme = newTheme;
    applyTheme(newTheme);
  }
}

/** Open the settings modal */
export async function openSettings(): Promise<void> {
  if (!modalEl) {
    createModal();
  }

  // Load fresh settings
  try {
    currentSettings = await api.get<Settings>("/api/settings");
  } catch {
    // Use cached settings on error
  }

  populateForm();
  modalEl!.hidden = false;
}

/** Close the modal */
function closeModal(): void {
  if (modalEl) {
    modalEl.hidden = true;
  }
}

/** Create the modal element */
function createModal(): void {
  modalEl = document.createElement("div");
  modalEl.id = "settings-modal";
  modalEl.className = "modal-overlay";
  modalEl.innerHTML = `
    <div class="modal-content">
      <div class="modal-header">
        <h2 class="modal-title">Settings</h2>
        <button class="modal-close" data-action="close" aria-label="Close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body">
        <div class="settings-form">
          <!-- Theme -->
          <div class="form-group">
            <label class="form-label">Theme</label>
            <div class="settings-option-group" id="theme-options">
              <label class="settings-option">
                <input type="radio" name="theme" value="dark">
                <span class="settings-option-content">
                  <svg class="settings-option-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
                  </svg>
                  <span class="settings-option-label">Dark</span>
                </span>
              </label>
              <label class="settings-option">
                <input type="radio" name="theme" value="light">
                <span class="settings-option-content">
                  <svg class="settings-option-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="12" cy="12" r="5"/>
                    <line x1="12" y1="1" x2="12" y2="3"/>
                    <line x1="12" y1="21" x2="12" y2="23"/>
                    <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/>
                    <line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/>
                    <line x1="1" y1="12" x2="3" y2="12"/>
                    <line x1="21" y1="12" x2="23" y2="12"/>
                    <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/>
                    <line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>
                  </svg>
                  <span class="settings-option-label">Light</span>
                </span>
              </label>
            </div>
          </div>

          <!-- Default Graph Layout -->
          <div class="form-group">
            <label class="form-label">Default Graph Layout</label>
            <p class="form-help">Layout used when loading a new graph</p>
            <div class="settings-layout-grid" id="layout-options">
              <label class="settings-option">
                <input type="radio" name="layout" value="force">
                <span class="settings-option-content">
                  <svg class="settings-option-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="12" cy="12" r="2"/>
                    <circle cx="6" cy="6" r="2"/>
                    <circle cx="18" cy="6" r="2"/>
                    <circle cx="6" cy="18" r="2"/>
                    <circle cx="18" cy="18" r="2"/>
                    <line x1="8" y1="8" x2="10" y2="10"/>
                    <line x1="14" y1="10" x2="16" y2="8"/>
                    <line x1="8" y1="16" x2="10" y2="14"/>
                    <line x1="14" y1="14" x2="16" y2="16"/>
                  </svg>
                  <span class="settings-option-label">Force</span>
                </span>
              </label>
              <label class="settings-option">
                <input type="radio" name="layout" value="hierarchical">
                <span class="settings-option-content">
                  <svg class="settings-option-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <rect x="9" y="3" width="6" height="4" rx="1"/>
                    <rect x="3" y="17" width="6" height="4" rx="1"/>
                    <rect x="15" y="17" width="6" height="4" rx="1"/>
                    <line x1="12" y1="7" x2="12" y2="12"/>
                    <line x1="6" y1="12" x2="18" y2="12"/>
                    <line x1="6" y1="12" x2="6" y2="17"/>
                    <line x1="18" y1="12" x2="18" y2="17"/>
                  </svg>
                  <span class="settings-option-label">Hierarchical</span>
                </span>
              </label>
              <label class="settings-option">
                <input type="radio" name="layout" value="grid">
                <span class="settings-option-content">
                  <svg class="settings-option-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <rect x="3" y="3" width="7" height="7" rx="1"/>
                    <rect x="14" y="3" width="7" height="7" rx="1"/>
                    <rect x="3" y="14" width="7" height="7" rx="1"/>
                    <rect x="14" y="14" width="7" height="7" rx="1"/>
                  </svg>
                  <span class="settings-option-label">Grid</span>
                </span>
              </label>
              <label class="settings-option">
                <input type="radio" name="layout" value="circular">
                <span class="settings-option-content">
                  <svg class="settings-option-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="12" cy="12" r="9"/>
                    <circle cx="12" cy="12" r="5"/>
                    <circle cx="12" cy="12" r="1"/>
                  </svg>
                  <span class="settings-option-label">Circular</span>
                </span>
              </label>
            </div>
          </div>
        </div>
      </div>
      <div class="modal-footer">
        <button class="btn btn-secondary" data-action="close">Cancel</button>
        <button class="btn btn-primary" data-action="save">Save</button>
      </div>
    </div>
  `;

  modalEl.addEventListener("click", handleClick);
  document.body.appendChild(modalEl);

  // Close on Escape
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && modalEl && !modalEl.hidden) {
      closeModal();
    }
  });
}

/** Populate form with current settings */
function populateForm(): void {
  if (!modalEl) return;

  // Theme
  const themeRadio = modalEl.querySelector(
    `input[name="theme"][value="${currentSettings.theme}"]`
  ) as HTMLInputElement | null;
  if (themeRadio) themeRadio.checked = true;

  // Layout
  const layoutRadio = modalEl.querySelector(
    `input[name="layout"][value="${currentSettings.defaultGraphLayout}"]`
  ) as HTMLInputElement | null;
  if (layoutRadio) layoutRadio.checked = true;
}

/** Handle click events */
function handleClick(e: Event): void {
  const target = e.target as HTMLElement;

  // Close on backdrop click
  if (target.classList.contains("modal-overlay")) {
    closeModal();
    return;
  }

  const actionEl = target.closest("[data-action]") as HTMLElement;
  if (!actionEl) return;

  const action = actionEl.getAttribute("data-action");

  switch (action) {
    case "close":
      closeModal();
      break;

    case "save":
      saveSettings();
      break;
  }
}

/** Save settings */
async function saveSettings(): Promise<void> {
  if (!modalEl) return;

  // Get form values
  const themeRadio = modalEl.querySelector('input[name="theme"]:checked') as HTMLInputElement | null;
  const layoutRadio = modalEl.querySelector('input[name="layout"]:checked') as HTMLInputElement | null;

  const newSettings: Settings = {
    theme: (themeRadio?.value as Theme) || "dark",
    defaultGraphLayout: (layoutRadio?.value as GraphLayout) || "force",
  };

  try {
    currentSettings = await api.put<Settings>("/api/settings", newSettings);

    // Apply theme immediately
    applyTheme(currentSettings.theme);

    showSuccess("Settings saved");
    closeModal();
  } catch (err) {
    showError(`Failed to save settings: ${err instanceof Error ? err.message : String(err)}`);
  }
}
