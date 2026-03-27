/**
 * Settings Component
 *
 * Modal for configuring application settings, organized into tabs:
 * - Appearance: Theme (dark/light)
 * - Graph: Default layout, force layout parameters, display options
 */

import { api } from "../api/client";
import type { Settings, Theme, GraphLayout, ForceLayoutSettings } from "../api/types";
import { applyTheme } from "../utils/theme";
import { showSuccess, showError } from "../utils/notifications";
import { setUserForceSettings } from "../graph";

/** Tab identifiers */
type SettingsTab = "appearance" | "graph";

/** Modal element */
let modalEl: HTMLElement | null = null;

/** Active tab */
let activeTab: SettingsTab = "appearance";

/** Default force layout settings */
const DEFAULT_FORCE_LAYOUT: ForceLayoutSettings = {
  gravity: 0.5,
  scalingRatio: 10,
  adjustSizes: true,
};

/** Current settings (cached) */
let currentSettings: Settings = {
  theme: "dark",
  defaultGraphLayout: "force",
  forceLayout: DEFAULT_FORCE_LAYOUT,
  fixedNodeSizes: true,
};

/** Callback for when fixedNodeSizes changes */
let onFixedNodeSizesChange: ((fixed: boolean) => void) | null = null;

/** Register a callback for fixedNodeSizes changes */
export function setFixedNodeSizesCallback(callback: (fixed: boolean) => void): void {
  onFixedNodeSizesChange = callback;
}

/** Get current fixedNodeSizes setting */
export function getFixedNodeSizes(): boolean {
  return currentSettings.fixedNodeSizes ?? true;
}

/**
 * Apply initial settings on app startup.
 * Loads settings from API and applies the theme immediately.
 */
export async function applyInitialSettings(): Promise<void> {
  try {
    currentSettings = await api.get<Settings>("/api/settings");
    applyTheme(currentSettings.theme);
    // Apply force layout settings
    if (currentSettings.forceLayout) {
      setUserForceSettings(currentSettings.forceLayout);
    }
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

  renderBody();
  attachSliderListeners();
  populateForm();
  modalEl!.hidden = false;
}

/** Close the modal */
function closeModal(): void {
  if (modalEl) {
    modalEl.hidden = true;
  }
}

/** Render the Appearance tab content */
function renderAppearanceTab(): string {
  return `
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
  `;
}

/** Render the Graph tab content */
function renderGraphTab(): string {
  return `
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
        <label class="settings-option">
          <input type="radio" name="layout" value="lattice">
          <span class="settings-option-content">
            <svg class="settings-option-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="6" cy="8" r="2"/>
              <circle cx="14" cy="5" r="2"/>
              <circle cx="10" cy="14" r="2"/>
              <circle cx="18" cy="11" r="2"/>
              <circle cx="6" cy="20" r="2"/>
              <circle cx="14" cy="17" r="2"/>
            </svg>
            <span class="settings-option-label">Lattice</span>
          </span>
        </label>
      </div>
    </div>

    <!-- Force Layout Settings -->
    <div class="form-group" id="force-layout-settings">
      <label class="form-label">Force Layout Settings</label>
      <p class="form-help">Fine-tune how force-directed layout spreads nodes</p>

      <div class="settings-slider-group">
        <div class="settings-slider">
          <div class="settings-slider-header">
            <span class="settings-slider-label">Gravity</span>
            <span class="settings-slider-value" id="gravity-value">0.5</span>
          </div>
          <input type="range" name="gravity" id="gravity-slider"
                 min="0.1" max="2" step="0.1" value="0.5">
          <p class="settings-slider-help">How strongly nodes pull toward center (lower = more spread)</p>
        </div>

        <div class="settings-slider">
          <div class="settings-slider-header">
            <span class="settings-slider-label">Spread</span>
            <span class="settings-slider-value" id="spread-value">10</span>
          </div>
          <input type="range" name="scalingRatio" id="spread-slider"
                 min="1" max="50" step="1" value="10">
          <p class="settings-slider-help">How far apart nodes spread (higher = more spacing)</p>
        </div>

        <label class="settings-checkbox">
          <input type="checkbox" name="adjustSizes" id="adjust-sizes-checkbox" checked>
          <span class="settings-checkbox-label">Prevent node overlap</span>
        </label>
      </div>
    </div>

    <!-- Graph Display Settings -->
    <div class="form-group">
      <label class="form-label">Display</label>
      <p class="form-help">Visual appearance of the graph</p>
      <div class="settings-slider-group">
        <label class="settings-checkbox">
          <input type="checkbox" name="fixedNodeSizes" id="fixed-node-sizes-checkbox" checked>
          <span class="settings-checkbox-label">Fixed node and relationship sizes</span>
        </label>
        <p class="settings-slider-help">When enabled, nodes and relationships stay the same visual size regardless of zoom level</p>
      </div>
    </div>
  `;
}

/** Render the modal body with tabs */
function renderBody(): void {
  const body = modalEl?.querySelector("#settings-body");
  if (!body) return;

  body.innerHTML = `
    <div class="db-type-tabs">
      <button class="db-type-tab ${activeTab === "appearance" ? "active" : ""}" data-tab="appearance">
        Appearance
      </button>
      <button class="db-type-tab ${activeTab === "graph" ? "active" : ""}" data-tab="graph">
        Graph
      </button>
    </div>
    <div class="settings-form">
      <div ${activeTab !== "appearance" ? "hidden" : ""} id="tab-appearance">
        ${renderAppearanceTab()}
      </div>
      <div ${activeTab !== "graph" ? "hidden" : ""} id="tab-graph">
        ${renderGraphTab()}
      </div>
    </div>
  `;
}

/** Attach slider input listeners (must be called after rendering body) */
function attachSliderListeners(): void {
  if (!modalEl) return;

  const gravitySlider = modalEl.querySelector("#gravity-slider") as HTMLInputElement | null;
  const gravityValue = modalEl.querySelector("#gravity-value") as HTMLElement | null;
  if (gravitySlider && gravityValue) {
    gravitySlider.addEventListener("input", () => {
      gravityValue.textContent = gravitySlider.value;
    });
  }

  const spreadSlider = modalEl.querySelector("#spread-slider") as HTMLInputElement | null;
  const spreadValue = modalEl.querySelector("#spread-value") as HTMLElement | null;
  if (spreadSlider && spreadValue) {
    spreadSlider.addEventListener("input", () => {
      spreadValue.textContent = spreadSlider.value;
    });
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
      <div class="modal-body" id="settings-body"></div>
      <div class="modal-footer">
        <button class="btn btn-secondary" data-action="close">Cancel</button>
        <button class="btn btn-primary" data-action="save">Save</button>
      </div>
    </div>
  `;

  modalEl.addEventListener("click", handleClick);
  document.body.appendChild(modalEl);
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

  // Force layout settings
  const forceLayout = currentSettings.forceLayout ?? DEFAULT_FORCE_LAYOUT;

  const gravitySlider = modalEl.querySelector("#gravity-slider") as HTMLInputElement | null;
  const gravityValue = modalEl.querySelector("#gravity-value") as HTMLElement | null;
  if (gravitySlider && gravityValue) {
    gravitySlider.value = String(forceLayout.gravity);
    gravityValue.textContent = String(forceLayout.gravity);
  }

  const spreadSlider = modalEl.querySelector("#spread-slider") as HTMLInputElement | null;
  const spreadValue = modalEl.querySelector("#spread-value") as HTMLElement | null;
  if (spreadSlider && spreadValue) {
    spreadSlider.value = String(forceLayout.scalingRatio);
    spreadValue.textContent = String(forceLayout.scalingRatio);
  }

  const adjustSizesCheckbox = modalEl.querySelector("#adjust-sizes-checkbox") as HTMLInputElement | null;
  if (adjustSizesCheckbox) {
    adjustSizesCheckbox.checked = forceLayout.adjustSizes;
  }

  // Fixed node sizes
  const fixedNodeSizesCheckbox = modalEl.querySelector("#fixed-node-sizes-checkbox") as HTMLInputElement | null;
  if (fixedNodeSizesCheckbox) {
    fixedNodeSizesCheckbox.checked = currentSettings.fixedNodeSizes ?? true;
  }
}

/** Handle click events */
function handleClick(e: Event): void {
  const target = e.target as HTMLElement;

  // Close on backdrop click
  if (target.classList.contains("modal-overlay")) {
    closeModal();
    return;
  }

  // Tab switching (toggle visibility without re-rendering to preserve form state)
  const tabBtn = target.closest("[data-tab]") as HTMLElement;
  if (tabBtn) {
    const tabId = tabBtn.getAttribute("data-tab") as SettingsTab;
    if (tabId && tabId !== activeTab) {
      activeTab = tabId;
      // Update tab button active states
      modalEl?.querySelectorAll("[data-tab]").forEach((btn) => {
        btn.classList.toggle("active", btn.getAttribute("data-tab") === activeTab);
      });
      // Toggle tab content visibility
      const tabs: SettingsTab[] = ["appearance", "graph"];
      for (const id of tabs) {
        const el = modalEl?.querySelector(`#tab-${id}`) as HTMLElement | null;
        if (el) el.hidden = id !== activeTab;
      }
    }
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
  const gravitySlider = modalEl.querySelector("#gravity-slider") as HTMLInputElement | null;
  const spreadSlider = modalEl.querySelector("#spread-slider") as HTMLInputElement | null;
  const adjustSizesCheckbox = modalEl.querySelector("#adjust-sizes-checkbox") as HTMLInputElement | null;
  const fixedNodeSizesCheckbox = modalEl.querySelector("#fixed-node-sizes-checkbox") as HTMLInputElement | null;

  const forceLayout: ForceLayoutSettings = {
    gravity: gravitySlider ? parseFloat(gravitySlider.value) : DEFAULT_FORCE_LAYOUT.gravity,
    scalingRatio: spreadSlider ? parseFloat(spreadSlider.value) : DEFAULT_FORCE_LAYOUT.scalingRatio,
    adjustSizes: adjustSizesCheckbox ? adjustSizesCheckbox.checked : DEFAULT_FORCE_LAYOUT.adjustSizes,
  };

  const fixedNodeSizes = fixedNodeSizesCheckbox ? fixedNodeSizesCheckbox.checked : true;

  const newSettings: Settings = {
    theme: (themeRadio?.value as Theme) || currentSettings.theme,
    defaultGraphLayout: (layoutRadio?.value as GraphLayout) || currentSettings.defaultGraphLayout,
    forceLayout,
    fixedNodeSizes,
  };

  try {
    currentSettings = await api.put<Settings>("/api/settings", newSettings);

    // Apply theme immediately
    applyTheme(currentSettings.theme);

    // Apply force layout settings immediately
    setUserForceSettings(currentSettings.forceLayout ?? null);

    // Apply fixed node sizes setting
    if (onFixedNodeSizesChange) {
      onFixedNodeSizesChange(currentSettings.fixedNodeSizes ?? true);
    }

    showSuccess("Settings saved");
    closeModal();
  } catch (err) {
    showError(`Failed to save settings: ${err instanceof Error ? err.message : String(err)}`);
  }
}
