/**
 * Settings Component
 *
 * Modal for configuring application settings, organized into tabs:
 * - Appearance: Theme (dark/light)
 * - Graph: Default layout, hierarchical direction
 * - Layout: Layout tuning (iterations, temperature), display options
 */

import { api } from "../api/client";
import type { Settings, Theme, GraphLayout, LayoutSettings, LayoutDirection } from "../api/types";
import { applyTheme } from "../utils/theme";
import { showSuccess, showError } from "../utils/notifications";
import { createModal, type ModalHandle } from "../utils/modal";

/** Tab identifiers */
type SettingsTab = "appearance" | "graph" | "layout";

/** Modal handle */
let modal: ModalHandle | null = null;

/** Modal element (alias for modal.overlay for internal use) */
let modalEl: HTMLElement | null = null;

/** Active tab */
let activeTab: SettingsTab = "appearance";

/** Default layout settings (visgraph) */
const DEFAULT_LAYOUT: LayoutSettings = {
  iterations: 300,
  temperature: 0.1,
  direction: "left_to_right",
};

/** Default auto-collapse threshold */
const DEFAULT_AUTO_COLLAPSE_THRESHOLD = 20;

/** Current settings (cached) */
let currentSettings: Settings = {
  theme: "dark",
  defaultGraphLayout: "force",
  layout: DEFAULT_LAYOUT,
  fixedNodeSizes: true,
  autoCollapseThreshold: DEFAULT_AUTO_COLLAPSE_THRESHOLD,
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
  } catch {
    // Use defaults if settings can't be loaded
    applyTheme("dark");
  }
}

/** Get the default graph layout from settings */
export function getDefaultLayout(): GraphLayout {
  return currentSettings.defaultGraphLayout;
}

/** Get current layout settings */
export function getServerLayoutSettings(): LayoutSettings {
  return currentSettings.layout ?? DEFAULT_LAYOUT;
}

/** Get auto-collapse threshold from settings */
export function getAutoCollapseThreshold(): number {
  return currentSettings.autoCollapseThreshold ?? DEFAULT_AUTO_COLLAPSE_THRESHOLD;
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
  if (!modal) {
    createSettingsModal();
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
  modal!.open();
}

/** Close the modal */
function closeModal(): void {
  modal?.close();
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
            <span class="settings-option-label">Force-Directed</span>
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

    <!-- Hierarchical Direction -->
    <div class="form-group">
      <label class="form-label">Hierarchical Direction</label>
      <p class="form-help">Flow direction for hierarchical layout</p>
      <select name="direction" id="direction-select" class="form-select">
        <option value="left_to_right">Left to Right</option>
        <option value="right_to_left">Right to Left</option>
        <option value="top_to_bottom">Top to Bottom</option>
        <option value="bottom_to_top">Bottom to Top</option>
      </select>
    </div>
  `;
}

/** Render the Layout tab content */
function renderLayoutTab(): string {
  return `
    <!-- Layout Tuning -->
    <div class="form-group" id="layout-settings">
      <label class="form-label">Algorithm Tuning</label>
      <p class="form-help">Fine-tune the visgraph layout algorithms</p>

      <div class="settings-slider-group">
        <div class="settings-slider">
          <div class="settings-slider-header">
            <span class="settings-slider-label">Iterations</span>
            <span class="settings-slider-value" id="iterations-value">300</span>
          </div>
          <input type="range" name="iterations" id="iterations-slider"
                 min="100" max="1000" step="50" value="300">
          <p class="settings-slider-help">Number of force simulation steps (more = better convergence, slower)</p>
        </div>

        <div class="settings-slider">
          <div class="settings-slider-header">
            <span class="settings-slider-label">Temperature</span>
            <span class="settings-slider-value" id="temperature-value">0.10</span>
          </div>
          <input type="range" name="temperature" id="temperature-slider"
                 min="0.01" max="1.0" step="0.01" value="0.1">
          <p class="settings-slider-help">Initial movement speed — lower values produce tighter layouts</p>
        </div>
      </div>
    </div>

    <!-- Display Settings -->
    <div class="form-group">
      <label class="form-label">Display</label>
      <p class="form-help">Visual appearance of the graph</p>
      <div class="settings-slider-group">
        <label class="settings-checkbox">
          <input type="checkbox" name="fixedNodeSizes" id="fixed-node-sizes-checkbox" checked>
          <span class="settings-checkbox-label">Fixed node and relationship sizes</span>
        </label>
        <p class="settings-slider-help">When enabled, nodes and relationships stay the same visual size regardless of zoom level</p>

        <div class="settings-slider">
          <div class="settings-slider-header">
            <span class="settings-slider-label">Auto-collapse threshold</span>
            <span class="settings-slider-value" id="auto-collapse-value">20</span>
          </div>
          <input type="range" name="autoCollapseThreshold" id="auto-collapse-slider"
                 min="0" max="100" step="1" value="20">
          <p class="settings-slider-help">Nodes with more than this many incoming connections are collapsed when a graph loads. Set to 0 to disable.</p>
        </div>
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
      <button class="db-type-tab ${activeTab === "layout" ? "active" : ""}" data-tab="layout">
        Layout
      </button>
    </div>
    <div class="settings-form">
      <div ${activeTab !== "appearance" ? "hidden" : ""} id="tab-appearance">
        ${renderAppearanceTab()}
      </div>
      <div ${activeTab !== "graph" ? "hidden" : ""} id="tab-graph">
        ${renderGraphTab()}
      </div>
      <div ${activeTab !== "layout" ? "hidden" : ""} id="tab-layout">
        ${renderLayoutTab()}
      </div>
    </div>
  `;
}

/** Attach slider input listeners (must be called after rendering body) */
function attachSliderListeners(): void {
  if (!modalEl) return;

  const iterationsSlider = modalEl.querySelector("#iterations-slider") as HTMLInputElement | null;
  const iterationsValue = modalEl.querySelector("#iterations-value") as HTMLElement | null;
  if (iterationsSlider && iterationsValue) {
    iterationsSlider.addEventListener("input", () => {
      iterationsValue.textContent = iterationsSlider.value;
    });
  }

  const temperatureSlider = modalEl.querySelector("#temperature-slider") as HTMLInputElement | null;
  const temperatureValue = modalEl.querySelector("#temperature-value") as HTMLElement | null;
  if (temperatureSlider && temperatureValue) {
    temperatureSlider.addEventListener("input", () => {
      temperatureValue.textContent = parseFloat(temperatureSlider.value).toFixed(2);
    });
  }

  const autoCollapseSlider = modalEl.querySelector("#auto-collapse-slider") as HTMLInputElement | null;
  const autoCollapseValue = modalEl.querySelector("#auto-collapse-value") as HTMLElement | null;
  if (autoCollapseSlider && autoCollapseValue) {
    autoCollapseSlider.addEventListener("input", () => {
      autoCollapseValue.textContent = autoCollapseSlider.value;
    });
  }
}

/** Create the modal element using the shared modal utility */
function createSettingsModal(): void {
  modal = createModal({
    id: "settings-modal",
    title: "Settings",
    buttons: [
      { label: "Cancel", action: "close", className: "btn btn-secondary" },
      { label: "Save", action: "save", className: "btn btn-primary" },
    ],
    onClick(action) {
      if (action === "save") {
        saveSettings();
      } else if (action === "close") {
        closeModal();
      }
    },
  });

  modal.body.id = "settings-body";
  modalEl = modal.overlay;

  // Separate listener for tab switching (tabs use data-tab, not data-action)
  modal.overlay.addEventListener("click", handleTabClick);
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

  // Layout settings
  const layout = currentSettings.layout ?? DEFAULT_LAYOUT;

  const iterationsSlider = modalEl.querySelector("#iterations-slider") as HTMLInputElement | null;
  const iterationsValue = modalEl.querySelector("#iterations-value") as HTMLElement | null;
  if (iterationsSlider && iterationsValue) {
    iterationsSlider.value = String(layout.iterations);
    iterationsValue.textContent = String(layout.iterations);
  }

  const temperatureSlider = modalEl.querySelector("#temperature-slider") as HTMLInputElement | null;
  const temperatureValue = modalEl.querySelector("#temperature-value") as HTMLElement | null;
  if (temperatureSlider && temperatureValue) {
    temperatureSlider.value = String(layout.temperature);
    temperatureValue.textContent = layout.temperature.toFixed(2);
  }

  const directionSelect = modalEl.querySelector("#direction-select") as HTMLSelectElement | null;
  if (directionSelect) {
    directionSelect.value = layout.direction;
  }

  // Fixed node sizes
  const fixedNodeSizesCheckbox = modalEl.querySelector("#fixed-node-sizes-checkbox") as HTMLInputElement | null;
  if (fixedNodeSizesCheckbox) {
    fixedNodeSizesCheckbox.checked = currentSettings.fixedNodeSizes ?? true;
  }

  // Auto-collapse threshold
  const autoCollapseSlider = modalEl.querySelector("#auto-collapse-slider") as HTMLInputElement | null;
  const autoCollapseValue = modalEl.querySelector("#auto-collapse-value") as HTMLElement | null;
  const threshold = currentSettings.autoCollapseThreshold ?? DEFAULT_AUTO_COLLAPSE_THRESHOLD;
  if (autoCollapseSlider && autoCollapseValue) {
    autoCollapseSlider.value = String(threshold);
    autoCollapseValue.textContent = String(threshold);
  }
}

/** Handle tab switching clicks */
function handleTabClick(e: Event): void {
  const target = e.target as HTMLElement;
  const tabBtn = target.closest("[data-tab]") as HTMLElement;
  if (!tabBtn) return;

  const tabId = tabBtn.getAttribute("data-tab") as SettingsTab;
  if (tabId && tabId !== activeTab) {
    activeTab = tabId;
    // Update tab button active states
    modalEl?.querySelectorAll("[data-tab]").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-tab") === activeTab);
    });
    // Toggle tab content visibility
    const tabs: SettingsTab[] = ["appearance", "graph", "layout"];
    for (const id of tabs) {
      const el = modalEl?.querySelector(`#tab-${id}`) as HTMLElement | null;
      if (el) el.hidden = id !== activeTab;
    }
  }
}

/** Save settings */
async function saveSettings(): Promise<void> {
  if (!modalEl) return;

  // Get form values
  const themeRadio = modalEl.querySelector('input[name="theme"]:checked') as HTMLInputElement | null;
  const layoutRadio = modalEl.querySelector('input[name="layout"]:checked') as HTMLInputElement | null;
  const iterationsSlider = modalEl.querySelector("#iterations-slider") as HTMLInputElement | null;
  const temperatureSlider = modalEl.querySelector("#temperature-slider") as HTMLInputElement | null;
  const directionSelect = modalEl.querySelector("#direction-select") as HTMLSelectElement | null;
  const fixedNodeSizesCheckbox = modalEl.querySelector("#fixed-node-sizes-checkbox") as HTMLInputElement | null;
  const autoCollapseSlider = modalEl.querySelector("#auto-collapse-slider") as HTMLInputElement | null;

  const layout: LayoutSettings = {
    iterations: iterationsSlider ? parseInt(iterationsSlider.value, 10) : DEFAULT_LAYOUT.iterations,
    temperature: temperatureSlider ? parseFloat(temperatureSlider.value) : DEFAULT_LAYOUT.temperature,
    direction: (directionSelect?.value as LayoutDirection) || DEFAULT_LAYOUT.direction,
  };

  const fixedNodeSizes = fixedNodeSizesCheckbox ? fixedNodeSizesCheckbox.checked : true;
  const autoCollapseThreshold = autoCollapseSlider
    ? parseInt(autoCollapseSlider.value, 10)
    : DEFAULT_AUTO_COLLAPSE_THRESHOLD;

  const newSettings: Settings = {
    theme: (themeRadio?.value as Theme) || currentSettings.theme,
    defaultGraphLayout: (layoutRadio?.value as GraphLayout) || currentSettings.defaultGraphLayout,
    layout,
    fixedNodeSizes,
    autoCollapseThreshold,
  };

  try {
    currentSettings = await api.put<Settings>("/api/settings", newSettings);

    // Apply theme immediately
    applyTheme(currentSettings.theme);

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
