/**
 * Database Connection Modal
 *
 * Provides a UI for connecting to different database backends:
 * - KuzuDB (file-based, Cypher)
 * - CozoDB (file-based, Datalog)
 * - Neo4j (network, Cypher)
 * - FalkorDB (network, Cypher)
 */

import { appState } from "../main";
import { showNoConnectionPlaceholder, updateGraphForConnectionState } from "./graph-view";
import { saveConnection, getDisplayName } from "./connection-history";
import { openDbManager } from "./db-manager";
import { redactUrlCredentials } from "../utils/html";
import { api, isRunningInTauri } from "../api/client";
import type { DatabaseStatusResponse, SupportedDatabaseInfo, DatabaseType } from "../api/types";

/** Cached list of supported database types */
let supportedDatabases: SupportedDatabaseInfo[] = [];

/** Current selected database type */
let selectedDbType: DatabaseType = "crustdb";

/** Update the connection status indicator */
export function updateConnectionStatus(): void {
  const statusEl = document.getElementById("connection-status");
  if (!statusEl) return;

  if (appState.databaseConnected) {
    statusEl.classList.add("connected");
    const textEl = statusEl.querySelector(".status-text");
    if (textEl) {
      textEl.textContent = appState.databaseType || "Connected";
    }
  } else {
    statusEl.classList.remove("connected");
    const textEl = statusEl.querySelector(".status-text");
    if (textEl) {
      textEl.textContent = "Not Connected";
    }
  }

  // Update menu items that require database connection
  updateMenuItemsForConnection(appState.databaseConnected);
}

/** Enable/disable menu items based on database connection state */
function updateMenuItemsForConnection(connected: boolean): void {
  const items = document.querySelectorAll("[data-requires-db]");
  for (const item of items) {
    if (connected) {
      item.removeAttribute("disabled");
      item.classList.remove("menu-disabled");
    } else {
      item.setAttribute("disabled", "");
      item.classList.add("menu-disabled");
    }
  }
}

/** Fetch and update connection status from server */
export async function refreshConnectionStatus(): Promise<void> {
  try {
    const status = await api.get<DatabaseStatusResponse>("/api/database/status");
    appState.databaseConnected = status.connected;
    appState.databaseType = status.database_type || null;
    updateConnectionStatus();
    // Update graph view based on connection state
    updateGraphForConnectionState(status.connected);
  } catch (error) {
    console.error("Failed to fetch database status:", error);
    // Could not reach server - show placeholder
    updateGraphForConnectionState(false, "Could not connect to server");
  }
}

/** Build connection URL from form data */
function buildConnectionUrl(): string {
  const form = document.getElementById("db-connect-form") as HTMLFormElement | null;
  if (!form) return "";

  switch (selectedDbType) {
    case "kuzu": {
      const path = (form.querySelector("#db-path") as HTMLInputElement)?.value || "";
      return `kuzu://${path}`;
    }
    case "cozo": {
      const path = (form.querySelector("#db-path") as HTMLInputElement)?.value || "";
      return `cozodb://${path}`;
    }
    case "crustdb": {
      const path = (form.querySelector("#db-path") as HTMLInputElement)?.value || "";
      return `crustdb://${path}`;
    }
    case "neo4j": {
      const host = (form.querySelector("#db-host") as HTMLInputElement)?.value || "localhost";
      const port = (form.querySelector("#db-port") as HTMLInputElement)?.value || "7687";
      const user = (form.querySelector("#db-user") as HTMLInputElement)?.value || "";
      const pass = (form.querySelector("#db-pass") as HTMLInputElement)?.value || "";
      const database = (form.querySelector("#db-name") as HTMLInputElement)?.value || "";
      const ssl = (form.querySelector("#db-ssl") as HTMLInputElement)?.checked || false;

      // Use neo4j+s:// for SSL, neo4j:// for plain
      let url = ssl ? "neo4j+s://" : "neo4j://";
      if (user) {
        url += user;
        if (pass) url += `:${pass}`;
        url += "@";
      }
      url += `${host}:${port}`;
      if (database) url += `/${database}`;
      return url;
    }
    case "falkordb": {
      const host = (form.querySelector("#db-host") as HTMLInputElement)?.value || "localhost";
      const port = (form.querySelector("#db-port") as HTMLInputElement)?.value || "6379";
      const user = (form.querySelector("#db-user") as HTMLInputElement)?.value || "";
      const pass = (form.querySelector("#db-pass") as HTMLInputElement)?.value || "";

      let url = "falkordb://";
      if (user) {
        url += user;
        if (pass) url += `:${pass}`;
        url += "@";
      }
      url += `${host}:${port}`;
      return url;
    }
    default:
      return "";
  }
}

/** Update form fields based on selected database type */
function updateFormFields(): void {
  const fileFields = document.getElementById("db-file-fields");
  const networkFields = document.getElementById("db-network-fields");
  const dbNameField = document.getElementById("db-name-group");
  const sslField = document.getElementById("db-ssl-group");
  const cozoWarning = document.getElementById("db-cozo-warning");

  if (!fileFields || !networkFields) return;

  const isFile = selectedDbType === "kuzu" || selectedDbType === "cozo" || selectedDbType === "crustdb";

  fileFields.hidden = !isFile;
  networkFields.hidden = isFile;

  // Neo4j has database name field and SSL option, FalkorDB doesn't
  if (dbNameField) {
    dbNameField.hidden = selectedDbType !== "neo4j";
  }
  if (sslField) {
    sslField.hidden = selectedDbType !== "neo4j";
  }

  // Show CozoDB warning about Cypher
  if (cozoWarning) {
    cozoWarning.hidden = selectedDbType !== "cozo";
  }

  // Update default port
  const portInput = document.getElementById("db-port") as HTMLInputElement | null;
  if (portInput) {
    portInput.value = selectedDbType === "neo4j" ? "7687" : "6379";
  }
}

/** Connect to database */
async function connectToDatabase(): Promise<void> {
  const url = buildConnectionUrl();
  if (!url) {
    showError("Please fill in the required fields");
    return;
  }

  const connectBtn = document.getElementById("db-connect-btn") as HTMLButtonElement | null;
  const errorEl = document.getElementById("db-connect-error");

  if (connectBtn) {
    connectBtn.disabled = true;
    connectBtn.innerHTML = '<span class="spinner-sm"></span> Connecting...';
  }

  if (errorEl) {
    errorEl.hidden = true;
  }

  try {
    const result = await api.post<DatabaseStatusResponse>("/api/database/connect", { url });
    appState.databaseConnected = result.connected;
    appState.databaseType = result.database_type;
    updateConnectionStatus();

    // Save to connection history
    const dbType = result.database_type || selectedDbType;
    await saveConnection({
      url,
      displayName: getDisplayName(url, dbType),
      databaseType: dbType,
    });

    closeDbConnect();
  } catch (error) {
    showError(`Connection error: ${error}`);
  } finally {
    if (connectBtn) {
      connectBtn.disabled = false;
      connectBtn.textContent = "Connect";
    }
  }
}

/** Connect to a database using a URL directly (for recent connections) */
export async function connectToUrl(url: string): Promise<boolean> {
  try {
    const result = await api.post<DatabaseStatusResponse>("/api/database/connect", { url });
    appState.databaseConnected = result.connected;
    appState.databaseType = result.database_type;
    updateConnectionStatus();

    // Save to connection history (moves it to top)
    const dbType = result.database_type || "unknown";
    await saveConnection({
      url,
      displayName: getDisplayName(url, dbType),
      databaseType: dbType,
    });

    return true;
  } catch (error) {
    // Use redacted URL in logs to avoid exposing credentials
    console.error("Connection error:", redactUrlCredentials(url), error);
    return false;
  }
}

/** Disconnect from database */
export async function disconnectDb(): Promise<void> {
  try {
    await api.postNoContent("/api/database/disconnect");
    appState.databaseConnected = false;
    appState.databaseType = null;
    updateConnectionStatus();

    // Show the no-connection placeholder (only in production, dev shows demo data)
    if (!import.meta.env.DEV) {
      showNoConnectionPlaceholder();
    }
  } catch (error) {
    console.error("Failed to disconnect:", error);
  }
}

/** Show error message in modal */
function showError(message: string): void {
  const errorEl = document.getElementById("db-connect-error");
  if (errorEl) {
    errorEl.textContent = message;
    errorEl.hidden = false;
  }
}

/** Browse response from API */
interface BrowseEntry {
  name: string;
  path: string;
  is_dir: boolean;
}

interface BrowseResponse {
  current: string;
  parent: string | null;
  entries: BrowseEntry[];
}

/** Browse for database path using Tauri dialog or web file browser */
async function browseForPath(): Promise<void> {
  // In Tauri mode, use native file dialog via plugin
  if (isRunningInTauri() && window.__TAURI_PLUGIN_DIALOG__) {
    try {
      // KuzuDB uses a directory, CozoDB/CrustDB use a file
      const isDirectory = selectedDbType === "kuzu";

      const options: Parameters<typeof window.__TAURI_PLUGIN_DIALOG__.open>[0] = {
        directory: isDirectory,
        multiple: false,
        title: isDirectory ? "Select Database Directory" : "Select Database File",
      };
      if (!isDirectory) {
        options.filters = [{ name: "Database Files", extensions: ["db", "sqlite", "sqlite3"] }];
      }
      const selected = await window.__TAURI_PLUGIN_DIALOG__.open(options);

      if (selected && typeof selected === "string") {
        const pathInput = document.getElementById("db-path") as HTMLInputElement | null;
        if (pathInput) {
          pathInput.value = selected;
        }
      }
      return;
    } catch (error) {
      console.error("Failed to open Tauri file dialog:", error);
      // Fall through to web file browser
    }
  }

  // Use web-based file browser (HTTP mode)
  openFileBrowser();
}

/** Open the web-based file browser modal */
async function openFileBrowser(startPath?: string): Promise<void> {
  // Create browser modal if it doesn't exist
  let browserModal = document.getElementById("file-browser-modal");
  if (!browserModal) {
    browserModal = createFileBrowserModal();
    document.body.appendChild(browserModal);
  }

  // Load initial directory
  await loadDirectory(startPath);

  // Show modal
  browserModal.hidden = false;
}

/** Create the file browser modal */
function createFileBrowserModal(): HTMLElement {
  const modal = document.createElement("div");
  modal.id = "file-browser-modal";
  modal.className = "modal-overlay";
  modal.hidden = true;

  const isDirectory = selectedDbType === "kuzu";
  const title = isDirectory ? "Select Database Directory" : "Select Database File";

  modal.innerHTML = `
    <div class="modal-content modal-lg">
      <div class="modal-header">
        <h2 class="modal-title">${title}</h2>
        <button class="modal-close" id="file-browser-close">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body">
        <div class="file-browser-path mb-3">
          <label class="form-label">Current Path</label>
          <div class="flex gap-2">
            <input type="text" id="file-browser-path-input" class="form-input flex-1" readonly />
            <button type="button" id="file-browser-up" class="btn btn-secondary" title="Go to parent directory">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="w-4 h-4">
                <path d="M9 19l-7-7 7-7M2 12h20"/>
              </svg>
            </button>
          </div>
        </div>
        <div id="file-browser-list" class="file-browser-list">
          <div class="text-center py-4 text-gray-400">Loading...</div>
        </div>
      </div>
      <div class="modal-footer">
        <button type="button" id="file-browser-cancel" class="btn btn-secondary">Cancel</button>
        <button type="button" id="file-browser-select" class="btn btn-primary">Select This Directory</button>
      </div>
    </div>
  `;

  // Event listeners
  modal.querySelector("#file-browser-close")?.addEventListener("click", closeFileBrowser);
  modal.querySelector("#file-browser-cancel")?.addEventListener("click", closeFileBrowser);
  modal.querySelector("#file-browser-up")?.addEventListener("click", goToParentDirectory);
  modal.querySelector("#file-browser-select")?.addEventListener("click", selectCurrentPath);

  // Close on backdrop click
  modal.addEventListener("click", (e) => {
    if (e.target === modal) closeFileBrowser();
  });

  return modal;
}

/** Current browser state */
let currentBrowsePath = "";
let currentParentPath: string | null = null;

/** Load a directory's contents */
async function loadDirectory(path?: string): Promise<void> {
  const listEl = document.getElementById("file-browser-list");
  const pathInput = document.getElementById("file-browser-path-input") as HTMLInputElement | null;

  if (!listEl) return;

  listEl.innerHTML = '<div class="text-center py-4 text-gray-400">Loading...</div>';

  try {
    const url = path ? `/api/browse?path=${encodeURIComponent(path)}` : "/api/browse";
    const response = await fetch(url);

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.message || "Failed to browse directory");
    }

    const data: BrowseResponse = await response.json();
    currentBrowsePath = data.current;
    currentParentPath = data.parent;

    if (pathInput) {
      pathInput.value = data.current;
    }

    // Update up button state
    const upBtn = document.getElementById("file-browser-up") as HTMLButtonElement | null;
    if (upBtn) {
      upBtn.disabled = !data.parent;
    }

    // Render entries
    if (data.entries.length === 0) {
      listEl.innerHTML = '<div class="text-center py-4 text-gray-400">Empty directory</div>';
      return;
    }

    const isDirectory = selectedDbType === "kuzu";
    listEl.innerHTML = data.entries
      .map((entry) => {
        const icon = entry.is_dir
          ? '<svg viewBox="0 0 24 24" fill="currentColor" class="w-5 h-5 text-yellow-500"><path d="M3 4a2 2 0 012-2h4.586a2 2 0 011.414.586l1.414 1.414H19a2 2 0 012 2v12a2 2 0 01-2 2H5a2 2 0 01-2-2V4z"/></svg>'
          : '<svg viewBox="0 0 24 24" fill="currentColor" class="w-5 h-5 text-gray-400"><path d="M5 4a2 2 0 012-2h6l4 4v12a2 2 0 01-2 2H7a2 2 0 01-2-2V4z"/></svg>';

        // For directory selection mode, only directories are clickable to navigate
        // For file selection mode, directories navigate and files can be selected
        const isSelectable = isDirectory ? entry.is_dir : true;
        const clickAction = entry.is_dir ? `data-navigate="${entry.path}"` : `data-select="${entry.path}"`;

        return `
          <div class="file-browser-item ${isSelectable ? "selectable" : ""}" ${clickAction}>
            ${icon}
            <span class="file-browser-name">${entry.name}</span>
          </div>
        `;
      })
      .join("");

    // Add click handlers
    for (const item of listEl.querySelectorAll("[data-navigate]")) {
      item.addEventListener("click", () => {
        const path = item.getAttribute("data-navigate");
        if (path) loadDirectory(path);
      });
    }

    for (const item of listEl.querySelectorAll("[data-select]")) {
      item.addEventListener("click", () => {
        const path = item.getAttribute("data-select");
        if (path) {
          setSelectedPath(path);
          closeFileBrowser();
        }
      });
    }
  } catch (error) {
    console.error("Failed to load directory:", error);
    listEl.innerHTML = `<div class="text-center py-4 text-red-400">Error: ${error instanceof Error ? error.message : "Unknown error"}</div>`;
  }
}

/** Go to parent directory */
function goToParentDirectory(): void {
  if (currentParentPath) {
    loadDirectory(currentParentPath);
  }
}

/** Select current directory path */
function selectCurrentPath(): void {
  setSelectedPath(currentBrowsePath);
  closeFileBrowser();
}

/** Set the selected path in the main form */
function setSelectedPath(path: string): void {
  const pathInput = document.getElementById("db-path") as HTMLInputElement | null;
  if (pathInput) {
    pathInput.value = path;
  }
}

/** Close the file browser modal */
function closeFileBrowser(): void {
  const modal = document.getElementById("file-browser-modal");
  if (modal) {
    modal.hidden = true;
  }
}

/** Open the database connection modal */
export function openDbConnect(): void {
  // Create modal if it doesn't exist
  let modal = document.getElementById("db-connect-modal");
  if (!modal) {
    modal = createModal();
    document.body.appendChild(modal);
  }

  // Reset form
  const form = document.getElementById("db-connect-form") as HTMLFormElement | null;
  form?.reset();

  // Reset to first supported type (or crustdb as fallback)
  selectedDbType = supportedDatabases.length > 0 ? supportedDatabases[0]!.id : "crustdb";
  const tabs = modal.querySelectorAll(".db-type-tab");
  for (const tab of tabs) {
    tab.classList.toggle("active", tab.getAttribute("data-type") === selectedDbType);
  }
  updateFormFields();

  // Show modal
  modal.hidden = false;
}

/** Close the database connection modal */
export function closeDbConnect(): void {
  const modal = document.getElementById("db-connect-modal");
  if (modal) {
    modal.hidden = true;
  }
}

/** Create the modal HTML */
function createModal(): HTMLElement {
  const modal = document.createElement("div");
  modal.id = "db-connect-modal";
  modal.className = "modal-overlay";
  modal.hidden = true;

  modal.innerHTML = `
    <div class="modal-content">
      <div class="modal-header">
        <h2 class="modal-title">Connect to Database</h2>
        <button class="modal-close" id="db-connect-close-btn">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M18 6L6 18M6 6l12 12"/>
          </svg>
        </button>
      </div>
      <div class="modal-body">
        <!-- Database Type Tabs (dynamically populated) -->
        <div class="db-type-tabs" id="db-type-tabs"></div>

        <form id="db-connect-form" class="add-form mt-4">
          <!-- File-based database fields -->
          <div id="db-file-fields">
            <div class="form-group">
              <label class="form-label" for="db-path">Database Path</label>
              <div class="input-with-button">
                <input type="text" id="db-path" class="form-input"
                       placeholder="/path/to/database" required>
                <button type="button" id="db-path-browse" class="btn btn-secondary">Browse...</button>
              </div>
              <p class="form-help">Path to the database file or directory</p>
            </div>
            <!-- CozoDB warning -->
            <div id="db-cozo-warning" class="form-warning" hidden>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="warning-icon">
                <path d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
              </svg>
              <span>CozoDB uses Datalog query language. Cypher queries are not supported.</span>
            </div>
          </div>

          <!-- Network database fields -->
          <div id="db-network-fields" hidden>
            <div class="form-group">
              <label class="form-label" for="db-host">Host</label>
              <input type="text" id="db-host" class="form-input"
                     placeholder="localhost" value="localhost">
            </div>
            <div class="form-group">
              <label class="form-label" for="db-port">Port</label>
              <input type="number" id="db-port" class="form-input"
                     placeholder="7687" value="7687">
            </div>
            <div class="form-group">
              <label class="form-label" for="db-user">Username</label>
              <input type="text" id="db-user" class="form-input"
                     placeholder="(optional)">
            </div>
            <div class="form-group">
              <label class="form-label" for="db-pass">Password</label>
              <input type="password" id="db-pass" class="form-input"
                     placeholder="(optional)">
            </div>
            <div class="form-group" id="db-name-group">
              <label class="form-label" for="db-name">Database</label>
              <input type="text" id="db-name" class="form-input"
                     placeholder="(default)">
              <p class="form-help">Database name (optional)</p>
            </div>
            <div class="form-group" id="db-ssl-group">
              <label class="form-checkbox">
                <input type="checkbox" id="db-ssl">
                <span class="checkmark"></span>
                <span>Use SSL/TLS encryption</span>
              </label>
              <p class="form-help">Enable for neo4j+s:// connections (recommended for production)</p>
            </div>
          </div>

          <!-- Error display -->
          <div id="db-connect-error" class="form-error" hidden></div>
        </form>
      </div>
      <div class="modal-footer">
        <button id="db-connect-cancel-btn" class="btn btn-secondary">Cancel</button>
        <button id="db-connect-btn" class="btn btn-primary">Connect</button>
      </div>
    </div>
  `;

  // Add event listeners
  modal.querySelector("#db-connect-close-btn")?.addEventListener("click", closeDbConnect);
  modal.querySelector("#db-connect-cancel-btn")?.addEventListener("click", closeDbConnect);
  modal.querySelector("#db-connect-btn")?.addEventListener("click", connectToDatabase);
  modal.querySelector("#db-path-browse")?.addEventListener("click", browseForPath);

  // Submit on Enter key
  modal.querySelector("#db-connect-form")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      connectToDatabase();
    }
  });

  // Populate database type tabs based on supported types
  const tabsContainer = modal.querySelector("#db-type-tabs");
  if (tabsContainer) {
    // Use supportedDatabases if available, otherwise fall back to defaults
    const databases =
      supportedDatabases.length > 0
        ? supportedDatabases
        : [
            { id: "kuzu" as DatabaseType, name: "KuzuDB", connection_type: "file" as const },
            { id: "cozo" as DatabaseType, name: "CozoDB", connection_type: "file" as const },
            { id: "crustdb" as DatabaseType, name: "CrustDB", connection_type: "file" as const },
            { id: "neo4j" as DatabaseType, name: "Neo4j", connection_type: "network" as const },
            { id: "falkordb" as DatabaseType, name: "FalkorDB", connection_type: "network" as const },
          ];

    for (const db of databases) {
      const btn = document.createElement("button");
      btn.className = "db-type-tab";
      btn.setAttribute("data-type", db.id);
      btn.textContent = db.name;
      if (db.id === selectedDbType) {
        btn.classList.add("active");
      }
      tabsContainer.appendChild(btn);
    }
  }

  // Tab switching
  const tabs = modal.querySelectorAll(".db-type-tab");
  for (const tab of tabs) {
    tab.addEventListener("click", () => {
      selectedDbType = tab.getAttribute("data-type") as DatabaseType;
      for (const t of tabs) {
        t.classList.toggle("active", t === tab);
      }
      updateFormFields();
    });
  }

  // Close on backdrop click
  modal.addEventListener("click", (e) => {
    if (e.target === modal) {
      closeDbConnect();
    }
  });

  return modal;
}

/** Fetch supported database types from server */
async function fetchSupportedDatabases(): Promise<void> {
  try {
    const result = await api.get<SupportedDatabaseInfo[]>("/api/database/supported");
    supportedDatabases = result;
    // Set default to first supported type
    if (supportedDatabases.length > 0) {
      selectedDbType = supportedDatabases[0]!.id;
    }
  } catch (error) {
    console.error("Failed to fetch supported databases:", error);
    // Fall back to showing all types
    supportedDatabases = [
      { id: "kuzu", name: "KuzuDB", connection_type: "file" },
      { id: "cozo", name: "CozoDB", connection_type: "file" },
      { id: "crustdb", name: "CrustDB", connection_type: "file" },
      { id: "neo4j", name: "Neo4j", connection_type: "network" },
      { id: "falkordb", name: "FalkorDB", connection_type: "network" },
    ];
  }
}

/** Initialize the database connection component */
export function initDbConnect(): void {
  // Fetch supported database types and connection status
  fetchSupportedDatabases();
  refreshConnectionStatus();

  // Add click handler for connection status indicator
  const statusEl = document.getElementById("connection-status");
  if (statusEl) {
    statusEl.addEventListener("click", () => {
      if (appState.databaseConnected) {
        openDbManager();
      } else {
        openDbConnect();
      }
    });
  }
}
