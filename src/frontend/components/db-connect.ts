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
import { showNoConnectionPlaceholder } from "./graph-view";
import { saveConnection, getDisplayName } from "./connection-history";

/** Database types */
type DatabaseType = "kuzu" | "cozo" | "neo4j" | "falkordb";

/** Current selected database type */
let selectedDbType: DatabaseType = "kuzu";

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
}

/** Fetch and update connection status from server */
export async function refreshConnectionStatus(): Promise<void> {
  try {
    const response = await fetch("/api/database/status");
    if (response.ok) {
      const status = await response.json();
      appState.databaseConnected = status.connected;
      appState.databaseType = status.database_type || null;
      updateConnectionStatus();
    }
  } catch (error) {
    console.error("Failed to fetch database status:", error);
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

  const isFile = selectedDbType === "kuzu" || selectedDbType === "cozo";

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
    const response = await fetch("/api/database/connect", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    });

    if (response.ok) {
      const result = await response.json();
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
    } else {
      const error = await response.text();
      showError(error || "Connection failed");
    }
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
    const response = await fetch("/api/database/connect", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    });

    if (response.ok) {
      const result = await response.json();
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
    } else {
      const error = await response.text();
      console.error("Connection failed:", error);
      return false;
    }
  } catch (error) {
    console.error("Connection error:", error);
    return false;
  }
}

/** Disconnect from database */
export async function disconnectDb(): Promise<void> {
  try {
    await fetch("/api/database/disconnect", { method: "POST" });
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

/** Browse for database path using Tauri dialog */
async function browseForPath(): Promise<void> {
  // Check if Tauri is available
  if (!("__TAURI__" in window)) {
    // Fallback: just focus the input field
    document.getElementById("db-path")?.focus();
    return;
  }

  try {
    // @ts-expect-error Tauri global
    const { open } = await window.__TAURI__.dialog;

    // KuzuDB uses a directory, CozoDB uses a file
    const isDirectory = selectedDbType === "kuzu";

    const selected = await open({
      directory: isDirectory,
      multiple: false,
      title: isDirectory ? "Select Database Directory" : "Select Database File",
    });

    if (selected && typeof selected === "string") {
      const pathInput = document.getElementById("db-path") as HTMLInputElement | null;
      if (pathInput) {
        pathInput.value = selected;
      }
    }
  } catch (error) {
    console.error("Failed to open file dialog:", error);
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

  // Reset to default type
  selectedDbType = "kuzu";
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
        <!-- Database Type Tabs -->
        <div class="db-type-tabs">
          <button class="db-type-tab active" data-type="kuzu">KuzuDB</button>
          <button class="db-type-tab" data-type="cozo">CozoDB</button>
          <button class="db-type-tab" data-type="neo4j">Neo4j</button>
          <button class="db-type-tab" data-type="falkordb">FalkorDB</button>
        </div>

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

/** Initialize the database connection component */
export function initDbConnect(): void {
  // Fetch initial connection status
  refreshConnectionStatus();
}
