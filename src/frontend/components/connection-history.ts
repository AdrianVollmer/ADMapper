/**
 * Connection History
 *
 * Stores and retrieves recent database connections.
 * Uses localStorage for web, or XDG_DATA_HOME/admapper for Tauri.
 */

const STORAGE_KEY = "admapper_recent_connections";
const MAX_CONNECTIONS = 10;

export interface ConnectionEntry {
  url: string;
  displayName: string;
  databaseType: string;
  lastUsed: number;
}

/** Get recent connections from storage */
export async function getRecentConnections(): Promise<ConnectionEntry[]> {
  // Try Tauri file storage first
  if ("__TAURI__" in window) {
    try {
      return await loadFromTauriStorage();
    } catch (err) {
      console.warn("Failed to load from Tauri storage, falling back to localStorage:", err);
    }
  }

  // Fallback to localStorage
  return loadFromLocalStorage();
}

/** Save a connection to history */
export async function saveConnection(entry: Omit<ConnectionEntry, "lastUsed">): Promise<void> {
  const connections = await getRecentConnections();

  // Remove existing entry with same URL
  const filtered = connections.filter((c) => c.url !== entry.url);

  // Add new entry at the beginning
  const newEntry: ConnectionEntry = {
    ...entry,
    lastUsed: Date.now(),
  };
  filtered.unshift(newEntry);

  // Keep only MAX_CONNECTIONS entries
  const trimmed = filtered.slice(0, MAX_CONNECTIONS);

  // Save
  if ("__TAURI__" in window) {
    try {
      await saveToTauriStorage(trimmed);
      return;
    } catch (err) {
      console.warn("Failed to save to Tauri storage, falling back to localStorage:", err);
    }
  }

  saveToLocalStorage(trimmed);
}

/** Clear connection history */
export async function clearConnectionHistory(): Promise<void> {
  if ("__TAURI__" in window) {
    try {
      await saveToTauriStorage([]);
      return;
    } catch (err) {
      console.warn("Failed to clear Tauri storage:", err);
    }
  }
  localStorage.removeItem(STORAGE_KEY);
}

/** Load connections from localStorage */
function loadFromLocalStorage(): ConnectionEntry[] {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (!stored) return [];
    const parsed = JSON.parse(stored);
    if (!Array.isArray(parsed)) return [];
    return parsed;
  } catch {
    return [];
  }
}

/** Save connections to localStorage */
function saveToLocalStorage(connections: ConnectionEntry[]): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(connections));
}

/** Load connections from Tauri storage (XDG_DATA_HOME/admapper) */
async function loadFromTauriStorage(): Promise<ConnectionEntry[]> {
  // @ts-expect-error Tauri global
  const { readTextFile, BaseDirectory } = await window.__TAURI__.fs;
  // @ts-expect-error Tauri global
  const { join, appDataDir } = await window.__TAURI__.path;

  const appDir = await appDataDir();
  const filePath = await join(appDir, "recent_connections.json");

  try {
    const content = await readTextFile(filePath, { dir: BaseDirectory.AppData });
    const parsed = JSON.parse(content);
    if (!Array.isArray(parsed)) return [];
    return parsed;
  } catch {
    // File doesn't exist or is invalid
    return [];
  }
}

/** Save connections to Tauri storage */
async function saveToTauriStorage(connections: ConnectionEntry[]): Promise<void> {
  // @ts-expect-error Tauri global
  const { writeTextFile, createDir, BaseDirectory } = await window.__TAURI__.fs;
  // @ts-expect-error Tauri global
  const { appDataDir } = await window.__TAURI__.path;

  // Ensure directory exists
  const appDir = await appDataDir();
  try {
    await createDir(appDir, { recursive: true });
  } catch {
    // Directory might already exist
  }

  await writeTextFile("recent_connections.json", JSON.stringify(connections, null, 2), {
    dir: BaseDirectory.AppData,
  });
}

/** Get a display name from a connection URL */
export function getDisplayName(url: string, dbType: string): string {
  try {
    // For file-based databases, show the file/directory name
    if (url.startsWith("kuzu://") || url.startsWith("cozodb://")) {
      const path = url.replace(/^(kuzu|cozodb):\/\//, "");
      const parts = path.split("/");
      const name = parts.pop() || parts.pop() || path;
      return `${name} (${dbType})`;
    }

    // For network databases, show host:port
    const match = url.match(/:\/\/(?:[^@]+@)?([^/:]+)(?::(\d+))?/);
    if (match) {
      const host = match[1] ?? "localhost";
      const port = match[2];
      return port ? `${host}:${port} (${dbType})` : `${host} (${dbType})`;
    }

    return `${dbType} database`;
  } catch {
    return `${dbType} database`;
  }
}
