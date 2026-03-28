/**
 * Query Storage
 *
 * Persistence logic for custom queries stored in XDG_DATA_HOME/admapper/customqueries.json
 * (Tauri desktop) or browser localStorage (web).
 */

import type { Query, QueryCategory } from "./queries";

export const STORAGE_KEY = "admapper_custom_queries";

/** Custom queries schema for validation */
export interface CustomQueriesFile {
  version: number;
  categories: QueryCategory[];
}

/** Default empty queries file */
export const DEFAULT_QUERIES: CustomQueriesFile = {
  version: 1,
  categories: [],
};

/** Example queries for new users */
export const EXAMPLE_QUERIES: CustomQueriesFile = {
  version: 1,
  categories: [
    {
      id: "my-queries",
      name: "My Custom Queries",
      expanded: true,
      queries: [
        {
          id: "example-1",
          name: "All Users",
          description: "Find all user objects",
          query: "MATCH (u:User) RETURN u",
        },
        {
          id: "example-2",
          name: "Admin Groups",
          description: "Find groups with 'admin' in the name",
          query: "MATCH (g:Group) WHERE g.label CONTAINS 'admin' RETURN g",
        },
      ],
    },
  ],
};

/** Load custom queries from storage */
export async function loadCustomQueries(): Promise<CustomQueriesFile> {
  // Try Tauri storage first
  if ("__TAURI__" in window) {
    try {
      return await loadFromTauriStorage();
    } catch {
      // Fall back to localStorage
    }
  }

  return loadFromLocalStorage();
}

/** Get custom categories for the query browser */
export async function getCustomCategories(): Promise<QueryCategory[]> {
  const queries = await loadCustomQueries();
  return queries.categories;
}

/** Load from localStorage */
export function loadFromLocalStorage(): CustomQueriesFile {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (!stored) return DEFAULT_QUERIES;
    const parsed = JSON.parse(stored);
    if (!validateSchema(parsed)) {
      console.warn("Invalid custom queries schema in localStorage");
      return DEFAULT_QUERIES;
    }
    return parsed;
  } catch {
    return DEFAULT_QUERIES;
  }
}

/** Save to localStorage */
export function saveToLocalStorage(queries: CustomQueriesFile): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(queries));
}

/** Load from Tauri storage (XDG_DATA_HOME/admapper/customqueries.json) */
export async function loadFromTauriStorage(): Promise<CustomQueriesFile> {
  // @ts-expect-error Tauri global
  const { readTextFile, BaseDirectory } = await window.__TAURI__.fs;

  try {
    const content = await readTextFile("customqueries.json", { dir: BaseDirectory.AppData });
    const parsed = JSON.parse(content);
    if (!validateSchema(parsed)) {
      console.warn("Invalid custom queries schema in Tauri storage");
      return DEFAULT_QUERIES;
    }
    return parsed;
  } catch {
    return DEFAULT_QUERIES;
  }
}

/** Save to Tauri storage */
export async function saveToTauriStorage(queries: CustomQueriesFile): Promise<void> {
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

  await writeTextFile("customqueries.json", JSON.stringify(queries, null, 2), {
    dir: BaseDirectory.AppData,
  });
}

/** Validate the schema of a queries file */
export function validateSchema(data: unknown): data is CustomQueriesFile {
  if (!data || typeof data !== "object") return false;
  const obj = data as Record<string, unknown>;

  if (typeof obj.version !== "number") return false;
  if (!Array.isArray(obj.categories)) return false;

  // Validate each category
  for (const cat of obj.categories) {
    if (!validateCategory(cat)) return false;
  }

  return true;
}

/** Validate a query category */
export function validateCategory(cat: unknown): cat is QueryCategory {
  if (!cat || typeof cat !== "object") return false;
  const obj = cat as Record<string, unknown>;

  if (typeof obj.id !== "string" || !obj.id) return false;
  if (typeof obj.name !== "string" || !obj.name) return false;

  // Optional queries array
  if (obj.queries !== undefined) {
    if (!Array.isArray(obj.queries)) return false;
    for (const query of obj.queries) {
      if (!validateQuery(query)) return false;
    }
  }

  // Optional subcategories array
  if (obj.subcategories !== undefined) {
    if (!Array.isArray(obj.subcategories)) return false;
    for (const subcat of obj.subcategories) {
      if (!validateCategory(subcat)) return false;
    }
  }

  return true;
}

/** Validate a query */
export function validateQuery(query: unknown): query is Query {
  if (!query || typeof query !== "object") return false;
  const obj = query as Record<string, unknown>;

  if (typeof obj.id !== "string" || !obj.id) return false;
  if (typeof obj.name !== "string" || !obj.name) return false;
  if (typeof obj.query !== "string" || !obj.query) return false;

  return true;
}
