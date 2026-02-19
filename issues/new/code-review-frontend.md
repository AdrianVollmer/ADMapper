# Frontend Code Review

## Overall Assessment

**AI slop rating: Low (2/10)**

This is well-structured vanilla TypeScript code that follows consistent patterns.
It does not exhibit typical AI-generated code symptoms (excessive comments,
over-abstraction, nonsensical naming, or cargo-culted patterns). The
architecture is deliberate and idiomatic.

**Should we switch to React?** No.

The project philosophy explicitly states "HTMX and vanilla TypeScript for the
frontend" and "Focus on server-side rendering, except for diagrams". The current
vanilla approach is appropriate for this use case:

- Heavy WebGL rendering via Sigma.js dominates the UI
- Most dynamic UI is form-based (modals, search)
- Component count is manageable (~20 modules)
- No complex nested state or prop drilling
- Server-side rendering handles the bulk of HTML

React would add ~40KB to the bundle for marginal benefit. The main pain points
(scattered state, large files) can be addressed with refactoring, not a
framework rewrite.

---

## Top 10 Issues

### 1. ~~String-based action dispatch lacks type safety~~ DONE

**File:** `components/actions.ts:23-201`

The `dispatchAction(action: string)` function uses string matching, making it
easy to typo action names without compiler errors.

**Fix:** Use a discriminated union or const enum:

```typescript
const Actions = {
  CONNECT_DB: "connect-db",
  DISCONNECT_DB: "disconnect-db",
  // ...
} as const;
type Action = (typeof Actions)[keyof typeof Actions];
```

### 2. ~~Inconsistent XSS escaping in dynamic HTML~~ DONE

**File:** `components/actions.ts:236-251`

The `updateRecentConnectionsMenu` function manually escapes only `"` and `<`:

```typescript
const escapedName = conn.displayName.replace(/"/g, "&quot;").replace(/</g, "&lt;");
```

This misses `>`, `'`, and `&`. Meanwhile, `escapeHtml()` from `utils/html.ts`
exists and handles all cases correctly.

**Fix:** Use `escapeHtml()` consistently everywhere dynamic content is inserted
into HTML.

### 3. ~~Large switch statement in actions.ts~~ DONE

**File:** `components/actions.ts:23-201`

A 200-line switch with 30+ cases is hard to maintain and causes merge conflicts.

**Fix:** Convert to a lookup table:

```typescript
const actionHandlers: Record<string, () => void> = {
  "connect-db": () => openDbConnect(),
  "disconnect-db": () => disconnectDb(),
  // ...
};
```

### 4. ~~Module-level state scattered across components~~ DONE

**Files:** `search.ts`, `import.ts`, `db-connect.ts`, `queries.ts`

Each component manages its own state via module-level `let` variables:

```typescript
let nodeSearchInput: HTMLInputElement | null = null;
let pathStartInput: HTMLInputElement | null = null;
// ... 6 more
```

This makes testing difficult and creates implicit dependencies between
components.

**Fix:** Either centralize state in `main.ts` appState, or create a minimal
reactive store with an observer pattern for UI updates.

### 5. ~~escapeHtml creates DOM elements on every call~~ DONE

**File:** `utils/html.ts:9-13`

```typescript
export function escapeHtml(str: string): string {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}
```

Creating a DOM element for every escape operation is wasteful, especially in
loops (sidebars.ts renders property lists).

**Fix:** Use string replacement instead:

```typescript
const escapeMap: Record<string, string> = {
  "&": "&amp;",
  "<": "&lt;",
  ">": "&gt;",
  '"': "&quot;",
  "'": "&#39;",
};
export function escapeHtml(str: string): string {
  return str.replace(/[&<>"']/g, (c) => escapeMap[c]!);
}
```

### 6. ~~Large files exceed 500 lines~~ DONE (queries.ts split)

**Files:**
- `sidebars.ts` (929 lines) - combines detail panel, node properties, actions,
  indicators
- `db-connect.ts` (554 lines) - handles 5 database types in one file
- `queries.ts` (559 lines) - mixes built-in and custom query logic

These violate the project convention of "files less than 1000 lines" and make
navigation difficult.

**Fix:** Extract logical sub-modules:
- `sidebars/detail-panel.ts`, `sidebars/node-properties.ts`
- `db-connect/kuzu.ts`, `db-connect/neo4j.ts`, etc.
- `queries/built-in.ts`, `queries/custom.ts`

### 7. Mixing inline onclick handlers with addEventListener

**Files:** `index.html:568,600`, `sidebars.ts:783`, `main.ts:75-83`

Some handlers are inline in HTML:
```html
onclick="showPlaceholderModal()"
```

While most use addEventListener. This creates:
- Global window pollution (`window.showPlaceholderModal`)
- TypeScript declaration hacks
- Inconsistent event handling patterns

**Fix:** Use `data-action` attributes consistently and handle all events via
delegation in `actions.ts`.

### 8. No request cancellation in API client

**File:** `api/client.ts`

The API client doesn't support AbortController, so rapid typing in search
creates race conditions where old responses arrive after new ones.

**Fix:** Add abort support:

```typescript
async get<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, { signal });
  // ...
}
```

### 9. Credential URLs may be logged

**File:** `db-connect.ts:109-126`

Database connection URLs include credentials in the URL string:
```typescript
url += user;
if (pass) url += `:${pass}`;
```

These could be logged to console or sent to error tracking.

**Fix:** Pass credentials separately from the URL, or redact passwords in any
logging. The backend should handle credentials securely.

### 10. Multiple document-level event listeners

**Files:** `sidebars.ts:287-356`, `search.ts:84`, `menubar.ts`

Multiple components attach separate click listeners to `document`:

```typescript
document.addEventListener("click", (e) => { /* handler 1 */ });
document.addEventListener("click", (e) => { /* handler 2 */ });
document.addEventListener("click", (e) => { /* handler 3 */ });
```

Each click event runs through all handlers.

**Fix:** Consolidate into a single delegated event handler in `main.ts` that
routes to components based on `data-*` attributes.

---

## Additional Observations

**Good practices found:**
- Consistent TypeScript usage with proper interfaces
- API types well-defined in `api/types.ts`
- XSS protection via `escapeHtml()` (when used)
- Clean barrel exports with `index.ts` files
- Good keyboard shortcut implementation

**Minor issues not in top 10:**
- TODO comments indicate unfinished features (settings, about, updates)
- CSS-in-JS in `notifications.ts` should move to stylesheets
- No debounce utility - reimplemented in `search.ts`
- `pathStartInput.setAttribute("data-node-id", nodeId)` stores state in DOM
  attributes instead of JS variables
