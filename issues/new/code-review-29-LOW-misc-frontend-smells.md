# Miscellaneous frontend code smells (LOW severity)

**Severity: LOW** | **Category: various**

## Problem

A collection of minor issues:

1. **Grammar error** (`add-node-edge.ts:405`):
   `"Please select an relationship type"` → should be "a relationship type"

2. **`executeQueryWithHistory` misleading name** (`utils/query.ts:303-311`):
   Accepts a `_name` parameter that is completely ignored. Comment says
   "History is managed automatically by the backend." The function is a
   passthrough to `executeQuery`. Name suggests it manages history but it
   doesn't.

3. **Single shared `searchTimer` for both inputs** (`add-node-edge.ts:54`):
   One `searchTimer` is shared for source AND target search in the Add
   Relationship modal. Typing in one cancels the debounce of the other.

4. **`showConfirm` used for non-confirmatory messages** (`db-manager.ts`):
   `showConfirm("The query cache is already empty.", { confirmText: "OK" })`
   — this is an alert, not a confirmation.

5. **`insights.ts` re-renders all 7 tabs on every state change**: Every
   `loadX()` function calls `renderModal()` which rebuilds all tab HTML.
   With 6 parallel loads, this means ~13 full re-renders in quick succession.

6. **`edit-tiers.ts` filter mismatch**: Group/OU filters only affect the
   backend batch-assign payload, not the client-side table filtering. User
   sees all nodes but batch assign affects a subset.

7. **`manage-queries.ts` subcategory deletion broken**: `handleDeleteCategory`
   only searches top-level categories (`categories.findIndex`), so
   subcategories cannot be deleted.

8. **`isDirectory` hardcoded to false** (`db-connect.ts:421`): Dead logic —
   `isSelectable` is always `true`.

9. **`_dbType` unused parameter** (`connection-history.ts:136`):
   `getDisplayName(url, _dbType)` never uses `_dbType`.

10. **`generateDemoGraph` ignores `_nodeCount` parameter**
    (`graph-view.ts:373`): Demo graph is hardcoded regardless of input.

## Solution

Fix each individually — they are all small, independent changes.
