# Two different localStorage keys for custom queries

**Severity: MEDIUM** | **Category: inconsistency**

## Problem

Custom queries are stored under two different localStorage keys:

```typescript
// queries/index.ts:60
const stored = localStorage.getItem("admapper-custom-queries");

// manage-queries.ts:3
const STORAGE_KEY = "admapper_custom_queries";
```

Note: `admapper-custom-queries` (hyphens) vs `admapper_custom_queries`
(underscores). Queries saved in one system are invisible to the other.

## Solution

Consolidate to a single key. Update whichever is the older/less-used
reference to match the other. Add a migration path if users may have data
under the old key.
