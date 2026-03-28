# Magic numbers throughout frontend code

## Severity: LOW

## Problem

Hardcoded numeric values appear without named constants:

- `add-node-edge.ts:263` — `setTimeout(..., 200)` debounce delay
- `add-node-edge.ts:256` — `query.length < 2` minimum search length
- `add-node-edge.ts:289` — `limit=10` result limit
- `sidebars.ts` — sidebar width values used inconsistently (sometimes via
  constants, sometimes inline)

## Solution

Extract to named constants:

```typescript
const SEARCH_DEBOUNCE_MS = 200;
const MIN_SEARCH_LENGTH = 2;
const SEARCH_RESULT_LIMIT = 10;
```
