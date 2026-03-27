# Duplicate QueryProgressEvent interface with subtly different types

**Severity: HIGH** | **Category: duplicate-code / inconsistency**

## Problem

`QueryProgressEvent` is defined in two places with different field types:

```typescript
// api/types.ts:101
export interface QueryProgressEvent {
  duration_ms: number | null;   // nullable
  result_count: number | null;  // nullable
  error: string | null;         // nullable
  results?: QueryResult;
  graph?: GraphData;
}

// api/transport.ts:66
export interface QueryProgressEvent {
  duration_ms?: number;         // optional (undefined, not null)
  result_count?: number;        // optional
  error?: string;               // optional
  results?: unknown;            // unknown, not QueryResult
  graph?: { nodes: unknown[]; relationships: unknown[] }; // inline type
}
```

This causes silent type mismatches — code using one definition may fail at
runtime when receiving data shaped by the other.

## Solution

Keep a single `QueryProgressEvent` definition in `api/types.ts` and import it
in `transport.ts`. Choose `T | null` or `T | undefined` consistently (prefer
`T | null` since this comes from JSON where `null` is explicit).
