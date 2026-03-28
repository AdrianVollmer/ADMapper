# Inconsistent use of || vs ?? for null coalescing

## Severity: LOW

## Problem

Frontend code inconsistently uses `||` and `??` for default values:

```typescript
// Some use || (treats "", 0, false as falsy):
const label = graph?.getNodeAttribute(nodeId, "label") || nodeId;

// Others use ?? (only null/undefined):
const label = graph?.getNodeAttribute(nodeId, "label") ?? nodeId;
```

`||` can cause subtle bugs when legitimate values like `""`, `0`, or `false`
should be preserved.

## Solution

Prefer `??` everywhere for null coalescing. Only use `||` when you
intentionally want to treat falsy values as "missing". An ESLint rule like
`prefer-nullish-coalescing` can enforce this.
