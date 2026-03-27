# ICON_COLORS duplicates NODE_COLORS with identical values

**Severity: HIGH** | **Category: duplicate-code**

## Problem

`graph/icons.ts:62-76` defines `ICON_COLORS` as a `Record<ADNodeType, string>`
with exactly the same values as `NODE_COLORS` in `graph/theme.ts:11-25`. The
comment even says "matching theme.ts".

```typescript
// icons.ts:62
const ICON_COLORS: Record<ADNodeType, string> = {
  User: "#22b8cf",
  Group: "#fab005",
  // ... identical to NODE_COLORS
};
```

If a color is changed in one place but not the other, node icons and node
badges will be out of sync.

## Solution

Delete `ICON_COLORS` from `icons.ts` and import `NODE_COLORS` from
`graph/theme.ts`.
