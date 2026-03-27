# Duplicate label-drawing and Sigma program initialization

**Severity: MEDIUM** | **Category: duplicate-code**

## Problem

1. **Label drawing**: `magnifier.ts:131-164` (`drawLensLabel`) is a
   stripped-down copy of `ADGraphRenderer.ts:176-241` (`drawLabel`). Core
   rendering logic (measuring text, centering, drawing clear/blurred parts)
   is duplicated.

2. **Sigma program init**: Both `magnifier.ts:112-128` and
   `ADGraphRenderer.ts:282-302` create `NodeImageProgram` and
   `CurvedArrowProgram` with identical configuration objects:

```typescript
const NodeImageProgram = createNodeImageProgram({
  size: { mode: "force", value: 512 },
  drawingMode: "background",
  colorAttribute: "color",
  imageAttribute: "image",
  padding: 0.12,
  keepWithinCircle: true,
});
```

## Solution

1. Extract a shared `drawNodeLabel(ctx, node, options)` function.
2. Extract `createSigmaPrograms()` returning the shared program config.
