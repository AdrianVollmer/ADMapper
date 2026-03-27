# Duplicate hierarchical layout normalization code

**Severity: HIGH** | **Category: duplicate-code**

## Problem

Position normalization (calculate bounds, compute scale, center) is
duplicated verbatim in three places:

- `graph/layout.ts:283-316` (`applyHierarchicalLayout`, sync)
- `graph/layout.ts:715-747` (`applyHierarchicalLayoutAsync`, async)
- `graph/layout-worker.ts:29-55` (web worker)

Additionally, dagre graph construction is duplicated between the sync
function and the layout worker. The force settings merging logic
(`DEFAULT_FORCE_SETTINGS + userForceSettings + options.settings`) is
also copy-pasted between `applyForceLayout` (line 193) and
`applyLayoutAsync` (line 797).

## Solution

Extract shared helpers:
1. `normalizeGraphPositions(positions, targetSize)` for the bounds/scale/center logic.
2. `buildDagreGraph(graph, options)` for dagre setup.
3. `mergeForceSettings(...settings)` for force config merging.

Note: `normalizePositions` already exists as an exported function
(`layout.ts:855`) but is never used — it could be repurposed.
