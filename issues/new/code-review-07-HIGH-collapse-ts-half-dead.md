# Half of collapse.ts is dead code (edge-collapse functions)

**Severity: HIGH** | **Category: dead-code**

## Problem

`graph/collapse.ts` exports two sets of functions:

**Used** (imported by `ADGraphRenderer.ts`, `magnifier.ts`, `graph-view.ts`):
- `isNodeCollapsed`, `getHiddenChildCount`, `getHiddenNodeIds`,
  `toggleNodeCollapse`, `clearCollapseState`

**Dead** (never imported or called anywhere):
- `collapseParallelEdges`, `expandParallelEdges`, `toggleEdgeCollapse`,
  `areEdgesCollapsed`, `getCollapsedEdgeInfo`, `getVisibleEdgeForCollapsedGroup`,
  `isEdgeHidden`, `isNodeHidden`, `getCollapsedNodeIds`

The parallel edge collapsing is handled by a different private
`collapseParallelEdges` in `ADGraph.ts:106`, which works at the graph data
level. The same-name collision is confusing.

## Solution

Delete the 7 dead edge-collapse functions and the `collapsedEdges` Map.
Keep the node-collapse functions that are actively used. Also remove the
dead `isNodeHidden` and `getCollapsedNodeIds` functions.
