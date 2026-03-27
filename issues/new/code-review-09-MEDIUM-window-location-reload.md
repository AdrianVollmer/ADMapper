# window.location.reload() used as data refresh mechanism

**Severity: MEDIUM** | **Category: vibe-coding-smell**

## Problem

After mutations (adding nodes/edges, clearing database, generating data),
several components do a full page reload instead of programmatically
refreshing the graph:

- `components/add-node-edge.ts:420,459`
- `components/db-manager.ts:235,258`
- `components/generate-data.ts:262`

This discards all UI state: selected node, sidebar position, scroll position,
modal state, query history cursor, etc. It's a classic "vibe coding" shortcut.

## Solution

After a mutation, reload graph data via the API and update the graph view
programmatically (e.g., call `loadGraphData()` or a targeted refresh).
The infrastructure for this already exists — `loadGraphData` in
`graph-view.ts` is used elsewhere in the codebase.
