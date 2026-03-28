# Frontend components exceed 1000-line guideline

## Severity: MEDIUM

## Problem

Several frontend files exceed the project's own guideline of <1000 lines
per file (from AGENTS.md):

- `sidebars.ts`: 1317 lines
- `insights.ts`: 1178 lines
- `manage-queries.ts`: 1168 lines
- `layout.ts`: 962 lines (close to limit)

`sidebars.ts` in particular mixes property formatting, detail panel
rendering, status indicators, and sidebar toggle logic. `insights.ts` has
multiple tab implementations that could be separate modules.

## Solution

Split by responsibility:

- **sidebars.ts** → `detail-panel.ts`, `formatters.ts`, `sidebar-toggle.ts`
- **insights.ts** → one file per tab (da-analysis, reachability,
  stale-objects, etc.)
- **manage-queries.ts** → `query-tree.ts`, `query-form.ts`
- **layout.ts** → `layout-force.ts`, `layout-hierarchical.ts`, etc.
