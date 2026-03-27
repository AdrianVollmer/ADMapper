# Escape key listeners accumulate and are never removed

**Severity: MEDIUM** | **Category: vibe-coding-smell**

## Problem

Every time a modal is created, a new global `document.addEventListener("keydown", ...)`
is registered and never removed. This happens in:

- `add-node-edge.ts` (4 modals = 4 listeners)
- `insights.ts`
- `settings.ts`
- `db-manager.ts`
- `generate-data.ts`
- `keyboard.ts`

Since modals are created on demand (not recreated), this isn't a growing leak
per se, but it means ~10+ permanent global keydown listeners exist after the
user opens each modal once. They all fire on every keypress.

## Solution

Either:
1. Register a single global Escape handler that checks which modal is visible
2. Or add/remove listeners when modals open/close
3. Or since modals are created once, accept the current approach but document
   it — the performance impact is negligible
