# Modal utility exists but is ignored by most components

## Severity: HIGH

## Problem

`src/frontend/utils/modal.ts` provides a `createModal()` utility that
handles modal DOM construction, close buttons, keyboard handling, and
lifecycle. However, most components manually construct modal DOM with
`innerHTML`:

- `add-node-edge.ts` (lines 96-170)
- `insights.ts` (lines 183-218)
- `run-query.ts` (lines 62-91)
- `manage-queries.ts`
- `query-history.ts`

There are 91 innerHTML assignments across 18 files. Additionally,
`notifications.ts:showConfirm()` (lines 76-137) completely reimplements
modal logic that `createModal()` already provides.

## Solution

Migrate all components to use `createModal()`. Extend the utility if it's
missing features that caused components to roll their own. Then remove the
duplicate implementation in `showConfirm()`.

This is a high-leverage cleanup — it centralizes modal behavior, making
styling changes and accessibility improvements automatic across the app.
