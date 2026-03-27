# Duplicate formatDuration and getQueryErrorMessage implementations

**Severity: MEDIUM** | **Category: duplicate-code**

## Problem

1. `formatDuration(ms)` is implemented identically in:
   - `components/run-query.ts:147-157`
   - `components/query-history.ts:320-331`

2. `getQueryErrorMessage` exists in:
   - `utils/query.ts` (canonical, imported by most components)
   - `components/run-query.ts:414-422` (private copy)

## Solution

Move `formatDuration` to `utils/query.ts` (or a new `utils/format.ts`) and
import it in both components. Delete the private `getQueryErrorMessage` in
`run-query.ts` and import from `utils/query.ts`.
