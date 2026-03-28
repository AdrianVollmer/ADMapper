# Dead code suppressed with #[allow(dead_code)]

## Severity: LOW

## Problem

Multiple `#[allow(dead_code)]` attributes suppress warnings instead of
addressing the root cause:

- `src/backend/src/generate.rs:15-33` — `Tier` enum and `Domain` struct
- `src/backend/src/db/types.rs:198-208` — `WELL_KNOWN_PRINCIPALS` and
  `DOMAIN_ADMIN_SID_SUFFIX` (actually used cross-module but flagged)
- `src/crustdb/src/query/operators.rs:98,311,423,443` — unused plan
  operators
- `src/crustdb/src/storage/query.rs:258` — `find_relationships_by_type`
- `src/crustdb/src/query/executor/mod.rs:175` — `execute` function

## Solution

For each case, either:
1. Use the code (complete the implementation)
2. Remove it if it's truly dead
3. Fix the visibility issue if it's used cross-crate but Rust doesn't see it
   (e.g. `pub(crate)` adjustments)
