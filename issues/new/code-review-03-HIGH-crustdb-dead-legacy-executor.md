# ~1,200 lines of dead legacy executor code in CrustDB

**Severity: HIGH** | **Category: dead-code**

## Problem

CrustDB was refactored from direct AST execution to a planner/plan-executor
architecture. The entire legacy execution path was left in place, hidden by a
crate-level `#![allow(dead_code)]` in `src/crustdb/src/lib.rs:9` and
`#[allow(unused_imports)]` on re-exports in `executor/mod.rs:26-45`.

Dead modules (confirmed — no callers outside their own re-exports):

- `query/executor/pattern/mod.rs` (~100 lines)
- `query/executor/pattern/matching.rs` (~240 lines)
- `query/executor/pattern/traversal.rs` (~594 lines)
- `query/executor/pattern/shortest_path.rs` (~295 lines)
- `query/executor/eval.rs` (~575 lines)
- `query/executor/aggregate.rs` (~83 lines)
- `query/executor/result.rs` (~265 lines)
- `query/executor/create.rs` (~221 lines)
- `query/executor/mutation.rs` (~109 lines)

The backend crate only imports `crustdb::{Database, EntityCacheConfig}` and
never calls any of these functions.

This doubles the maintenance surface and creates inconsistency risks (e.g.,
the legacy evaluator uses relative float tolerance `1e-10` while the plan
executor uses `f64::EPSILON`).

## Solution

1. Delete the dead modules listed above.
2. Remove the `#![allow(dead_code)]` from `lib.rs` and fix any remaining
   legitimate warnings with targeted `#[allow]` or `#[cfg]` attributes.
3. Clean up the re-exports in `executor/mod.rs`.
