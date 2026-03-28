# BloodHound test file is 2132 lines with heavy duplication

## Severity: MEDIUM

## Problem

`src/backend/src/import/bloodhound/tests.rs` contains 70 tests in 2132
lines. `test_importer()` is called 18+ times, `serde_json::json!` used 48
times with similar structures. Many tests follow an identical pattern with
minimal variation (e.g. 6+ tests for tier assignment that differ only in the
SID suffix).

Hard to navigate, hard to spot subtle differences between similar tests.

## Solution

1. Split into focused modules: `tests/node_extraction.rs`,
   `tests/edge_extraction.rs`, `tests/import.rs`,
   `tests/bhce_compatibility.rs`.

2. Use parameterized tests with `rstest`:

```rust
#[rstest]
#[case("S-1-5-21-...-512", 0, "Domain Admins")]
#[case("S-1-5-21-...-519", 0, "Enterprise Admins")]
fn test_tier_assignment(#[case] sid: &str, #[case] tier: i64, #[case] name: &str) {
    // single test replaces 6+ copies
}
```
