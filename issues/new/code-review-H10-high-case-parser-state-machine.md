# Confusing state machine in CASE expression parser

## Severity: HIGH

## Problem

In `src/crustdb/src/query/parser/expression.rs` (lines 872-901), the CASE
expression parser uses a `saw_first_expr` boolean flag with
`#[allow(unused_assignments)]` to distinguish between the CASE operand and
ELSE expression. The flag is set in two places with different meanings,
creating an implicit state machine that is fragile and hard to follow.

Similarly, `build_case_alternative` (lines 907-929) uses an `is_then` flag
to distinguish WHEN vs THEN expressions, relying on parse tree ordering.

## Solution

Replace the boolean flags with explicit state tracking. For example, use
the parse tree structure directly:

```rust
let children: Vec<_> = pair.into_inner().collect();
// First Expression (if before any CaseAlternative) is the operand
// Last Expression (after all CaseAlternatives) is the ELSE
```

Or use an enum for explicit state transitions instead of booleans.
