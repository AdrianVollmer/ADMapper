# Parser tests use panic!() instead of proper assertions

## Severity: LOW

## Problem

In `src/crustdb/src/query/parser/mod.rs` (lines 341-796), 57 test
assertions use `panic!("Expected ...")` inside `if let` / `else` blocks
instead of `assert!`, `assert_eq!`, or `assert_matches!`.

This gives poor diagnostics on failure — no expected-vs-actual output, no
context about what was actually returned.

Similarly, `src/crustdb/src/query/planner/optimizer/tests.rs` has 20+
`panic!` calls with generic messages instead of assertions.

## Solution

Replace with proper assertions:

```rust
// Before
if let Expression::BinaryOp { op, .. } = &result {
    assert_eq!(*op, BinaryOperator::And);
} else {
    panic!("Expected BinaryOp");
}

// After
assert_matches!(&result, Expression::BinaryOp { op, .. } if *op == BinaryOperator::And);
```

Or use helper assertion functions for recurring plan structure checks.
