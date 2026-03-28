# DRY violation: binary expression parser functions

## Severity: HIGH

## Problem

In `src/crustdb/src/query/parser/expression.rs` (lines 33-107), three
functions — `build_or_expression`, `build_xor_expression`, and
`build_and_expression` — are identical copy-paste with only the operator and
child rule differing.

Each follows the same pattern: collect operands, fold left into
`Expression::BinaryOp`. This makes maintenance harder and risks inconsistency
if one copy is updated but not the others.

## Solution

Extract a generic helper:

```rust
fn build_left_assoc_binary(
    pair: Pair<Rule>,
    child_rule: Rule,
    op: BinaryOperator,
    child_builder: fn(Pair<Rule>) -> Result<Expression>,
) -> Result<Expression> {
    // shared fold logic
}
```

Then each function becomes a one-liner calling this helper.
