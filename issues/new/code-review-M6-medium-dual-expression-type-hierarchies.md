# Dual expression type hierarchies in query operators

## Severity: MEDIUM

## Problem

`src/crustdb/src/query/operators.rs` defines two parallel type systems:

- `PlanExpr` (lines 332-358): Literal, Variable, Property, Function,
  PathLength, Case
- `FilterPredicate` (lines 251-330): Eq, Ne, Lt, Le, Gt, Ge, And, Or, Not,
  IsNull, IsNotNull, StartsWith, EndsWith, Contains, Regex, HasLabel, In,
  ListPredicate, True

These overlap conceptually and increase code duplication. Adding a new
operation requires updating both hierarchies. Evaluation logic is split
across separate match blocks.

## Solution

Consider unifying: `FilterPredicate` could be a special case of `PlanExpr`
(an expression that evaluates to boolean), or `PlanExpr` could subsume
predicates. This would simplify the executor and planner.
