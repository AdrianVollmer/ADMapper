# Inconsistent error message styling in CrustDB parser

## Severity: LOW

## Problem

Error messages in `src/crustdb/src/query/parser/` use inconsistent
capitalization and phrasing:

- "Expression requires OrExpression" (expression.rs:22)
- "OR expression requires operands" (expression.rs:43)
- "Expression requires a pattern" (clause.rs:51)

Some capitalize, some don't. Some say "requires", others say "missing".

## Solution

Standardize on a consistent format, e.g. lowercase with "expected" phrasing:

- "expected OR expression operands"
- "expected pattern in expression"

A quick grep-and-replace pass through the parser error strings.
