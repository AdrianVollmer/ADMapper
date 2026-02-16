# CrustDB Test Fixtures

Language-agnostic test cases in TOML format, organized by milestone.

## Attribution

Test cases derived from [GrandCypher](https://github.com/aplbrain/grand-cypher),
licensed under Apache 2.0.

## Format

Each `.toml` file contains one or more test cases:

```toml
[[test]]
name = "descriptive_test_name"
description = "What this test verifies"

# Optional: Graph setup (nodes and edges to create before running the query)
[test.setup]
nodes = [
    { id = "a", labels = ["Person"], properties = { name = "Alice", age = 30 } },
    { id = "b", labels = ["Person"], properties = { name = "Bob" } },
]
edges = [
    { from = "a", to = "b", type = "KNOWS", properties = { since = 2020 } },
]

# The Cypher query to execute
[test.query]
cypher = """
MATCH (n:Person)
RETURN n.name
"""

# Expected result
[test.expected]
# For queries returning rows:
columns = ["n.name"]
rows = [
    ["Alice"],
    ["Bob"],
]

# Or for mutations, specify counts:
# nodes_created = 2
# edges_created = 1

# Or for error cases:
# error = "SyntaxError"
```

## Directory Structure

- `m2_create/` - CREATE clause tests
- `m3_match/` - Basic MATCH and RETURN tests
- `m4_where/` - WHERE clause filtering
- `m5_single_hop/` - Single relationship traversal
- `m6_multi_hop/` - Variable-length paths
- `m7_mutation/` - SET and DELETE operations
- `m8_aggregation/` - Aggregate functions
