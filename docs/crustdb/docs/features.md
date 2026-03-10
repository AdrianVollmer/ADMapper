# Feature Support

CrustDB implements a subset of [openCypher 9](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf), the vendor-neutral graph query language specification. This page lists supported and unsupported features.

## Implemented Features

### Clauses

| Feature | Notes |
|---------|-------|
| `CREATE` | Nodes with labels and properties, relationships with types and properties |
| `MATCH` | Pattern matching for nodes and relationships |
| `WHERE` | Filtering with comparisons, boolean operators, string predicates |
| `RETURN` | Projections, aliases, expressions |
| `SET` | Update properties, add labels |
| `DELETE` | Delete nodes and relationships |
| `DETACH DELETE` | Delete nodes with their relationships |
| `ORDER BY` | Sort results ascending or descending |
| `LIMIT` | Limit result count |
| `SKIP` | Skip first N results |
| `DISTINCT` | Remove duplicate rows |
| `UNION ALL` | Concatenate results from multiple queries |

### Pattern Matching

| Feature | Notes |
|---------|-------|
| Node patterns | `(n)`, `(n:Label)`, `(n:Label {prop: value})` |
| Relationship patterns | `-[r]->`, `-[r:TYPE]->`, `-[r:TYPE {prop: value}]->` |
| Undirected relationships | `(a)-[r]-(b)` |
| Multiple labels | `(n:Label1:Label2)` |
| Variable-length paths | `(a)-[:TYPE*1..3]->(b)` |
| Shortest path | `shortestPath((a)-[:TYPE*]->(b))` |
| All shortest paths | `allShortestPaths((a)-[:TYPE*]->(b))` |

### Operators

| Feature | Notes |
|---------|-------|
| Comparison | `=`, `<>`, `<`, `>`, `<=`, `>=` |
| Boolean | `AND`, `OR`, `NOT` |
| String predicates | `STARTS WITH`, `ENDS WITH`, `CONTAINS` |
| Null checks | `IS NULL`, `IS NOT NULL` |
| List membership | `IN` |

### Functions

| Feature | Notes |
|---------|-------|
| `count()` | Count nodes, relationships, or rows |
| `type()` | Get relationship type |
| `labels()` | Get node labels |
| `id()` | Get internal ID |
| `length()` | Path length |

### Data Types

| Type | Notes |
|------|-------|
| Integer | 64-bit signed |
| Float | 64-bit |
| Boolean | `true`, `false` |
| String | UTF-8 |
| Null | `null` |
| List | Heterogeneous |
| Map | String keys |

## Partially Implemented

| Feature | Status |
|---------|--------|
| `MERGE` | Parser support only, executor not implemented |
| Aggregation functions | `count()` works, `sum()`, `avg()`, `min()`, `max()` in progress |

## Not Implemented

### Clauses

| Feature | Notes |
|---------|-------|
| `OPTIONAL MATCH` | Left outer join semantics |
| `WITH` | Query chaining and aggregation scoping |
| `UNWIND` | List expansion |
| `UNION` | Deduplicated combine (`UNION ALL` is supported, `UNION` dedup is not) |
| `CALL` | Procedure calls |
| `FOREACH` | Iterative updates |
| `LOAD CSV` | CSV import |

### Expressions

| Feature | Notes |
|---------|-------|
| `CASE` | Conditional expressions |
| List comprehensions | `[x IN list WHERE condition \| expression]` |
| Pattern comprehensions | `[(a)-->(b) \| b.name]` |
| Subqueries | `EXISTS {}`, `COUNT {}` |
| Map projections | `node {.prop1, .prop2}` |

### Functions

| Feature | Notes |
|---------|-------|
| `collect()` | Aggregate into list |
| `sum()`, `avg()`, `min()`, `max()` | Numeric aggregations |
| `head()`, `tail()`, `last()` | List functions |
| `keys()`, `properties()` | Property introspection |
| `coalesce()` | Null handling |
| `toString()`, `toInteger()`, `toFloat()` | Type conversion |
| `size()` | Collection size |
| `range()` | Generate list |
| String functions | `trim()`, `toLower()`, `toUpper()`, `split()`, `replace()` |
| Math functions | `abs()`, `ceil()`, `floor()`, `round()`, `sqrt()` |
| Temporal functions | `date()`, `datetime()`, `duration()` |
| Spatial functions | `point()`, `distance()` |

### Other

| Feature | Notes |
|---------|-------|
| Indexes | Create via API only, not via `CREATE INDEX` |
| Constraints | Not supported |
| Transactions | Implicit only, no explicit `BEGIN`/`COMMIT` |
| Parameters | No parameter substitution |
