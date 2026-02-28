# Cypher Query Reference

CrustDB supports a subset of the Cypher query language for graph pattern matching and manipulation.

## CREATE

Create nodes and relationships.

### Create a Node

```cypher
CREATE (n:Person {name: 'Alice', age: 30})
```

### Create with Multiple Labels

```cypher
CREATE (n:User:Admin {email: 'admin@example.com'})
```

### Create a Relationship

```cypher
CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})
```

### Create a Chain

```cypher
CREATE (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)
```

## MATCH

Find patterns in the graph.

### Match All Nodes

```cypher
MATCH (n) RETURN n
```

### Match by Label

```cypher
MATCH (n:Person) RETURN n
```

### Match by Property

```cypher
MATCH (n:Person {name: 'Alice'}) RETURN n
```

### Match Relationships

```cypher
MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b
```

### Match Any Relationship

```cypher
MATCH (a)-[r]->(b) RETURN type(r), a.name, b.name
```

### Match Undirected

```cypher
MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name, b.name
```

## WHERE

Filter results with conditions.

### Property Comparison

```cypher
MATCH (n:Person) WHERE n.age > 25 RETURN n.name
```

### Boolean Operators

```cypher
MATCH (n:Person) WHERE n.age > 25 AND n.active = true RETURN n
MATCH (n:Person) WHERE n.age < 18 OR n.age > 65 RETURN n
MATCH (n:Person) WHERE NOT n.disabled RETURN n
```

### String Predicates

```cypher
MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n
MATCH (n:Person) WHERE n.name ENDS WITH 'son' RETURN n
MATCH (n:Person) WHERE n.name CONTAINS 'lic' RETURN n
```

### Null Checks

```cypher
MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n
MATCH (n:Person) WHERE n.phone IS NULL RETURN n
```

### IN Operator

```cypher
MATCH (n:Person) WHERE n.status IN ['active', 'pending'] RETURN n
```

## RETURN

Specify output columns.

### Return Node

```cypher
MATCH (n:Person) RETURN n
```

### Return Properties

```cypher
MATCH (n:Person) RETURN n.name, n.age
```

### Return with Alias

```cypher
MATCH (n:Person) RETURN n.name AS personName
```

### Return Expressions

```cypher
MATCH (n:Person) RETURN n.name, n.age + 1 AS nextAge
```

## Aggregation

### COUNT

```cypher
MATCH (n:Person) RETURN count(n)
MATCH (n:Person) RETURN count(*) AS total
MATCH (n:Person) RETURN count(n.email) AS withEmail
```

## Variable-Length Paths

Match paths with multiple hops.

### Fixed Length

```cypher
MATCH (a:Person)-[:KNOWS*2]->(b:Person) RETURN b.name
```

### Range

```cypher
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b.name
```

### Minimum Only

```cypher
MATCH (a:Person)-[:KNOWS*2..]->(b:Person) RETURN b.name
```

### Maximum Only

```cypher
MATCH (a:Person)-[:KNOWS*..5]->(b:Person) RETURN b.name
```

### Any Length (Kleene Plus)

```cypher
MATCH (a:Person)-[:KNOWS]-+(b:Person) RETURN b.name
```

## SHORTEST

Find shortest paths between nodes.

### Single Shortest Path

```cypher
MATCH p = SHORTEST (a:Person {name: 'Alice'})-[:KNOWS]-+(b:Person {name: 'Bob'})
RETURN p
```

### K Shortest Paths

```cypher
MATCH p = SHORTEST 3 (a:Person)-[:KNOWS]-+(b:Person)
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN p
```

### Path Length

```cypher
MATCH p = SHORTEST (a)-[:KNOWS]-+(b)
WHERE a.name = 'Alice' AND b.name = 'Bob'
RETURN length(p) AS hops
```

## SET

Modify properties and labels.

### Set Property

```cypher
MATCH (n:Person {name: 'Alice'}) SET n.age = 31
```

### Set Multiple Properties

```cypher
MATCH (n:Person {name: 'Alice'}) SET n.age = 31, n.active = true
```

### Add Label

```cypher
MATCH (n:Person {name: 'Alice'}) SET n:Employee
```

## DELETE

Remove nodes and relationships.

### Delete Relationship

```cypher
MATCH (a)-[r:KNOWS]->(b) DELETE r
```

### Delete Node

```cypher
MATCH (n:Person {name: 'Bob'}) DELETE n
```

Node must have no relationships. Use DETACH DELETE otherwise.

### Detach Delete

```cypher
MATCH (n:Person {name: 'Alice'}) DETACH DELETE n
```

Deletes the node and all its relationships.

## Functions

### Type Functions

| Function | Description | Example |
|----------|-------------|---------|
| `type(r)` | Edge type | `RETURN type(r)` |
| `labels(n)` | Node labels | `RETURN labels(n)` |
| `id(n)` | Internal ID | `RETURN id(n)` |

### Aggregation Functions

| Function | Description |
|----------|-------------|
| `count(x)` | Count items |
| `count(*)` | Count rows |

### Path Functions

| Function | Description |
|----------|-------------|
| `length(p)` | Path length (number of edges) |

## Ordering and Pagination

### ORDER BY

```cypher
MATCH (n:Person) RETURN n.name ORDER BY n.name
MATCH (n:Person) RETURN n.name ORDER BY n.age DESC
```

### LIMIT

```cypher
MATCH (n:Person) RETURN n.name LIMIT 10
```

### SKIP

```cypher
MATCH (n:Person) RETURN n.name SKIP 5 LIMIT 10
```

## DISTINCT

Remove duplicate rows.

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT b.name
```

## Data Types

### Property Types

| Type | Example |
|------|---------|
| Integer | `42`, `-7` |
| Float | `3.14`, `-0.5` |
| Boolean | `true`, `false` |
| String | `'hello'`, `"world"` |
| Null | `null` |
| List | `[1, 2, 3]`, `['a', 'b']` |
| Map | `{key: 'value', num: 42}` |

### Comparison Operators

| Operator | Description |
|----------|-------------|
| `=` | Equal |
| `<>` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |

## Limitations

CrustDB does not currently support:

- `OPTIONAL MATCH`
- `WITH` clause
- `MERGE` (parser only, executor not implemented)
- `CALL` procedures
- `UNION`
- Subqueries
- List comprehensions
- `CASE` expressions
- Most mathematical functions
