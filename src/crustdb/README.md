# CrustDB

An embedded graph database with SQLite backend and Cypher support, written in Rust.

## Features

- **Embedded**: No separate server process, links directly into your application
- **SQLite Backend**: Battle-tested storage with ACID transactions
- **Cypher Support**: Industry-standard graph query language
- **Property Graph Model**: Nodes and edges with labels, types, and arbitrary properties

## SQLite Schema

### Table `meta`

Schema versioning for migrations.

| Column | Type | Description |
|--------|------|-------------|
| key | TEXT PRIMARY KEY | Metadata key |
| value | TEXT | Metadata value |

### Table `node_labels`

Normalized storage for node label strings.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Label ID |
| name | TEXT UNIQUE | Label name (e.g., "Person", "Movie") |

### Table `edge_types`

Normalized storage for edge type strings.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Type ID |
| name | TEXT UNIQUE | Type name (e.g., "KNOWS", "ACTED_IN") |

### Table `nodes`

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Node ID |
| properties | TEXT | JSON-encoded properties |

### Table `node_label_map`

Many-to-many relationship between nodes and labels.

| Column | Type | Description |
|--------|------|-------------|
| node_id | INTEGER | FK -> nodes.id |
| label_id | INTEGER | FK -> node_labels.id |

### Table `edges`

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Edge ID |
| source_id | INTEGER | FK -> nodes.id |
| target_id | INTEGER | FK -> nodes.id |
| type_id | INTEGER | FK -> edge_types.id |
| properties | TEXT | JSON-encoded properties |

### Indexes

- `idx_node_label_map_label` on node_label_map(label_id) - Fast label lookups
- `idx_node_label_map_node` on node_label_map(node_id) - Fast node label retrieval
- `idx_edges_source` on edges(source_id) - Fast outgoing edge traversal
- `idx_edges_target` on edges(target_id) - Fast incoming edge traversal
- `idx_edges_type` on edges(type_id) - Fast edge type filtering

## Milestones

### M1: Storage Layer [done]

Basic CRUD operations for nodes and edges.

- [x] SQLite schema with migrations
- [x] Insert/get/delete nodes
- [x] Insert/get/delete edges
- [x] Property serialization (JSON)
- [x] Database statistics

### M2: Simple CREATE Queries [open]

Parse and execute basic node/edge creation.

```cypher
CREATE (n:Person {name: 'Alice', age: 30})

CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})

CREATE (charlie:Person:Actor {name: 'Charlie Sheen'}),
       (oliver:Person:Director {name: 'Oliver Stone'}),
       (wallStreet:Movie {title: 'Wall Street'}),
       (charlie)-[:ACTED_IN {role: 'Bud Fox'}]->(wallStreet),
       (oliver)-[:DIRECTED]->(wallStreet)
```

- [ ] Cypher lexer/tokenizer
- [ ] CREATE clause parser
- [ ] Node pattern parser (labels, properties)
- [ ] Relationship pattern parser
- [ ] CREATE executor

### M3: Simple MATCH Queries [open]

Parse and execute basic read queries.

```cypher
MATCH (n) RETURN n

MATCH (n:Person) RETURN n

MATCH (n:Person) RETURN n.name, n.age

MATCH (n:Person {name: 'Alice'}) RETURN n
```

- [ ] MATCH clause parser
- [ ] RETURN clause parser
- [ ] Node scan operator
- [ ] Label filter operator
- [ ] Property filter operator
- [ ] Projection operator

### M4: WHERE Clause [open]

Add filtering with WHERE conditions.

```cypher
MATCH (n:Person) WHERE n.age > 30 RETURN n

MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n

MATCH (n:Person) WHERE n.age >= 18 AND n.age <= 65 RETURN n
```

- [ ] WHERE clause parser
- [ ] Comparison operators (=, <>, <, <=, >, >=)
- [ ] Logical operators (AND, OR, NOT)
- [ ] String predicates (STARTS WITH, ENDS WITH, CONTAINS)
- [ ] NULL checks (IS NULL, IS NOT NULL)

### M5: Single-Hop Traversal [open]

Navigate one relationship step.

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name

MATCH (a)-[r]->(b) RETURN a, r, b

MATCH (m:Movie)<-[:ACTED_IN]-(a:Actor) RETURN m.title, a.name
```

- [ ] Relationship pattern parser (direction, type, variable)
- [ ] Expand operator (outgoing)
- [ ] Expand operator (incoming)
- [ ] Expand operator (undirected)

### M6: Multi-Hop Traversal [open]

Variable-length path queries.

```cypher
MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b

MATCH p = (a:Person)-[:KNOWS*]->(b:Person) RETURN p

MATCH (a)-[*]-(b) WHERE a <> b RETURN DISTINCT a, b
```

- [ ] Variable-length pattern parser
- [ ] BFS/DFS traversal operator
- [ ] Path construction
- [ ] Cycle detection

### M7: Mutation Queries [open]

UPDATE and DELETE operations.

```cypher
MATCH (n:Person {name: 'Alice'}) SET n.age = 31

MATCH (n:Person {name: 'Alice'}) SET n:Employee

MATCH (n:Person {name: 'Bob'}) DELETE n

MATCH (n:Person {name: 'Charlie'}) DETACH DELETE n
```

- [ ] SET clause parser
- [ ] DELETE clause parser
- [ ] Update operator
- [ ] Delete operator (with DETACH)

### M8: Aggregation [open]

GROUP BY and aggregate functions.

```cypher
MATCH (n:Person) RETURN count(n)

MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, count(f) as friends

MATCH (n:Person) RETURN avg(n.age), min(n.age), max(n.age)
```

- [ ] Aggregate function parser (count, sum, avg, min, max, collect)
- [ ] GROUP BY detection
- [ ] Aggregation operator

### M9: Query Optimization [open]

Basic query planning and optimization.

- [ ] Cost-based join ordering
- [ ] Index selection
- [ ] Predicate pushdown
- [ ] Projection pushdown

### M10: Advanced Features [open]

- [ ] MERGE (upsert)
- [ ] OPTIONAL MATCH
- [ ] UNION
- [ ] WITH clause (query chaining)
- [ ] ORDER BY, SKIP, LIMIT
- [ ] DISTINCT
- [ ] CASE expressions
- [ ] List comprehensions

## Usage

```rust
use crustdb::Database;

// Open or create database
let db = Database::open("my_graph.db")?;

// Create nodes and relationships
db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")?;

// Query the graph
let results = db.execute("MATCH (n:Person) RETURN n.name")?;
for row in results.rows {
    println!("{:?}", row);
}
```

## License

MIT
