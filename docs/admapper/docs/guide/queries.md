# Running Queries

ADMapper supports Cypher queries for exploring the AD graph.

## Query Editor

Open the query editor from the toolbar. Enter your Cypher query and click Run or press `Ctrl+Enter`.

## Common Queries

### Count Objects

```cypher
MATCH (n) RETURN count(n) AS total
```

```cypher
MATCH (u:User) RETURN count(u) AS users
```

```cypher
MATCH (c:Computer) RETURN count(c) AS computers
```

### Find Specific Objects

```cypher
MATCH (u:User)
WHERE u.name CONTAINS 'admin'
RETURN u.name
```

```cypher
MATCH (c:Computer)
WHERE c.operatingsystem CONTAINS 'Server'
RETURN c.name, c.operatingsystem
```

### Group Membership

```cypher
MATCH (u:User)-[:MemberOf]->(g:Group)
WHERE g.name CONTAINS 'Domain Admins'
RETURN u.name
```

```cypher
MATCH (u:User)-[:MemberOf*1..3]->(g:Group)
WHERE g.name = 'Domain Admins@CORP.LOCAL'
RETURN u.name, length(p) AS hops
```

### Sessions

```cypher
MATCH (u:User)-[:HasSession]->(c:Computer)
RETURN u.name, c.name
LIMIT 20
```

### Dangerous Permissions

```cypher
MATCH (n)-[r:GenericAll]->(m)
RETURN n.name, type(r), m.name
```

```cypher
MATCH (n)-[r:AdminTo]->(c:Computer)
RETURN n.name, c.name
```

### Shortest Path

Find the shortest path between two nodes:

```cypher
MATCH p = SHORTEST (src:User)-[:MemberOf|AdminTo|HasSession*1..]->(dst:Group)
WHERE src.name = 'JSMITH@CORP.LOCAL'
  AND dst.name = 'Domain Admins@CORP.LOCAL'
RETURN p
```

### Kerberoastable Users

```cypher
MATCH (u:User)
WHERE u.hasspn = true AND u.enabled = true
RETURN u.name, u.serviceprincipalnames
```

### AS-REP Roastable

```cypher
MATCH (u:User)
WHERE u.dontreqpreauth = true AND u.enabled = true
RETURN u.name
```

### Unconstrained Delegation

```cypher
MATCH (c:Computer)
WHERE c.unconstraineddelegation = true
RETURN c.name
```

### DCSync Rights

```cypher
MATCH (n)-[:DCSync|GetChanges|GetChangesAll]->(d:Domain)
RETURN n.name, d.name
```

## Query Results

Results appear in a table below the editor:

- Click a row to select the corresponding node/relationship in the graph
- Export results as CSV

## Query History

Recent queries are saved and can be re-run from the history panel.
