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
MATCH p = (u:User)-[:MemberOf*1..3]->(g:Group)
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

## Cypher Best Practices

If you are new to Cypher or BloodHound-style graph analysis, read this section
before writing queries against large environments. AD graphs can contain
millions of relationships, and a careless query will overwhelm both the database
and the graph visualization.

### Always prefer shortest paths

When looking for attack paths, use `shortestPath` (or the newer GQL `SHORTEST`
syntax) rather than matching all possible paths. Seeing every path between two
nodes is rarely useful -- there can be thousands of them, and visual inspection
becomes meaningless.

```cypher
-- Good: returns one shortest path
MATCH p = shortestPath(
  (src:User {name: 'JSMITH@CORP.LOCAL'})
  -[:MemberOf|AdminTo|HasSession*1..]->
  (dst:Group {name: 'Domain Admins@CORP.LOCAL'})
)
RETURN p
```

```cypher
-- Bad: returns every path, potentially millions
MATCH p = (src:User {name: 'JSMITH@CORP.LOCAL'})
  -[:MemberOf|AdminTo|HasSession*1..]->
  (dst:Group {name: 'Domain Admins@CORP.LOCAL'})
RETURN p
```

### Always use LIMIT

Any query that could return a large number of rows should include a `LIMIT`
clause. Without it, the database may return tens of thousands of results, which
slows down rendering and makes the output impossible to work with. A limit of
1000 is a reasonable default; go lower if you plan to visually inspect the
graph.

```cypher
MATCH (u:User)
WHERE u.owned = true
RETURN u.name, u.displayname
LIMIT 1000
```

### Reduce graph noise by removing stale objects

If queries are still slow or returning too many results despite `shortestPath`
and `LIMIT`, the graph likely contains a large number of stale or disabled
objects that inflate path counts without providing actionable insights.

Consider these approaches:

1. **Delete disabled objects.** Disabled accounts and computers rarely
   participate in real attack paths but add significant noise. You can remove
   them via the ADMapper GUI (Settings > Database > Remove Disabled Objects),
   or manually with a Cypher query:

    ```cypher
    MATCH (n) WHERE n.enabled = false DETACH DELETE n
    ```

2. **Exclude stale objects in queries.** If you do not want to modify the
   database, filter them out at query time:

    ```cypher
    MATCH p = shortestPath(
      (src:User)-[:MemberOf|AdminTo*1..]->(dst:Group)
    )
    WHERE src.enabled = true
      AND dst.name = 'Domain Admins@CORP.LOCAL'
    RETURN p
    LIMIT 1000
    ```

3. **Exclude objects that have not authenticated recently.** Objects with an
   old `lastlogontimestamp` are likely stale and rarely relevant to active
   attack paths:

    ```cypher
    MATCH (u:User)
    WHERE u.enabled = true
      AND u.lastlogontimestamp > (datetime().epochSeconds - 90 * 86400)
    RETURN u.name
    LIMIT 1000
    ```

!!! tip
    Cleaning up stale objects is not just a performance optimization -- it also
    makes your security analysis more accurate. Paths through disabled accounts
    are not exploitable and only distract from real findings.

## Query Results

Results appear in a table below the editor:

- Click a row to select the corresponding node/relationship in the graph
- Export results as CSV

## Query History

Recent queries are saved and can be re-run from the history panel.
