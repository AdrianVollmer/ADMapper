# Running Queries

ADMapper supports Cypher queries for exploring the AD graph.

## Query Editor

Open the query editor from the toolbar or by clicking on the "Play"
icon. Enter your Cypher query and click Run or press `Ctrl+Enter`.

The query editor also shows the last query run by clicking on one of the
built-in queries.

## Common Queries

### Find Specific Objects

``` cypher
MATCH (u:User)
WHERE u.name CONTAINS 'ADMIN'
RETURN u
```

``` cypher
MATCH (c:Computer)
WHERE c.operatingsystem CONTAINS 'Server'
RETURN c
```

### Group Membership

``` cypher
MATCH (u:User)-[:MemberOf]->(g:Group)
WHERE g.name CONTAINS 'DOMAIN ADMINS'
RETURN u
```

``` cypher
MATCH p = shortestPath((u:User)-[:MemberOf*1..3]->(g:Group))
WHERE g.name = 'DOMAIN ADMINS@CORP.LOCAL'
RETURN p
```

### Sessions

``` cypher
MATCH p = (u:User)-[:HasSession]->(c:Computer)
RETURN p
LIMIT 20
```

### Dangerous Permissions

``` cypher
MATCH p=(n)-[r:GenericAll]->(m)
RETURN p
```

``` cypher
MATCH p = (n)-[r:AdminTo]->(c:Computer)
RETURN p
```

### Shortest Path

Find the shortest path between two nodes:

``` cypher
MATCH p = shortestPath((src:User)-[:MemberOf|AdminTo|HasSession*1..]->(dst:Group))
WHERE src.name = 'JSMITH@CORP.LOCAL'
  AND dst.name = 'DOMAIN ADMINS@CORP.LOCAL'
RETURN p
```

### Kerberoastable Users

``` cypher
MATCH (u:User)
WHERE u.hasspn = true AND u.enabled = true
RETURN u
```

### AS-REP Roastable

``` cypher
MATCH (u:User)
WHERE u.dontreqpreauth = true AND u.enabled = true
RETURN u
```

### Unconstrained Delegation

``` cypher
MATCH (c:Computer)
WHERE c.unconstraineddelegation = true
RETURN c
```

### DCSync Rights

``` cypher
MATCH p = (n)-[:DCSync|GetChanges|GetChangesAll]->(d:Domain)
RETURN p
```

## Cypher Best Practices

If you are new to Cypher or BloodHound-style graph analysis, read this
section before writing queries against large environments. AD graphs can
contain millions of relationships, and a careless query will overwhelm
both the database and the graph visualization.

### Always prefer shortest paths

When looking for attack paths, use `shortestPath` (or the newer GQL
`SHORTEST` syntax) rather than matching all possible paths. Seeing every
path between two nodes is rarely useful -- there can be thousands of
them, and visual inspection becomes meaningless.

``` cypher
-- Good: returns one shortest path
MATCH p = shortestPath(
  (src:User {name: 'JSMITH@CORP.LOCAL'})
  -[:MemberOf|AdminTo|HasSession*1..]->
  (dst:Group {name: 'DOMAIN ADMINS@CORP.LOCAL'})
)
RETURN p
```

``` cypher
-- Bad: returns every path, potentially millions
MATCH p = (src:User {name: 'JSMITH@CORP.LOCAL'})
  -[:MemberOf|AdminTo|HasSession*1..]->
  (dst:Group {name: 'DOMAIN ADMINS@CORP.LOCAL'})
RETURN p
```

### Always use LIMIT

Any query that could return a large number of rows should include a
`LIMIT` clause. Without it, the database may return tens of thousands of
results, which slows down rendering and makes the output impossible to
work with. A limit of 1000 is a reasonable default; go lower if you plan
to visually inspect the graph.

``` cypher
MATCH (u:User)
WHERE u.owned = true
RETURN u
LIMIT 1000
```

### Reduce graph noise by removing stale objects

If queries are still slow or returning too many results despite
`shortestPath` and `LIMIT`, the graph likely contains a large number of
stale or disabled objects that inflate path counts without providing
actionable insights.

Consider these approaches:

1.  **Delete disabled objects.** Disabled accounts and computers rarely
    participate in real attack paths but add significant noise. You can
    remove them via the ADMapper GUI (Edit \> Clear Disabled Objects),
    or manually with a Cypher query:

    ``` cypher
    MATCH (n) WHERE n.enabled = false DETACH DELETE n
    ```

2.  **Exclude stale objects in queries.** If you do not want to modify
    the database, filter them out at query time:

    ``` cypher
    MATCH p = shortestPath(
      (src:User)-[:MemberOf|AdminTo*1..]->(dst:Group)
    )
    WHERE src.enabled = true
      AND dst.name = 'Domain Admins@CORP.LOCAL'
    RETURN p
    LIMIT 1000
    ```

3.  **Exclude objects that have not authenticated recently.** Objects
    with an old `lastlogontimestamp` are likely stale and rarely
    relevant to active attack paths:

    ``` cypher
    MATCH (u:User)
    WHERE u.enabled = true
      AND u.lastlogontimestamp > (datetime().epochSeconds - 90 * 86400)
    RETURN u.name
    LIMIT 1000
    ```

!!! tip Cleaning up stale objects is not just a performance optimization
-- it also makes your security analysis more accurate. Paths through
disabled accounts are not exploitable and only distract from real
findings.

## Query Results

Click Tools \> List view to export results as CSV.

## Query History

Recent queries are saved and can be re-run from the history panel.
