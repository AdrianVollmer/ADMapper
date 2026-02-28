# Database Backends

ADMapper supports multiple graph database backends. Choose based on your deployment needs.

## CrustDB (Default)

An embedded graph database built into ADMapper.

**URL format:**
```
crustdb:///path/to/database.db
```

**Advantages:**

- No external dependencies
- Single-file storage
- Fast for typical AD datasets
- Included by default

**Use when:**

- Running ADMapper as a standalone tool
- Analyzing single environments
- Portability is important

## Neo4j

Enterprise graph database with network protocol.

**URL format:**
```
neo4j://user:password@host:7687
```

**Advantages:**

- Mature, battle-tested
- Rich Cypher support
- Clustering and high availability
- Browser-based console

**Use when:**

- Integrating with existing Neo4j infrastructure
- Need advanced Cypher features
- Multiple tools access the same data

**Setup:**

```bash
# Docker
docker run -d \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  neo4j:5
```

## FalkorDB

Redis-based graph database with Cypher support.

**URL format:**
```
falkordb://host:6379
```

**Advantages:**

- Redis ecosystem integration
- In-memory performance
- Simple deployment

**Use when:**

- Already using Redis
- Need fastest query performance
- Data fits in memory

**Setup:**

```bash
# Docker
docker run -d -p 6379:6379 falkordb/falkordb:latest
```

## Backend Comparison

| Feature | CrustDB | Neo4j | FalkorDB |
|---------|---------|-------|----------|
| Deployment | Embedded | Server | Server |
| Storage | SQLite file | Disk | Memory/Disk |
| Cypher Support | Partial | Full | Partial |
| Dependencies | None | Java | Redis |
| Clustering | No | Yes | Yes |

## Configuration

### Command Line

```bash
# CrustDB (default)
admapper crustdb:///data/graph.db

# Neo4j
admapper neo4j://neo4j:password@localhost:7687

# FalkorDB
admapper falkordb://localhost:6379
```

### Environment Variables

```bash
# Neo4j
export NEO4J_HOST=localhost
export NEO4J_PORT=7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=password

# FalkorDB
export FALKORDB_HOST=localhost
export FALKORDB_PORT=6379
```

## Switching Backends

Data is not automatically migrated between backends. To switch:

1. Export data from the current backend (or keep original BloodHound files)
2. Start ADMapper with the new backend URL
3. Re-import the BloodHound data
