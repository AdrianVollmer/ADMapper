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

### Running with Docker

Keep a separate data directory per project so you can switch between
environments by swapping the mount.

**Neo4j:**

```bash
# Create data directories per project
mkdir -p ~/admapper/neo4j/{corp,staging}

# Run Neo4j with a specific project directory
docker run -d --name admapper-neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  -v ~/admapper/neo4j/corp:/data \
  neo4j:5

# To switch projects, stop the container and start a new one
# pointing at a different directory
docker stop admapper-neo4j && docker rm admapper-neo4j
docker run -d --name admapper-neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  -v ~/admapper/neo4j/staging:/data \
  neo4j:5
```

**FalkorDB:**

```bash
mkdir -p ~/admapper/falkor/corp

docker run -d --name admapper-falkor \
  -p 6379:6379 \
  -v ~/admapper/falkor/corp:/data \
  falkordb/falkordb:latest
```

Replace `docker` with `podman` if you prefer a rootless setup.

## Important: Neo4j Compatibility

Do not use ADMapper with a Neo4j database that was populated by the original
BloodHound. ADMapper uses its own schema and import logic; connecting to a
BloodHound-managed Neo4j instance will produce incorrect results. Always start
from a fresh database and import your collector data through ADMapper.
