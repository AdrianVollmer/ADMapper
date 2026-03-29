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

CrustDB is an experimental embedded graph database which supports
[openCypher](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf).
It uses SQLite under the hood and is written in Rust. It does not aim
at competing with a commercial product like Neo4j in terms of
performance. Especially for larger datasets, it is recommended to use
Neo4j or FalkorDB.

It's easiest to run these in a container using Docker or Podman.

``` bash
docker run --rm -it --init -p 7687:7687 \
    --userns $UID=7474 \
    -e NEO4J_AUTH=none -v ./data:/data \
    neo4j:5
```

Just like CrustDB or most other local data storage, this does not use
authentication. Adjust according to your threat model. Being able to
mount different volumes to `/data` means you can easily switch between
separate projects.

Similarly, for FalkorDB:

``` bash
docker run --rm -it --init -p 6379:6379 \
    -v ./data:/data \
    docker.io/falkordb/falkordb:v4.2.1
```

If you run ADMapper natively, just connect to `localhost` in the
respective tab of the connection dialog.

If you run ADMapper also inside a container, it's best to use a
`docker-compose.yml` file. Example:

``` yaml
services:
  admapper:
    image: ghcr.io/adrianvollmer/admapper
    ports:
      - "9191:9191"
    command: ["neo4j://neo4j"]
    depends_on:
      neo4j:
        condition: service_healthy

  neo4j:
    image: docker.io/neo4j:5
    init: true
    environment:
      NEO4J_AUTH: none
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - ${DATA_DIR}:/data
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:7474"]
      interval: 5s
      timeout: 5s
      retries: 12
```

Then you can easily get going by running
`DATA_DIR=./data docker compose up`.

Replace `docker` with `podman` if you prefer a rootless setup.

## Important: Neo4j Compatibility

Do not use ADMapper with a Neo4j database that was populated by the original
BloodHound. ADMapper uses its own schema and import logic; connecting to a
BloodHound-managed Neo4j instance will produce incorrect results. Always start
from a fresh database and import your collector data through ADMapper.
