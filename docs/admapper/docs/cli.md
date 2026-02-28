# CLI Reference

ADMapper command-line interface options.

## Usage

```
admapper [OPTIONS] [DATABASE_URL]
```

## Arguments

### DATABASE_URL

Database connection URL. Format depends on the backend:

```bash
# CrustDB (embedded SQLite)
crustdb:///path/to/database.db

# Neo4j
neo4j://user:password@host:port

# FalkorDB
falkordb://host:port
```

If not specified, defaults to an in-memory CrustDB database.

## Options

### `--headless`

Run without GUI as a web server.

```bash
admapper --headless crustdb:///data.db
```

### `--port <PORT>`

HTTP server port for headless mode. Default: `9191`.

```bash
admapper --headless --port 8080 crustdb:///data.db
```

### `--bind <ADDRESS>`

Bind address for the HTTP server. Default: `127.0.0.1`.

```bash
# Allow external connections
admapper --headless --bind 0.0.0.0 crustdb:///data.db
```

### `--help`

Show help message.

```bash
admapper --help
```

### `--version`

Show version information.

```bash
admapper --version
```

## Examples

### Desktop Mode

```bash
# Default database
admapper

# Specific database file
admapper crustdb:///home/user/ad_analysis.db
```

### Headless Web Server

```bash
# Local only
admapper --headless --port 9191 crustdb:///data.db

# Network accessible
admapper --headless --bind 0.0.0.0 --port 9191 crustdb:///data.db
```

### External Database

```bash
# Neo4j
admapper --headless neo4j://neo4j:password@localhost:7687

# FalkorDB
admapper --headless falkordb://localhost:6379
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NEO4J_HOST` | Neo4j server hostname | `localhost` |
| `NEO4J_PORT` | Neo4j bolt port | `7687` |
| `NEO4J_USER` | Neo4j username | `neo4j` |
| `NEO4J_PASSWORD` | Neo4j password | - |
| `FALKORDB_HOST` | FalkorDB hostname | `localhost` |
| `FALKORDB_PORT` | FalkorDB port | `6379` |
| `RUST_LOG` | Log level | `info` |

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid arguments |

## Logging

Control log verbosity with `RUST_LOG`:

```bash
# Debug logging
RUST_LOG=debug admapper --headless crustdb:///data.db

# Specific module
RUST_LOG=admapper=debug admapper --headless crustdb:///data.db
```
