# Development Guide

This document covers the essential development tasks for ADMapper.

## Prerequisites

- **Rust** (stable toolchain)
- **Node.js** (for frontend development)
- **pnpm** (package manager for frontend)
- **Docker** (for e2e tests)

## Project Structure

```
src/
├── backend/     # Rust backend (Axum server)
└── frontend/    # React frontend (Vite + TypeScript)
scripts/         # Build, test, and utility scripts
e2e/             # End-to-end testing infrastructure
```

## Building

Use `scripts/build.sh` to build the project:

```bash
# Build everything (frontend + backend)
./scripts/build.sh all

# Build only the backend (release mode)
./scripts/build.sh backend

# Build backend in debug mode
./scripts/build.sh backend-debug

# Build only the frontend
./scripts/build.sh frontend

# Build Tauri desktop app (release)
./scripts/build.sh tauri

# Build Tauri desktop app (debug)
./scripts/build.sh tauri-debug

# Clean build artifacts
./scripts/build.sh clean
```

## Testing

### Unit and Integration Tests

Use `scripts/test.sh` to run tests:

```bash
# Run all tests (frontend + backend)
./scripts/test.sh all

# Run backend tests only
./scripts/test.sh backend

# Run frontend tests only
./scripts/test.sh frontend

# Run backend tests with coverage
./scripts/test.sh coverage
```

### End-to-End Tests

E2E tests verify the full import and query workflow against real database backends.

#### Embedded Backends (crustdb)

For testing embedded databases, use the standalone Docker image:

```bash
# Build the e2e Docker image
docker build -t admapper-e2e -f e2e/Dockerfile .

# Test all embedded backends
docker run --rm -v $(pwd):/workspace admapper-e2e \
    ./e2e/run-tests.sh /path/to/test_data.zip

# Test a specific backend
docker run --rm -v $(pwd):/workspace admapper-e2e \
    ./e2e/run-tests.sh /path/to/test_data.zip crustdb
```

#### Network Backends (Neo4j, FalkorDB)

For testing network databases, use docker-compose which provides official
database images:

```bash
# Start databases and run tests in one command
docker compose -f e2e/docker-compose.yml run --rm tests \
    ./e2e/run-tests.sh /path/to/test_data.zip

# Or manage databases separately
docker compose -f e2e/docker-compose.yml up -d neo4j falkordb
docker compose -f e2e/docker-compose.yml run --rm tests \
    ./e2e/run-tests.sh /path/to/test_data.zip
docker compose -f e2e/docker-compose.yml down
```

The docker-compose setup provides:
- **Neo4j**: `neo4j:5.18.1-community` on ports 7474 (HTTP) and 7687 (Bolt)
- **FalkorDB**: `falkordb/falkordb:v4.2.1` on port 6379

Environment variables are automatically configured for the test runner to
connect to these services.

#### Test Data

The first argument to `run-tests.sh` is the path to a BloodHound data zip
file. The optional second argument specifies which backend to test (`crustdb`,
`neo4j`, `falkordb`, or `all`).

#### Test Reports

Reports are generated in `e2e/reports/` as XML files with CSS styling for
browser viewing.

## Code Quality

### Checking

Use `scripts/check.sh` to run all linters and type checks:

```bash
./scripts/check.sh
```

This runs:
- **Frontend**: ESLint, TypeScript type checking, Prettier format check
- **Backend**: Cargo clippy (lints), cargo fmt check

### Formatting

Use `scripts/format.sh` to auto-format code:

```bash
./scripts/format.sh
```

This runs:
- **Frontend**: Prettier
- **Backend**: cargo fmt

## Database Backends

ADMapper supports multiple graph database backends via Cargo features:

| Backend   | Feature    | Description                          |
|-----------|------------|--------------------------------------|
| CrustDB   | `crustdb`  | Embedded, SQLite-based (default)     |
| Neo4j     | `neo4j`    | Network, Bolt protocol               |
| FalkorDB  | `falkordb` | Network, Redis-based                 |

Build with specific features:

```bash
cargo build --release -p admapper --features crustdb,neo4j,falkordb
```

## Running the Server

```bash
# Run with default backend (crustdb)
./target/release/admapper

# Run headless (no browser)
./target/release/admapper --headless

# Specify database URL
./target/release/admapper crustdb:///path/to/file.db
```

Database URL formats:
- `crustdb:///path/to/file.db`
- `neo4j://user:pass@host:7687`
- `falkordb://host:6379`
