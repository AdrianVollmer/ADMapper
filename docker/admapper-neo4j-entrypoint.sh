#!/bin/bash
set -euo pipefail

NEO4J_PID=

cleanup() {
    if [ -n "$NEO4J_PID" ]; then
        echo "Stopping Neo4j..."
        kill "$NEO4J_PID" 2>/dev/null || true
        wait "$NEO4J_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

echo "Starting Neo4j..."
/startup/docker-entrypoint.sh neo4j &
NEO4J_PID=$!

echo "Waiting for Neo4j to be ready..."
max_wait=120
waited=0
until wget -qO- http://localhost:7474 >/dev/null 2>&1; do
    if ! kill -0 "$NEO4J_PID" 2>/dev/null; then
        echo "Neo4j process died unexpectedly" >&2
        exit 1
    fi
    if [ "$waited" -ge "$max_wait" ]; then
        echo "Timed out waiting for Neo4j after ${max_wait}s" >&2
        exit 1
    fi
    sleep 2
    waited=$((waited + 2))
done

echo "Neo4j is ready, starting admapper..."
exec admapper --headless \
    --bind "${ADMAPPER_HOST:-0.0.0.0}" \
    --port "${ADMAPPER_PORT:-9191}" \
    neo4j://localhost
