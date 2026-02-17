#!/usr/bin/env bash
#
# API helper functions for E2E tests
#

# Guard against multiple inclusion
[ -n "$_E2E_API_LOADED" ] && return 0
_E2E_API_LOADED=1

# Source utilities if not already loaded
_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_LIB_DIR/utils.sh"

# Default server configuration
API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-9191}"
API_BASE="http://${API_HOST}:${API_PORT}"

# Make an API request
# Usage: api_request "METHOD" "endpoint" ["data"]
# Returns: response body (status code is available via $?)
api_request() {
    local method="$1"
    local endpoint="$2"
    local data="$3"

    local url="${API_BASE}${endpoint}"
    local curl_args=(-s -w '\n%{http_code}' -X "$method")

    if [ -n "$data" ]; then
        curl_args+=(-H "Content-Type: application/json" -d "$data")
    fi

    local response
    response=$(curl "${curl_args[@]}" "$url" 2>/dev/null)
    local curl_exit=$?

    if [ $curl_exit -ne 0 ]; then
        echo ""
        return $curl_exit
    fi

    # Extract body and status code
    local body status_code
    body=$(echo "$response" | sed '$d')
    status_code=$(echo "$response" | tail -n1)

    echo "$body"

    # Return success only for 2xx status codes
    case "$status_code" in
        2*) return 0 ;;
        *) return 1 ;;
    esac
}

# Health check endpoint
api_health() {
    api_request "GET" "/api/health"
}

# Database status
api_db_status() {
    api_request "GET" "/api/database/status"
}

# Connect to database
# Usage: api_connect "database_url"
api_connect() {
    local db_url="$1"
    api_request "POST" "/api/database/connect" "{\"url\": \"$db_url\"}"
}

# Disconnect from database
api_disconnect() {
    api_request "POST" "/api/database/disconnect"
}

# Import BloodHound data
# Usage: api_import "zip_path"
api_import() {
    local zip_path="$1"

    # Upload the file
    local response
    response=$(curl -s -w '\n%{http_code}' \
        -X POST \
        -F "file=@${zip_path}" \
        "${API_BASE}/api/import" 2>/dev/null)

    local body status_code
    body=$(echo "$response" | sed '$d')
    status_code=$(echo "$response" | tail -n1)

    echo "$body"

    case "$status_code" in
        2*) return 0 ;;
        *) return 1 ;;
    esac
}

# Get import progress
# Usage: api_import_progress "job_id"
api_import_progress() {
    local job_id="$1"
    api_request "GET" "/api/import/progress/$job_id"
}

# Get graph statistics
api_stats() {
    api_request "GET" "/api/graph/stats"
}

# Get detailed graph statistics
api_detailed_stats() {
    api_request "GET" "/api/graph/detailed-stats"
}

# Clear the graph
api_clear() {
    api_request "POST" "/api/graph/clear"
}

# Execute a Cypher query
# Usage: api_query "cypher_query"
api_query() {
    local query="$1"
    local escaped_query
    escaped_query=$(echo "$query" | jq -Rs '.')
    api_request "POST" "/api/graph/query" "{\"query\": $escaped_query}"
}

# Search the graph
# Usage: api_search "search_term" ["node_type"] ["limit"]
api_search() {
    local term="$1"
    local node_type="${2:-}"
    local limit="${3:-10}"

    local query="term=$(jq -rn --arg t "$term" '$t | @uri')"
    [ -n "$node_type" ] && query="${query}&node_type=$node_type"
    query="${query}&limit=$limit"

    api_request "GET" "/api/graph/search?$query"
}

# Get node types
api_node_types() {
    api_request "GET" "/api/graph/node-types"
}

# Get edge types
api_edge_types() {
    api_request "GET" "/api/graph/edge-types"
}

# Wait for server to be ready
# Usage: wait_for_server [timeout_seconds]
wait_for_server() {
    local timeout="${1:-30}"
    wait_for "server health check" "$timeout" "api_health"
}

# Wait for import to complete
# Usage: wait_for_import "job_id" [timeout_seconds]
wait_for_import() {
    local job_id="$1"
    local timeout="${2:-120}"
    local elapsed=0

    log_debug "Waiting for import job $job_id to complete..."

    while [ $elapsed -lt $timeout ]; do
        local progress
        progress=$(api_import_progress "$job_id")

        local status
        status=$(echo "$progress" | jq -r '.status // empty' 2>/dev/null)

        case "$status" in
            "completed")
                log_debug "Import completed after ${elapsed}s"
                echo "$progress"
                return 0
                ;;
            "failed")
                log_error "Import failed"
                echo "$progress"
                return 1
                ;;
            "processing"|"pending")
                log_debug "Import in progress..."
                ;;
        esac

        sleep 2
        elapsed=$((elapsed + 2))
    done

    log_error "Timeout waiting for import after ${timeout}s"
    return 1
}

# Start the ADMapper server
# Usage: start_server "backend_type" "db_path" [port]
start_server() {
    local backend="$1"
    local db_path="$2"
    local port="${3:-9191}"

    local db_url
    case "$backend" in
        kuzu)
            db_url="kuzu://${db_path}"
            ;;
        crustdb)
            db_url="crustdb://${db_path}"
            ;;
        *)
            log_error "Unknown backend: $backend"
            return 1
            ;;
    esac

    log_info "Starting ADMapper with $backend backend on port $port..."

    # Start server in background
    ADMAPPER_BIN="${ADMAPPER_BIN:-/workspace/target/release/admapper}"
    "$ADMAPPER_BIN" --headless --port "$port" --bind "0.0.0.0" "$db_url" &
    SERVER_PID=$!

    # Update API port
    API_PORT="$port"
    API_BASE="http://${API_HOST}:${API_PORT}"

    # Wait for server to start
    if wait_for_server 30; then
        log_info "Server started (PID: $SERVER_PID)"
        return 0
    else
        log_error "Failed to start server"
        kill $SERVER_PID 2>/dev/null
        return 1
    fi
}

# Stop the ADMapper server
stop_server() {
    if [ -n "$SERVER_PID" ]; then
        log_info "Stopping server (PID: $SERVER_PID)..."
        kill $SERVER_PID 2>/dev/null
        wait $SERVER_PID 2>/dev/null
        SERVER_PID=""
    fi
}
