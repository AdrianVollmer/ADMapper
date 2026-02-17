#!/usr/bin/env bash
#
# Utility functions for E2E tests
#

# Guard against multiple inclusion
[ -n "$_E2E_UTILS_LOADED" ] && return 0
_E2E_UTILS_LOADED=1

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    if [ -n "$DEBUG" ]; then
        echo -e "${CYAN}[DEBUG]${NC} $1"
    fi
}

log_test() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
}

# Timer functions
timer_start() {
    TIMER_START=$(date +%s.%N)
}

timer_elapsed() {
    local end=$(date +%s.%N)
    echo "$(echo "$end - $TIMER_START" | bc)s"
}

# Wait for a condition with timeout
# Usage: wait_for "description" timeout_seconds check_command
wait_for() {
    local desc="$1"
    local timeout="$2"
    shift 2
    local cmd="$*"

    local elapsed=0
    log_debug "Waiting for $desc (timeout: ${timeout}s)..."

    while [ $elapsed -lt $timeout ]; do
        if eval "$cmd" >/dev/null 2>&1; then
            log_debug "$desc ready after ${elapsed}s"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    log_error "Timeout waiting for $desc after ${timeout}s"
    return 1
}

# Generate a unique test ID
generate_test_id() {
    echo "e2e-$(date +%s)-$$"
}

# Clean up temporary directories
cleanup_temp() {
    local dir="$1"
    if [ -d "$dir" ] && [[ "$dir" == /tmp/* ]]; then
        rm -rf "$dir"
    fi
}
