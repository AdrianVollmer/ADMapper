#!/usr/bin/env bash
#
# Test: Health Check
#
# Verifies that the server is running and responding to health checks.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/utils.sh"
source "$SCRIPT_DIR/../lib/assertions.sh"
source "$SCRIPT_DIR/../lib/api.sh"

run_tests() {
    test_start "Health check returns 200"
    local response
    if response=$(api_health); then
        test_pass
    else
        test_fail "Health check failed"
        return 1
    fi

    test_start "Health check returns valid JSON"
    if echo "$response" | jq -e . >/dev/null 2>&1; then
        test_pass
    else
        test_fail "Response is not valid JSON: $response"
        return 1
    fi

    test_start "Health check indicates healthy status"
    if assert_json_field "$response" '.status' "ok" "status should be ok"; then
        test_pass
    else
        test_fail
        return 1
    fi

    test_start "Database status endpoint works"
    local db_status
    if db_status=$(api_db_status); then
        test_pass
    else
        test_fail "Database status endpoint failed"
        return 1
    fi

    test_start "Database is connected"
    local connected
    connected=$(echo "$db_status" | jq -r '.connected // false')
    if [ "$connected" = "true" ]; then
        test_pass
    else
        test_fail "Database not connected"
        return 1
    fi
}

# Run if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    run_tests
fi
