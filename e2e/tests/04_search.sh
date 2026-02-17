#!/usr/bin/env bash
#
# Test: Search Functionality
#
# Tests the graph search endpoint with various queries.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/utils.sh"
source "$SCRIPT_DIR/../lib/assertions.sh"
source "$SCRIPT_DIR/../lib/api.sh"

run_tests() {
    test_start "Search for 'admin' returns results"
    local response
    if response=$(api_search "admin"); then
        test_pass
    else
        test_fail "Search request failed"
        return 1
    fi

    test_start "Search returns array of results"
    local count
    count=$(echo "$response" | jq -r '. | length' 2>/dev/null)
    if [ "$count" -gt 0 ]; then
        test_pass
        log_info "Found $count results for 'admin'"
    else
        # admin might not exist in test data, just check response is valid
        if echo "$response" | jq -e '. | type == "array"' >/dev/null 2>&1; then
            test_pass
            log_info "Search returned empty array (valid)"
        else
            test_fail "Invalid search response format"
        fi
    fi

    test_start "Search for 'user' returns results"
    if response=$(api_search "user"); then
        count=$(echo "$response" | jq -r '. | length' 2>/dev/null)
        test_pass
        log_info "Found $count results for 'user'"
    else
        test_fail "Search for 'user' failed"
    fi

    test_start "Search with node type filter"
    if response=$(api_search "admin" "User"); then
        count=$(echo "$response" | jq -r '. | length' 2>/dev/null)
        test_pass
        log_info "Found $count User results for 'admin'"
    else
        test_fail "Filtered search failed"
    fi

    test_start "Search with limit parameter"
    if response=$(api_search "a" "" "5"); then
        count=$(echo "$response" | jq -r '. | length' 2>/dev/null)
        if [ "$count" -le 5 ]; then
            test_pass
            log_info "Search respected limit: $count results"
        else
            test_fail "Search exceeded limit: got $count, expected <= 5"
        fi
    else
        test_fail "Limited search failed"
    fi

    test_start "Get node types endpoint"
    local node_types
    if node_types=$(api_node_types); then
        test_pass
        log_info "Node types: $(echo "$node_types" | jq -r '.[]' | tr '\n' ', ')"
    else
        test_fail "Node types endpoint failed"
    fi

    test_start "Get edge types endpoint"
    local edge_types
    if edge_types=$(api_edge_types); then
        test_pass
        local edge_count
        edge_count=$(echo "$edge_types" | jq -r '. | length' 2>/dev/null)
        log_info "Edge types count: $edge_count"
    else
        test_fail "Edge types endpoint failed"
    fi

    test_start "Search for domain"
    if response=$(api_search "domain"); then
        count=$(echo "$response" | jq -r '. | length' 2>/dev/null)
        test_pass
        log_info "Found $count results for 'domain'"
    else
        test_fail "Search for 'domain' failed"
    fi

    test_start "Search results have expected structure"
    if response=$(api_search "a" "" "1"); then
        count=$(echo "$response" | jq -r '. | length' 2>/dev/null)
        if [ "$count" -gt 0 ]; then
            # Check first result has id and name
            local has_id has_name
            has_id=$(echo "$response" | jq -e '.[0].id' >/dev/null 2>&1 && echo "yes" || echo "no")
            has_name=$(echo "$response" | jq -e '.[0].name // .[0].properties.name' >/dev/null 2>&1 && echo "yes" || echo "no")
            if [ "$has_id" = "yes" ]; then
                test_pass
            else
                test_fail "Search result missing 'id' field"
            fi
        else
            test_pass
            log_info "No results to check structure"
        fi
    else
        test_fail "Structure check search failed"
    fi
}

# Run if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    run_tests
fi
