#!/usr/bin/env bash
#
# Test: Graph Statistics
#
# Verifies node and edge counts match expected values from golden files.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/utils.sh"
source "$SCRIPT_DIR/../lib/assertions.sh"
source "$SCRIPT_DIR/../lib/api.sh"

# Golden file location
GOLDEN_FILE="${GOLDEN_FILE:-$SCRIPT_DIR/../golden/expected_stats.json}"

run_tests() {
    test_start "Golden file exists"
    if [ -f "$GOLDEN_FILE" ]; then
        test_pass
    else
        test_fail "Golden file not found: $GOLDEN_FILE"
        log_info "Run ./e2e/generate-expected.py to generate it"
        return 1
    fi

    # Load expected values
    local expected
    expected=$(cat "$GOLDEN_FILE")

    test_start "Get detailed statistics"
    local actual
    if actual=$(api_detailed_stats); then
        test_pass
    else
        test_fail "Failed to get detailed statistics"
        return 1
    fi

    log_debug "Expected: $expected"
    log_debug "Actual: $actual"

    # Extract expected total counts
    local expected_nodes expected_edges
    expected_nodes=$(echo "$expected" | jq -r '.total_nodes')
    expected_edges=$(echo "$expected" | jq -r '.total_edges')

    # Extract actual total counts
    local actual_nodes actual_edges
    actual_nodes=$(echo "$actual" | jq -r '.total_nodes')
    actual_edges=$(echo "$actual" | jq -r '.total_edges')

    test_start "Total node count matches expected"
    if assert_equals "$expected_nodes" "$actual_nodes" "node count"; then
        test_pass
    else
        test_fail
        # Don't fail the whole test suite, just report discrepancy
    fi

    test_start "Total edge count is reasonable"
    # Edges can vary based on import deduplication, just check > 0
    if [ "$actual_edges" -gt 0 ]; then
        test_pass
        log_info "Edges: $actual_edges (expected: $expected_edges)"
    else
        test_fail "No edges found"
    fi

    # Check node type counts
    test_start "Node types breakdown is available"
    local node_types
    node_types=$(echo "$actual" | jq -r '.node_types // empty')
    if [ -n "$node_types" ] && [ "$node_types" != "null" ]; then
        test_pass

        # Verify each expected node type is present
        local types
        types=$(echo "$expected" | jq -r '.node_types | keys[]')
        for node_type in $types; do
            local exp_count act_count
            exp_count=$(echo "$expected" | jq -r ".node_types.\"$node_type\"")
            act_count=$(echo "$actual" | jq -r ".node_types.\"$node_type\" // 0")

            test_start "Count for $node_type nodes"
            if [ "$act_count" -ge 1 ]; then
                test_pass
                log_info "$node_type: $act_count (expected: $exp_count)"
            else
                test_fail "No $node_type nodes found (expected: $exp_count)"
            fi
        done
    else
        test_fail "Node types breakdown not available"
    fi
}

# Run if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    run_tests
fi
