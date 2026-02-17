#!/usr/bin/env bash
#
# Test: Import BloodHound Data
#
# Imports test data and verifies the import completes successfully.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/utils.sh"
source "$SCRIPT_DIR/../lib/assertions.sh"
source "$SCRIPT_DIR/../lib/api.sh"

# Test data location
TEST_DATA="${TEST_DATA:-/workspace/_data/ad_example_data.zip}"

run_tests() {
    test_start "Test data file exists"
    if [ -f "$TEST_DATA" ]; then
        test_pass
    else
        test_fail "Test data not found: $TEST_DATA"
        return 1
    fi

    test_start "Import request succeeds"
    local response
    if response=$(api_import "$TEST_DATA"); then
        test_pass
    else
        test_fail "Import request failed: $response"
        return 1
    fi

    test_start "Import returns job ID"
    local job_id
    job_id=$(echo "$response" | jq -r '.job_id // empty')
    if assert_not_empty "$job_id" "job_id should not be empty"; then
        test_pass
        log_info "Import job ID: $job_id"
    else
        test_fail
        return 1
    fi

    test_start "Import completes successfully"
    local progress
    # Neo4j/FalkorDB imports can take longer due to network overhead
    local import_timeout="${IMPORT_TIMEOUT:-300}"
    if progress=$(wait_for_import "$job_id" "$import_timeout"); then
        test_pass
    else
        test_fail "Import did not complete"
        return 1
    fi

    test_start "Import processed files"
    local files_processed
    files_processed=$(echo "$progress" | jq -r '.files_processed // 0')
    if [ "$files_processed" -gt 0 ]; then
        test_pass
        log_info "Files processed: $files_processed"
    else
        test_fail "No files were processed"
        return 1
    fi

    test_start "Graph has nodes after import"
    local stats
    stats=$(api_stats)
    local node_count
    node_count=$(echo "$stats" | jq -r '.nodes // 0')
    if [ "$node_count" -gt 0 ]; then
        test_pass
        log_info "Nodes imported: $node_count"
    else
        test_fail "No nodes in graph after import"
        return 1
    fi

    test_start "Graph has edges after import"
    local edge_count
    edge_count=$(echo "$stats" | jq -r '.edges // 0')
    if [ "$edge_count" -gt 0 ]; then
        test_pass
        log_info "Edges imported: $edge_count"
    else
        test_fail "No edges in graph after import"
        return 1
    fi
}

# Run if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    run_tests
fi
