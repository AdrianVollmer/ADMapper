#!/usr/bin/env bash
#
# Test: Cypher Queries
#
# Runs sample Cypher queries and verifies results.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/utils.sh"
source "$SCRIPT_DIR/../lib/assertions.sh"
source "$SCRIPT_DIR/../lib/api.sh"

run_tests() {
    test_start "Simple node count query"
    local response
    if response=$(api_query "MATCH (n) RETURN count(n) AS total"); then
        test_pass
    else
        test_fail "Query failed: $response"
        return 1
    fi

    test_start "Query returns results"
    local total
    # Handle different response formats: .results.rows[0][0] for Kuzu/CrustDB, .results.results[0].total for others
    total=$(echo "$response" | jq -r '.results.rows[0][0] // .results.results[0].total // .results[0].total // 0' 2>/dev/null)
    total="${total:-0}"  # Default to 0 if empty
    if [ "$total" -gt 0 ] 2>/dev/null; then
        test_pass
        log_info "Total nodes: $total"
    else
        test_fail "Query returned no results"
    fi

    test_start "Query for User nodes"
    if response=$(api_query "MATCH (u:User) RETURN count(u) AS users"); then
        local users
        users=$(echo "$response" | jq -r '.results.rows[0][0] // .results.results[0].users // .results[0].users // 0' 2>/dev/null)
        test_pass
        log_info "User count: $users"
    else
        test_fail "User query failed"
    fi

    test_start "Query for Computer nodes"
    if response=$(api_query "MATCH (c:Computer) RETURN count(c) AS computers"); then
        local computers
        computers=$(echo "$response" | jq -r '.results.rows[0][0] // .results.results[0].computers // .results[0].computers // 0' 2>/dev/null)
        test_pass
        log_info "Computer count: $computers"
    else
        test_fail "Computer query failed"
    fi

    test_start "Query for Group nodes"
    if response=$(api_query "MATCH (g:Group) RETURN count(g) AS groups"); then
        local groups
        groups=$(echo "$response" | jq -r '.results.rows[0][0] // .results.results[0].groups // .results[0].groups // 0' 2>/dev/null)
        test_pass
        log_info "Group count: $groups"
    else
        test_fail "Group query failed"
    fi

    test_start "Query with relationship"
    if response=$(api_query "MATCH (n)-[r]->(m) RETURN count(r) AS edges LIMIT 1"); then
        test_pass
        local edges
        edges=$(echo "$response" | jq -r '.results[0].edges // .rows[0][0] // 0' 2>/dev/null)
        log_info "Edge count from query: $edges"
    else
        test_fail "Relationship query failed"
    fi

    test_start "Query with property filter"
    if response=$(api_query "MATCH (u:User) WHERE u.enabled = true RETURN count(u) AS enabled_users"); then
        test_pass
        local enabled
        enabled=$(echo "$response" | jq -r '.results[0].enabled_users // .rows[0][0] // 0' 2>/dev/null)
        log_info "Enabled users: $enabled"
    else
        test_fail "Property filter query failed"
    fi

    test_start "Query returning node properties"
    if response=$(api_query "MATCH (u:User) RETURN u.name AS name LIMIT 5"); then
        test_pass
        local names
        names=$(echo "$response" | jq -r '.results[].name // .rows[][0]' 2>/dev/null | head -3)
        log_info "Sample user names: $names"
    else
        test_fail "Property return query failed"
    fi

    test_start "Query with type() function"
    if response=$(api_query "MATCH (n)-[r]->(m) RETURN type(r) AS rel_type LIMIT 5"); then
        test_pass
        local types
        types=$(echo "$response" | jq -r '.results[].rel_type // .rows[][0]' 2>/dev/null | sort -u | head -3)
        log_info "Sample relationship types: $types"
    else
        test_fail "type() function query failed"
    fi

    test_start "Query with labels() function"
    if response=$(api_query "MATCH (n) RETURN labels(n) AS labels LIMIT 5"); then
        test_pass
    else
        test_fail "labels() function query failed"
    fi
}

# Run if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    run_tests
fi
