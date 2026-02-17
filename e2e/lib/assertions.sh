#!/usr/bin/env bash
#
# Test assertions and result tracking for E2E tests
#

# Guard against multiple inclusion
[ -n "$_E2E_ASSERTIONS_LOADED" ] && return 0
_E2E_ASSERTIONS_LOADED=1

# Source utilities if not already loaded
_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_LIB_DIR/utils.sh"

# Test counters
TEST_PASSED=0
TEST_FAILED=0
TEST_TOTAL=0
CURRENT_TEST=""
CURRENT_TEST_START=0

# Test results array for XML report
# Format: "status|name|duration_ms|message"
declare -a TEST_RESULTS=()

# Report configuration
REPORT_DIR="${REPORT_DIR:-}"
CURRENT_BACKEND="${CURRENT_BACKEND:-unknown}"

# Start a test
test_start() {
    CURRENT_TEST="$1"
    CURRENT_TEST_START=$(date +%s%3N)
    TEST_TOTAL=$((TEST_TOTAL + 1))
    log_test "Running: $CURRENT_TEST"
}

# Calculate test duration in milliseconds
_test_duration_ms() {
    local end=$(date +%s%3N)
    echo $((end - CURRENT_TEST_START))
}

# Mark current test as passed
test_pass() {
    local msg="${1:-$CURRENT_TEST}"
    local duration=$(_test_duration_ms)
    TEST_PASSED=$((TEST_PASSED + 1))
    TEST_RESULTS+=("pass|$CURRENT_TEST|$duration|")
    log_pass "$msg (${duration}ms)"
}

# Mark current test as failed
test_fail() {
    local msg="${1:-$CURRENT_TEST}"
    local duration=$(_test_duration_ms)
    TEST_FAILED=$((TEST_FAILED + 1))
    TEST_RESULTS+=("fail|$CURRENT_TEST|$duration|$msg")
    log_fail "$msg (${duration}ms)"
}

# Assert that two values are equal
# Usage: assert_equals "expected" "actual" "description"
assert_equals() {
    local expected="$1"
    local actual="$2"
    local desc="${3:-values should be equal}"

    if [ "$expected" = "$actual" ]; then
        log_debug "Assert passed: $desc (expected=$expected, actual=$actual)"
        return 0
    else
        log_error "Assert failed: $desc"
        log_error "  Expected: $expected"
        log_error "  Actual:   $actual"
        return 1
    fi
}

# Assert that a value is not empty
# Usage: assert_not_empty "value" "description"
assert_not_empty() {
    local value="$1"
    local desc="${2:-value should not be empty}"

    if [ -n "$value" ]; then
        log_debug "Assert passed: $desc"
        return 0
    else
        log_error "Assert failed: $desc (value is empty)"
        return 1
    fi
}

# Assert that a JSON field exists and has expected value
# Usage: assert_json_field "json" "field" "expected" "description"
assert_json_field() {
    local json="$1"
    local field="$2"
    local expected="$3"
    local desc="${4:-JSON field $field should equal $expected}"

    local actual
    actual=$(echo "$json" | jq -r "$field" 2>/dev/null)

    if [ "$actual" = "$expected" ]; then
        log_debug "Assert passed: $desc"
        return 0
    else
        log_error "Assert failed: $desc"
        log_error "  Field:    $field"
        log_error "  Expected: $expected"
        log_error "  Actual:   $actual"
        return 1
    fi
}

# Assert that a JSON field is greater than or equal to a value
# Usage: assert_json_gte "json" "field" "min_value" "description"
assert_json_gte() {
    local json="$1"
    local field="$2"
    local min_value="$3"
    local desc="${4:-JSON field $field should be >= $min_value}"

    local actual
    actual=$(echo "$json" | jq -r "$field" 2>/dev/null)

    if [ "$actual" -ge "$min_value" ] 2>/dev/null; then
        log_debug "Assert passed: $desc (actual=$actual)"
        return 0
    else
        log_error "Assert failed: $desc"
        log_error "  Field:     $field"
        log_error "  Min value: $min_value"
        log_error "  Actual:    $actual"
        return 1
    fi
}

# Assert HTTP status code
# Usage: assert_http_status "actual_status" "expected_status" "description"
assert_http_status() {
    local actual="$1"
    local expected="$2"
    local desc="${3:-HTTP status should be $expected}"

    assert_equals "$expected" "$actual" "$desc"
}

# Print test summary in TAP format
print_summary() {
    echo ""
    echo "=================================="
    echo "Test Summary"
    echo "=================================="
    echo "Total:  $TEST_TOTAL"
    echo -e "Passed: ${GREEN}$TEST_PASSED${NC}"
    echo -e "Failed: ${RED}$TEST_FAILED${NC}"
    echo ""

    # TAP output
    echo "TAP version 13"
    echo "1..$TEST_TOTAL"

    if [ $TEST_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}Some tests failed!${NC}"
        return 1
    fi
}

# Generate XML report for current backend
# Usage: generate_xml_report "backend_name" "output_file"
generate_xml_report() {
    local backend="$1"
    local output_file="$2"
    local timestamp=$(date -Iseconds)
    local total_time_ms=0

    # Calculate total time
    for result in "${TEST_RESULTS[@]}"; do
        local duration=$(echo "$result" | cut -d'|' -f3)
        total_time_ms=$((total_time_ms + duration))
    done
    local total_time_s=$(echo "scale=3; $total_time_ms / 1000" | bc)

    cat > "$output_file" << 'XMLHEADER'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/css" href="report.css"?>
XMLHEADER

    cat >> "$output_file" << XMLROOT
<testsuites>
  <testsuite name="e2e-${backend}" tests="${TEST_TOTAL}" failures="${TEST_FAILED}" time="${total_time_s}" timestamp="${timestamp}">
XMLROOT

    # Add each test case
    for result in "${TEST_RESULTS[@]}"; do
        local status=$(echo "$result" | cut -d'|' -f1)
        local name=$(echo "$result" | cut -d'|' -f2)
        local duration_ms=$(echo "$result" | cut -d'|' -f3)
        local message=$(echo "$result" | cut -d'|' -f4-)
        local duration_s=$(echo "scale=3; $duration_ms / 1000" | bc)

        # Escape XML special characters in name and message
        name=$(echo "$name" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g')
        message=$(echo "$message" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g')

        if [ "$status" = "pass" ]; then
            cat >> "$output_file" << TESTCASE
    <testcase name="${name}" time="${duration_s}" status="passed"/>
TESTCASE
        else
            cat >> "$output_file" << TESTCASE
    <testcase name="${name}" time="${duration_s}" status="failed">
      <failure message="${message}"/>
    </testcase>
TESTCASE
        fi
    done

    cat >> "$output_file" << 'XMLFOOTER'
  </testsuite>
</testsuites>
XMLFOOTER

    log_info "Generated report: $output_file"
}

# Generate CSS file for XML report styling
generate_report_css() {
    local output_file="$1"
    cat > "$output_file" << 'CSS'
/* E2E Test Report Styles */
testsuites {
  display: block;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  max-width: 900px;
  margin: 2rem auto;
  padding: 1rem;
}

testsuite {
  display: block;
  background: #f8f9fa;
  border-radius: 8px;
  padding: 1.5rem;
  margin-bottom: 1rem;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

testsuite::before {
  content: attr(name) " - " attr(tests) " tests, " attr(failures) " failures, " attr(time) "s";
  display: block;
  font-size: 1.25rem;
  font-weight: 600;
  margin-bottom: 1rem;
  padding-bottom: 0.75rem;
  border-bottom: 2px solid #dee2e6;
}

testcase {
  display: block;
  padding: 0.75rem 1rem;
  margin: 0.5rem 0;
  border-radius: 4px;
  border-left: 4px solid #28a745;
  background: #d4edda;
}

testcase::before {
  content: "✓ " attr(name) " (" attr(time) "s)";
  font-weight: 500;
}

testcase[status="failed"] {
  border-left-color: #dc3545;
  background: #f8d7da;
}

testcase[status="failed"]::before {
  content: "✗ " attr(name) " (" attr(time) "s)";
  color: #721c24;
}

failure {
  display: block;
  margin-top: 0.5rem;
  padding: 0.5rem;
  background: rgba(0,0,0,0.05);
  border-radius: 4px;
  font-size: 0.9rem;
  color: #721c24;
}

failure::before {
  content: attr(message);
}
CSS
    log_info "Generated CSS: $output_file"
}

# Reset test results for new backend
reset_test_results() {
    TEST_PASSED=0
    TEST_FAILED=0
    TEST_TOTAL=0
    TEST_RESULTS=()
}

# Check if all tests passed (for script exit code)
all_tests_passed() {
    [ $TEST_FAILED -eq 0 ]
}
