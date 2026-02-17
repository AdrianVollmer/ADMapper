#!/usr/bin/env bash
#
# E2E Test Runner for ADMapper
#
# Runs integration tests against all supported database backends.
#
# Usage:
#   ./e2e/run-tests.sh <test_data.zip> [backend]
#
# Arguments:
#   test_data.zip - Path to BloodHound data zip file (required)
#   backend       - Backend to test: kuzu, crustdb, or all (default: all)
#
# Environment variables:
#   ADMAPPER_BIN  - Path to admapper binary (default: target/release/admapper)
#   DEBUG         - Enable debug output
#

set -e

# Get script directory for consistent paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
# Store tests directory (test scripts overwrite SCRIPT_DIR when sourced)
TESTS_DIR="$SCRIPT_DIR/tests"

# Source library files
source "$SCRIPT_DIR/lib/utils.sh"
source "$SCRIPT_DIR/lib/assertions.sh"
source "$SCRIPT_DIR/lib/api.sh"

# Configuration
ADMAPPER_BIN="${ADMAPPER_BIN:-$PROJECT_ROOT/src/backend/target/release/admapper}"
GOLDEN_FILE="/tmp/golden/expected_stats.json"

# Available backends
BACKENDS=(kuzu crustdb)

# Cleanup handler
cleanup() {
	log_info "Cleaning up..."
	stop_server
	if [ -n "$TEMP_DB_DIR" ] && [ -d "$TEMP_DB_DIR" ]; then
		rm -rf "$TEMP_DB_DIR"
	fi
}

trap cleanup EXIT

# Check prerequisites
check_prerequisites() {
	log_info "Checking prerequisites..."

	# Check binary exists
	if [ ! -x "$ADMAPPER_BIN" ]; then
		log_error "ADMapper binary not found: $ADMAPPER_BIN"
		log_info "Build it with: ./scripts/build.sh backend"
		exit 1
	fi

	# Check test data exists
	if [ ! -f "$TEST_DATA" ]; then
		log_error "Test data not found: $TEST_DATA"
		exit 1
	fi

	# Always regenerate expected stats from test data
	export GOLDEN_FILE
	log_info "Generating expected stats from test data..."
	python3 "$SCRIPT_DIR/generate-expected.py" "$TEST_DATA"

	log_info "Prerequisites OK"
}

# Run test suite for a single backend
run_backend_tests() {
	local backend="$1"
	local port=$((9191 + RANDOM % 1000))

	log_info "=========================================="
	log_info "Testing backend: $backend"
	log_info "=========================================="

	# Reset test results for this backend
	reset_test_results
	CURRENT_BACKEND="$backend"

	# Create temporary database directory
	TEMP_DB_DIR=$(mktemp -d -t "e2e-${backend}-XXXXXX")
	log_info "Database directory: $TEMP_DB_DIR"

	# Set environment for test scripts
	export API_PORT="$port"
	export TEST_DATA
	export GOLDEN_FILE
	export ADMAPPER_BIN

	# Start server
	if ! start_server "$backend" "$TEMP_DB_DIR" "$port"; then
		log_error "Failed to start server for $backend"
		return 1
	fi

	# Run tests in order
	local test_failed=0
	for test_script in "$TESTS_DIR/"*.sh; do
		if [ -x "$test_script" ]; then
			log_info "Running: $(basename "$test_script")"
			# Source the test script and run its tests
			source "$test_script"
			if ! run_tests; then
				log_error "Test failed: $(basename "$test_script")"
				test_failed=1
				# Continue with other tests
			fi
		fi
	done

	# Stop server
	stop_server

	# Generate XML report for this backend
	generate_xml_report "$backend" "$REPORT_DIR/report-${backend}.xml"

	# Cleanup temp directory
	rm -rf "$TEMP_DB_DIR"
	TEMP_DB_DIR=""

	return $test_failed
}

show_usage() {
	echo "Usage: $0 <test_data.zip> [backend]"
	echo ""
	echo "Arguments:"
	echo "  test_data.zip  Path to BloodHound data zip file (required)"
	echo "  backend        Backend to test (default: all)"
	echo ""
	echo "Backends:"
	echo "  all      - Test all backends"
	echo "  kuzu     - Test KuzuDB backend only"
	echo "  crustdb  - Test CrustDB backend only"
	echo ""
	echo "Environment variables:"
	echo "  ADMAPPER_BIN  - Path to admapper binary"
	echo "  DEBUG         - Enable debug output"
}

# Main entry point
main() {
	# Parse arguments
	if [ $# -lt 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
		show_usage
		exit 1
	fi

	TEST_DATA="$1"
	local target="${2:-all}"

	log_info "ADMapper E2E Test Suite"
	log_info "======================="

	check_prerequisites

	# Determine which backends to test
	local backends_to_test=()
	case "$target" in
	all)
		backends_to_test=("${BACKENDS[@]}")
		;;
	kuzu | crustdb)
		backends_to_test=("$target")
		;;
	*)
		log_error "Unknown backend: $target"
		echo ""
		show_usage
		exit 1
		;;
	esac

	# Set up reports directory
	REPORT_DIR="$SCRIPT_DIR/reports"
	mkdir -p "$REPORT_DIR"
	generate_report_css "$REPORT_DIR/report.css"

	local overall_failed=0
	local total_passed=0
	local total_failed=0

	# Run tests for each backend
	for backend in "${backends_to_test[@]}"; do
		if ! run_backend_tests "$backend"; then
			overall_failed=1
		fi
		total_passed=$((total_passed + TEST_PASSED))
		total_failed=$((total_failed + TEST_FAILED))
	done

	# Print overall summary
	echo ""
	echo "=================================="
	echo "Overall Summary"
	echo "=================================="
	echo "Total passed: $total_passed"
	echo "Total failed: $total_failed"
	echo ""
	echo "Reports generated in: $REPORT_DIR"
	ls -1 "$REPORT_DIR"/*.xml 2>/dev/null | while read f; do echo "  - $(basename "$f")"; done
	echo ""

	if [ $overall_failed -eq 0 ]; then
		echo -e "${GREEN}All backends passed!${NC}"
	else
		echo -e "${RED}Some backends failed!${NC}"
	fi

	exit $overall_failed
}

# Run if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
	main "$@"
fi
