#!/usr/bin/env bash
#
# ADMapper E2E Test Runner
#
# Runs integration tests using podman-compose or docker-compose.
#
# Usage:
#   ./e2e-test.sh <test_data.zip> [options]
#
# Options:
#   --backend <name>   Test specific backend: kuzu, crustdb, or all (default: all)
#   --build            Build the admapper binary before testing
#   --no-cleanup       Don't remove containers after tests
#   --help             Show this help message
#
# Examples:
#   ./e2e-test.sh _data/ad_example_data.zip
#   ./e2e-test.sh _data/ad_example_data.zip --backend crustdb
#   ./e2e-test.sh _data/ad_example_data.zip --build
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default options
BACKEND="all"
BUILD_FIRST=false
CLEANUP=true
TEST_DATA=""

# Detect container runtime (prefer podman)
detect_runtime() {
	if command -v podman &>/dev/null; then
		CONTAINER_RUNTIME="podman"
		if command -v podman-compose &>/dev/null; then
			COMPOSE_CMD="podman-compose"
		elif command -v docker-compose &>/dev/null; then
			# podman can work with docker-compose via podman socket
			COMPOSE_CMD="docker-compose"
		else
			echo -e "${RED}Error: podman-compose or docker-compose not found${NC}"
			exit 1
		fi
	elif command -v docker &>/dev/null; then
		CONTAINER_RUNTIME="docker"
		if command -v docker-compose &>/dev/null; then
			COMPOSE_CMD="docker-compose"
		elif docker compose version &>/dev/null 2>&1; then
			COMPOSE_CMD="docker compose"
		else
			echo -e "${RED}Error: docker-compose not found${NC}"
			exit 1
		fi
	else
		echo -e "${RED}Error: Neither podman nor docker found${NC}"
		exit 1
	fi

	echo -e "${BLUE}Using: $CONTAINER_RUNTIME with $COMPOSE_CMD${NC}"
}

# Show usage
show_usage() {
	echo "ADMapper E2E Test Runner"
	echo ""
	echo "Usage: $0 <test_data.zip> [options]"
	echo ""
	echo "Arguments:"
	echo "  test_data.zip      Path to BloodHound data zip file (required)"
	echo ""
	echo "Options:"
	echo "  --backend <name>   Test specific backend: cozo, crustdb, neo4j, falkordb, or all (default: all)"
	echo "  --build            Build the admapper binary before testing"
	echo "  --no-cleanup       Don't remove containers after tests"
	echo "  --help             Show this help message"
	echo ""
	echo "Examples:"
	echo "  $0 _data/ad_example_data.zip"
	echo "  $0 _data/ad_example_data.zip --backend crustdb"
	echo "  $0 _data/ad_example_data.zip --backend neo4j"
	echo "  $0 _data/ad_example_data.zip --build"
	echo ""
	echo "Environment variables:"
	echo "  ADMAPPER_BIN       Path to pre-built admapper binary"
	echo "  DEBUG              Enable debug output"
}

# Parse arguments
parse_args() {
	while [[ $# -gt 0 ]]; do
		case $1 in
		--backend)
			BACKEND="$2"
			shift 2
			;;
		--build)
			BUILD_FIRST=true
			shift
			;;
		--no-cleanup)
			CLEANUP=false
			shift
			;;
		--help | -h)
			show_usage
			exit 0
			;;
		-*)
			echo -e "${RED}Unknown option: $1${NC}"
			show_usage
			exit 1
			;;
		*)
			if [ -z "$TEST_DATA" ]; then
				TEST_DATA="$1"
			else
				echo -e "${RED}Unexpected argument: $1${NC}"
				show_usage
				exit 1
			fi
			shift
			;;
		esac
	done

	# Validate test data
	if [ -z "$TEST_DATA" ]; then
		echo -e "${RED}Error: Test data file is required${NC}"
		echo ""
		show_usage
		exit 1
	fi

	if [ ! -f "$TEST_DATA" ]; then
		echo -e "${RED}Error: Test data file not found: $TEST_DATA${NC}"
		exit 1
	fi

	# Convert to absolute path
	TEST_DATA="$(cd "$(dirname "$TEST_DATA")" && pwd)/$(basename "$TEST_DATA")"

	# Validate backend
	case "$BACKEND" in
	all | cozo | crustdb | neo4j | falkordb) ;;
	*)
		echo -e "${RED}Error: Invalid backend: $BACKEND${NC}"
		echo "Valid backends: cozo, crustdb, neo4j, falkordb, all"
		exit 1
		;;
	esac
}

# Build admapper binary
build_admapper() {
	echo -e "${BLUE}Building admapper...${NC}"

	cd "$SCRIPT_DIR/../src/backend"

	# Build with all backends except kuzu (slow to compile)
	cargo build --release --no-default-features --features cozo,crustdb,neo4j,falkordb

	cd "$SCRIPT_DIR"

	echo -e "${GREEN}Build complete${NC}"
}

# Check if admapper binary exists
check_binary() {
	local binary="${ADMAPPER_BIN:-$SCRIPT_DIR/../src/backend/target/release/admapper}"

	if [ ! -x "$binary" ]; then
		echo -e "${YELLOW}ADMapper binary not found: $binary${NC}"

		if [ "$BUILD_FIRST" = true ]; then
			build_admapper
		else
			echo -e "${YELLOW}Use --build to build it, or set ADMAPPER_BIN${NC}"
			exit 1
		fi
	fi

	export ADMAPPER_BIN="$binary"
	echo -e "${BLUE}Using binary: $ADMAPPER_BIN${NC}"
}

# Run tests
run_tests() {
	echo -e "${BLUE}Running E2E tests...${NC}"
	echo -e "${BLUE}Test data: $TEST_DATA${NC}"
	echo -e "${BLUE}Backend(s): $BACKEND${NC}"
	echo ""

	cd "$SCRIPT_DIR"/../e2e

	# Build the test container
	echo -e "${BLUE}Building test container...${NC}"
	$COMPOSE_CMD build tests

	# Run the tests
	# Mount the binary and test data into the container
	local exit_code=0

	$COMPOSE_CMD run \
		--rm \
		-v "$ADMAPPER_BIN:/admapper:ro" \
		-v "$TEST_DATA:/test_data.zip:ro" \
		-e "ADMAPPER_BIN=/admapper" \
        -e RUST_LOG=debug \
		tests \
		./e2e/run_tests.py /test_data.zip "$BACKEND" || exit_code=$?

	return $exit_code
}

# Cleanup
cleanup() {
	if [ "$CLEANUP" = true ]; then
		echo -e "${BLUE}Cleaning up containers...${NC}"
		cd "$SCRIPT_DIR"/../e2e
		$COMPOSE_CMD down --remove-orphans 2>/dev/null || true
	else
		echo -e "${YELLOW}Skipping cleanup (--no-cleanup specified)${NC}"
		echo "To clean up manually: cd e2e && $COMPOSE_CMD down"
	fi
}

# Main
main() {
	echo ""
	echo -e "${GREEN}ADMapper E2E Test Suite${NC}"
	echo "========================="
	echo ""

	detect_runtime
	parse_args "$@"
	check_binary

	# Set up cleanup trap for all exit scenarios
	trap cleanup EXIT
	trap 'echo ""; echo -e "${YELLOW}Interrupted, cleaning up...${NC}"; exit 130' INT TERM

	echo ""

	local exit_code=0
	run_tests || exit_code=$?

	echo ""
	if [ $exit_code -eq 0 ]; then
		echo -e "${GREEN}All tests passed!${NC}"
	else
		echo -e "${RED}Some tests failed!${NC}"
	fi

	# Show reports location
	echo ""
	echo -e "${BLUE}Test reports available in: e2e/reports/${NC}"

	exit $exit_code
}

main "$@"
