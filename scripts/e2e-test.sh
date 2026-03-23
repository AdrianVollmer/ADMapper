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
#   --backend <name>     Test specific backend: crustdb, neo4j, falkordb, or all (default: all)
#   --with-bloodhound    Also start BloodHound CE and compare its graph against CrustDB
#   --build              Build the admapper binary before testing
#   --no-cleanup         Don't remove containers after tests
#   --help               Show this help message
#
# Examples:
#   ./e2e-test.sh _data/ad_example_data.zip
#   ./e2e-test.sh _data/ad_example_data.zip --backend crustdb
#   ./e2e-test.sh _data/ad_example_data.zip --backend crustdb --with-bloodhound
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
WITH_BLOODHOUND=false
TEST_DATA=""
DOCKERHOUND_PID=""

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
	echo "  --backend <name>     Test specific backend: crustdb, neo4j, falkordb, or all (default: all)"
	echo "  --with-bloodhound    Start BloodHound CE v8.9 and compare its import against CrustDB"
	echo "  --build              Build the admapper binary before testing"
	echo "  --no-cleanup         Don't remove containers after tests"
	echo "  --help               Show this help message"
	echo ""
	echo "Examples:"
	echo "  $0 _data/ad_example_data.zip"
	echo "  $0 _data/ad_example_data.zip --backend crustdb"
	echo "  $0 _data/ad_example_data.zip --backend crustdb --with-bloodhound"
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
		--with-bloodhound)
			WITH_BLOODHOUND=true
			shift
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
	all | crustdb | neo4j | falkordb) ;;
	*)
		echo -e "${RED}Error: Invalid backend: $BACKEND${NC}"
		echo "Valid backends: crustdb, neo4j, falkordb, all"
		exit 1
		;;
	esac
}

# Build admapper binary
build_admapper() {
	echo -e "${BLUE}Building admapper...${NC}"

	cd "$SCRIPT_DIR/../src/backend"

	cargo build --release --no-default-features --features crustdb,neo4j,falkordb

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

	# Warn if the binary is older than any source files
	local newest_src
	newest_src=$(find "$SCRIPT_DIR"/../src -name '*.rs' -newer "$binary" 2>/dev/null | head -1)
	if [ -n "$newest_src" ]; then
		echo -e "${YELLOW}WARNING: Binary is older than source files. Consider rebuilding with --build${NC}"
		echo -e "${YELLOW}  Binary: $(stat -c '%y' "$binary" 2>/dev/null || stat -f '%Sm' "$binary" 2>/dev/null)${NC}"
		echo -e "${YELLOW}  Newer:  $newest_src${NC}"
	fi
}

# Start BloodHound CE via dockerhound, import test data, dump graph to JSON.
# Sets BH_GRAPH_FILE on success.
start_bloodhound() {
	echo -e "${BLUE}Starting BloodHound CE via dockerhound...${NC}"

	# Ensure reports directory exists (used as output location)
	local reports_dir="$SCRIPT_DIR/../e2e/reports"
	mkdir -p "$reports_dir"

	# Create isolated data directory for dockerhound (cleaned up on exit)
	BH_DATA_DIR=$(mktemp -d)

	# Start dockerhound in background (BH CE on localhost:8181, Neo4j HTTP on 7474)
	uv tool run dockerhound --port 8181 --data-dir "$BH_DATA_DIR" &
	DOCKERHOUND_PID=$!

	# Wait for BH CE health endpoint
	echo -e "${BLUE}Waiting for BloodHound CE to become ready...${NC}"
	local elapsed=0
	local timeout=180
	while [ $elapsed -lt $timeout ]; do
		if curl -sfL http://localhost:8181/ >/dev/null 2>&1; then
			echo -e "${GREEN}BloodHound CE is ready${NC}"
			break
		fi
		sleep 2
		elapsed=$((elapsed + 2))
	done
	if [ $elapsed -ge $timeout ]; then
		echo -e "${RED}BloodHound CE not ready after ${timeout}s${NC}"
		return 1
	fi

	# Authenticate with BH CE API
	_bh_authenticate || return 1

	# Upload test data and wait for BH CE to finish ingestion
	_bh_upload_test_data || return 1

	# Dump graph to JSON
	_bh_dump_graph "$reports_dir/bloodhound_graph.json" || return 1

	# Stop dockerhound
	_bh_stop

	export BH_GRAPH_FILE="$reports_dir/bloodhound_graph.json"
	echo -e "${GREEN}BloodHound CE graph saved to $BH_GRAPH_FILE${NC}"
}

# Authenticate with BH CE. Sets BH_TOKEN on success.
_bh_authenticate() {
	local login_resp
	login_resp=$(curl -sf http://localhost:8181/api/v2/login \
		-H 'Content-Type: application/json' \
		-d '{"login_method":"secret","secret":"admin","username":"admin"}') || {
		echo -e "${RED}BH CE login request failed${NC}"
		return 1
	}

	BH_TOKEN=$(echo "$login_resp" | jq -r '.data.session_token // empty')
	if [ -z "$BH_TOKEN" ]; then
		echo -e "${RED}No session token in BH CE login response${NC}"
		return 1
	fi

	echo -e "${GREEN}Authenticated with BloodHound CE${NC}"
}

# Upload test data to BH CE and wait for ingestion to complete.
_bh_upload_test_data() {
	echo -e "${BLUE}Uploading test data to BloodHound CE...${NC}"

	# Step 1: Create upload job
	local start_resp
	start_resp=$(curl -sf -X POST http://localhost:8181/api/v2/file-upload/start \
		-H "Authorization: Bearer $BH_TOKEN") || {
		echo -e "${RED}Failed to create upload job${NC}"
		return 1
	}
	local job_id
	job_id=$(echo "$start_resp" | jq -r '.data.id // empty')
	if [ -z "$job_id" ]; then
		echo -e "${RED}No job ID in upload start response${NC}"
		return 1
	fi
	echo -e "${BLUE}  Upload job created: $job_id${NC}"

	# Step 2: Upload the ZIP file
	local upload_code
	upload_code=$(curl -s -o /dev/null -w '%{http_code}' \
		-X POST "http://localhost:8181/api/v2/file-upload/${job_id}" \
		-H "Authorization: Bearer $BH_TOKEN" \
		-H "Content-Type: application/zip" \
		--data-binary "@${TEST_DATA}")
	if [ "$upload_code" -lt 200 ] || [ "$upload_code" -ge 300 ]; then
		echo -e "${RED}File upload failed (HTTP $upload_code)${NC}"
		return 1
	fi
	echo -e "${BLUE}  ZIP uploaded${NC}"

	# Step 3: End the upload job to trigger ingestion
	local end_code
	end_code=$(curl -s -o /dev/null -w '%{http_code}' \
		-X POST "http://localhost:8181/api/v2/file-upload/${job_id}/end" \
		-H "Authorization: Bearer $BH_TOKEN")
	if [ "$end_code" -lt 200 ] || [ "$end_code" -ge 300 ]; then
		echo -e "${RED}Failed to finalize upload job (HTTP $end_code)${NC}"
		return 1
	fi
	echo -e "${BLUE}  Upload job ended, waiting for ingestion...${NC}"

	# Step 4: Poll the job status until BH CE reports completion
	_bh_wait_for_job "$job_id" || return 1
}

# Poll BH CE file-upload job status until it reaches a terminal state.
_bh_wait_for_job() {
	local job_id="$1"
	local elapsed=0
	local timeout=300

	while [ $elapsed -lt $timeout ]; do
		sleep 5
		elapsed=$((elapsed + 5))

		local job_json
		job_json=$(curl -sf "http://localhost:8181/api/v2/file-upload?skip=0&limit=10&sort_by=-id" \
			-H "Authorization: Bearer $BH_TOKEN") || continue

		local status status_msg total_files failed_files
		status=$(echo "$job_json" | jq -r ".data[] | select(.id == $job_id) | .status")
		status_msg=$(echo "$job_json" | jq -r ".data[] | select(.id == $job_id) | .status_message")
		total_files=$(echo "$job_json" | jq -r ".data[] | select(.id == $job_id) | .total_files")
		failed_files=$(echo "$job_json" | jq -r ".data[] | select(.id == $job_id) | .failed_files")

		if [ -z "$status" ]; then
			continue
		fi

		echo -e "${BLUE}  Job $job_id: status=$status ($status_msg) files=$total_files failed=$failed_files${NC}"

		# status 2 = Complete
		if [ "$status" -eq 2 ]; then
			if [ "$failed_files" -gt 0 ]; then
				echo -e "${YELLOW}WARNING: $failed_files/$total_files files failed to ingest${NC}"
			fi
			echo -e "${GREEN}Ingestion complete: $total_files files${NC}"
			return 0
		fi

		# status 5 = all failed, status -1 = invalid
		if [ "$status" -eq 5 ] || [ "$status" -eq -1 ]; then
			echo -e "${RED}Ingestion failed: $status_msg${NC}"
			return 1
		fi
	done

	echo -e "${RED}Ingestion not complete after ${timeout}s${NC}"
	return 1
}

# Query all nodes and edges from Neo4j and write to a JSON file.
_bh_dump_graph() {
	local output_file="$1"
	echo -e "${BLUE}Dumping BloodHound CE graph...${NC}"

	local nodes_file edges_file
	nodes_file=$(mktemp)
	edges_file=$(mktemp)
	trap "rm -f '$nodes_file' '$edges_file'" RETURN

	curl -sf http://localhost:7474/db/neo4j/tx/commit \
		-u neo4j:bloodhoundcommunityedition \
		-H 'Content-Type: application/json' \
		-d '{"statements":[{"statement":"MATCH (n) RETURN labels(n) AS labels, n.objectid AS objectid ORDER BY objectid"}]}' \
		>"$nodes_file" || {
		echo -e "${RED}Failed to query nodes from Neo4j${NC}"
		return 1
	}

	curl -sf http://localhost:7474/db/neo4j/tx/commit \
		-u neo4j:bloodhoundcommunityedition \
		-H 'Content-Type: application/json' \
		-d '{"statements":[{"statement":"MATCH (n)-[r]->(m) RETURN n.objectid AS src, type(r) AS rel, m.objectid AS tgt ORDER BY src, rel, tgt"}]}' \
		>"$edges_file" || {
		echo -e "${RED}Failed to query edges from Neo4j${NC}"
		return 1
	}

	# Transform Neo4j responses into clean JSON
	jq -n \
		--slurpfile nodes "$nodes_file" \
		--slurpfile edges "$edges_file" \
		'{
			nodes: [($nodes[0].results[0].data // [])[] | {labels: .row[0], objectid: .row[1]}],
			edges: [($edges[0].results[0].data // [])[] | {src: .row[0], rel: .row[1], tgt: .row[2]}]
		}' >"$output_file" || {
		echo -e "${RED}Failed to write graph JSON${NC}"
		return 1
	}

	local n_nodes n_edges
	n_nodes=$(jq '.nodes | length' "$output_file")
	n_edges=$(jq '.edges | length' "$output_file")
	echo -e "${GREEN}Dumped $n_nodes nodes, $n_edges edges${NC}"
}

# Stop dockerhound process and clean up its data directory.
_bh_stop() {
	if [ -n "$DOCKERHOUND_PID" ] && kill -0 "$DOCKERHOUND_PID" 2>/dev/null; then
		echo -e "${BLUE}Stopping BloodHound CE...${NC}"
		kill "$DOCKERHOUND_PID" 2>/dev/null
		wait "$DOCKERHOUND_PID" 2>/dev/null || true
		DOCKERHOUND_PID=""
		echo -e "${GREEN}BloodHound CE stopped${NC}"
	fi
	if [ -n "$BH_DATA_DIR" ] && [ -d "$BH_DATA_DIR" ]; then
		rm -rf "$BH_DATA_DIR"
		BH_DATA_DIR=""
	fi
}

# Run tests
run_tests() {
	echo -e "${BLUE}Running E2E tests...${NC}"
	echo -e "${BLUE}Test data: $TEST_DATA${NC}"
	echo -e "${BLUE}Backend(s): $BACKEND${NC}"
	if [ "$WITH_BLOODHOUND" = true ]; then
		echo -e "${BLUE}BloodHound CE comparison: enabled${NC}"
	fi
	echo ""

	cd "$SCRIPT_DIR"/../e2e

	# Build the test container
	echo -e "${BLUE}Building test container...${NC}"
	$COMPOSE_CMD build tests

	# Start BloodHound CE if requested
	local bh_env_args=()
	local bh_test_arg=""

	if [ "$WITH_BLOODHOUND" = true ]; then
		start_bloodhound
		# Graph file lands in e2e/reports/ which is mounted rw in the container
		bh_env_args=(
			-e "WITH_BLOODHOUND=1"
			-e "BH_GRAPH_FILE=/workspace/e2e/reports/bloodhound_graph.json"
		)
		bh_test_arg="--with-bloodhound"
	fi

	# Run the tests
	# Mount the binary and test data into the container
	local exit_code=0

	# Get git commit hash to pass to container (git not available inside)
	local git_commit
	git_commit=$(git -C "$SCRIPT_DIR/.." rev-parse HEAD 2>/dev/null || echo "unknown")

	$COMPOSE_CMD run \
		--rm \
		-v "$ADMAPPER_BIN:/admapper:ro" \
		-v "$TEST_DATA:/test_data.zip:ro" \
		-e "ADMAPPER_BIN=/admapper" \
		-e "GIT_COMMIT=$git_commit" \
		-e RUST_LOG=debug \
		"${bh_env_args[@]}" \
		tests \
		./e2e/run_tests.py /test_data.zip "$BACKEND" $bh_test_arg || exit_code=$?

	return $exit_code
}

# Cleanup
cleanup() {
	# Always stop dockerhound if it's still running (e.g. interrupted mid-import)
	_bh_stop

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
