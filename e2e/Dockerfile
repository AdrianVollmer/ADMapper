# ADMapper E2E Test Environment
#
# Contains test tools for integration testing.
# Database servers are provided via docker-compose using official images.
#
# Usage with docker-compose (recommended):
#   docker compose -f e2e/docker-compose.yml run --rm tests ./e2e/run-tests.sh <data.zip>
#
# Standalone usage (crustdb only, no external databases):
#   docker build -t admapper-e2e -f e2e/Dockerfile .
#   docker run --rm -v $(pwd):/workspace admapper-e2e ./e2e/run-tests.sh <data.zip>

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

# Install test tools and dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Test tools
    curl \
    jq \
    python3 \
    netcat-openbsd \
    bc \
    # CA certificates for HTTPS
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create test directories
RUN mkdir -p /tmp/e2e-tests

WORKDIR /workspace

# Default command shows usage
CMD ["echo", "Usage: ./e2e/run-tests.sh <test_data.zip> [backend]"]
