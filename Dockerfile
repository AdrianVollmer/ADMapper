# ADMapper Production Image (Headless Mode)
#
# Multi-stage build for minimal image size.
#
# Usage:
#   docker build -t admapper .
#   docker run -p 9191:9191 admapper
#
# With a database:
#   docker run -p 9191:9191 -v /path/to/data:/data admapper crustdb:///data/admapper.db

# ==============================================================================
# Stage 1: Build frontend
# ==============================================================================
FROM node:20-slim AS frontend-builder

WORKDIR /app

# Install dependencies first (better caching)
COPY package.json package-lock.json ./
RUN npm ci

# Build frontend (vite root is src/frontend, index.html is there)
COPY src/frontend src/frontend
COPY vite.config.ts tsconfig.json tailwind.config.js postcss.config.js ./
RUN npm run build

# ==============================================================================
# Stage 2: Build backend
# ==============================================================================
FROM rust:1-bookworm AS backend-builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY src/backend src/backend
COPY src/crustdb src/crustdb
COPY src/crustdb-cli src/crustdb-cli

# Build backend in release mode (headless only, no desktop features)
ARG CARGO_BUILD_JOBS
RUN cargo build --manifest-path src/backend/Cargo.toml \
    --no-default-features \
    --features crustdb,neo4j,falkordb \
    --release \
    ${CARGO_BUILD_JOBS:+--jobs $CARGO_BUILD_JOBS}

# ==============================================================================
# Stage 3: Runtime image
# ==============================================================================
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -s /bin/bash admapper

# Copy built artifacts
COPY --from=backend-builder /app/src/backend/target/release/admapper /usr/local/bin/
COPY --from=frontend-builder /app/build /app/build

# Set ownership of static files
RUN chown -R admapper:admapper /app/build

USER admapper
WORKDIR /app

# Expose default port
EXPOSE 9191

# Default to headless mode
ENV RUST_LOG=info
ENV ADMAPPER_HOST="0.0.0.0"
ENV ADMAPPER_PORT=9191

ENTRYPOINT ["sh", "-c", "exec admapper --headless --bind $ADMAPPER_HOST --port $ADMAPPER_PORT \"$@\"", "--"]
CMD []
