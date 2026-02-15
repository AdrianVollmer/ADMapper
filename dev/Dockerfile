# ADMapper Build Environment
#
# Builds both frontend (Node.js) and backend (Rust/Tauri).
#
# Usage:
#   docker build -t admapper-builder -f dev/Dockerfile .
#   docker run --rm -v $(pwd):/workspace -w /workspace admapper-builder npm run build
#   docker run --rm -v $(pwd):/workspace -w /workspace admapper-builder cargo build --release --manifest-path src-backend/Cargo.toml
#
# Or interactive:
#   docker run --rm -it -v $(pwd):/workspace -w /workspace admapper-builder bash

FROM debian:bookworm-slim

# Avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build essentials
    build-essential \
    pkg-config \
    curl \
    ca-certificates \
    git \
    # Tauri dependencies (Linux/GTK)
    libwebkit2gtk-4.1-dev \
    libappindicator3-dev \
    librsvg2-dev \
    libssl-dev \
    libgtk-3-dev \
    libayatana-appindicator3-dev \
    # Icon generation
    librsvg2-bin \
    imagemagick \
    # Clean up
    && rm -rf /var/lib/apt/lists/*

# Install Node.js (LTS)
ENV NODE_VERSION=20
RUN curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal \
    && rustup --version \
    && cargo --version \
    && rustc --version

# Install Tauri CLI
RUN cargo install tauri-cli

# Set working directory
WORKDIR /workspace

# Default command
CMD ["bash"]
