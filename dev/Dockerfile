# ADMapper Build Environment
#
# Uses pre-built Rust and downloads tauri-cli binary for fast builds.
#
# Usage:
#   docker build -t admapper-dev -f dev/Dockerfile .
#   docker run --rm -v $(pwd):/workspace -w /workspace admapper-dev scripts/build.sh

FROM rust:1-bookworm

# Avoid prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
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
    # Node.js
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js (LTS)
ENV NODE_VERSION=20
RUN curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Download pre-built tauri-cli
ENV TAURI_CLI_VERSION=2.2.5
RUN curl -fsSL "https://github.com/tauri-apps/tauri/releases/download/tauri-cli-v${TAURI_CLI_VERSION}/cargo-tauri-x86_64-unknown-linux-gnu.tgz" \
    | tar -xzf - -C /usr/local/bin \
    && chmod +x /usr/local/bin/cargo-tauri

WORKDIR /workspace

CMD ["bash"]
