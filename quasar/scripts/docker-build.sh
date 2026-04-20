#!/bin/bash
# Fast Docker build using pre-compiled local binary.
#
# Usage:
#   cd quasar
#   ./scripts/docker-build.sh
#
# Why: the original Dockerfile compiles inside the container,
# which takes 10+ minutes on slow networks. This script builds
# locally (reusing cargo cache, ~15s) and then packages the
# binary into a slim Docker image (~1s).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

TMP_BINARY="quasar-server-bin"
IMAGE_TAG="${IMAGE_TAG:-quasar-server:latest}"

cleanup() {
    if [ -f "${TMP_BINARY}" ]; then
        rm -f "${TMP_BINARY}"
    fi
}
trap cleanup EXIT

echo "[1/3] Building quasar-server (local, release)..."
cargo build --release -p quasar-server

echo "[2/3] Copying binary for Docker build context..."
cp target/release/quasar-server "${TMP_BINARY}"

echo "[3/3] Building Docker image using Dockerfile.fast..."
docker build -f Dockerfile.fast -t "${IMAGE_TAG}" .

echo "[4/4] Cleaning up dangling images from previous builds..."
docker images "${IMAGE_TAG%%:*}" --filter "dangling=true" -q | xargs -r docker rmi -f 2>/dev/null || true

echo ""
echo "Done! Image: ${IMAGE_TAG}"
echo ""
echo "Start minimal deployment:"
echo "  docker compose up -d"
echo ""
echo "Start Lance integration deployment:"
echo "  docker compose -f docker-compose.lance.yml up -d"
