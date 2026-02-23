#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}" uv run ty check src
UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache}" uv run pytest --cov=src --cov-report=term-missing --cov-fail-under=100
