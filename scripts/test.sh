#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Syncing dev dependencies..."
uv sync --extra dev

echo "Running type checks (ty)..."
uv run ty check src

echo "Running tests with 100% coverage..."
uv run pytest --cov=src --cov-report=term-missing --cov-fail-under=100

echo "Formatting with ruff..."
uv run ruff format
