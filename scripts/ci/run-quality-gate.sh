#!/usr/bin/env bash
set -euo pipefail

python -m pip install --upgrade pip
pip install -e ".[dev]"

ruff check src tests
pytest -q tests/unit tests/integration
docker compose --env-file .env.example config >/dev/null

echo "Quality gate checks passed."
