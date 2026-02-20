#!/usr/bin/env bash
set -euo pipefail

bash /app/scripts/run-ingest-flow.sh
bash /app/scripts/run-stage-validate-flow.sh
