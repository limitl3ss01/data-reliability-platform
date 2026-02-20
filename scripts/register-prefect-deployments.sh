#!/usr/bin/env bash
set -euo pipefail

: "${PREFECT_WORK_POOL:=drp-default-pool}"

echo "[prefect] registering deployments to pool: ${PREFECT_WORK_POOL}"
prefect deploy --all --prefect-file /app/prefect.yaml --pool "${PREFECT_WORK_POOL}"
