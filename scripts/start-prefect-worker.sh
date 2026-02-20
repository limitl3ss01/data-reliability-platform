#!/usr/bin/env bash
set -euo pipefail

echo "[pipeline] waiting for Prefect API: ${PREFECT_API_URL}"
until prefect work-pool ls >/dev/null 2>&1; do
  sleep 3
done

if ! prefect work-pool inspect "${PREFECT_WORK_POOL}" >/dev/null 2>&1; then
  prefect work-pool create "${PREFECT_WORK_POOL}" --type process
fi

echo "[pipeline] starting worker on pool: ${PREFECT_WORK_POOL}"
exec prefect worker start --pool "${PREFECT_WORK_POOL}" --type process
