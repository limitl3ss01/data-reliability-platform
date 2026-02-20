#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f ".env" ]]; then
  echo "[demo] .env not found. Creating from .env.example"
  cp .env.example .env
fi

set -a
source .env
set +a

: "${POSTGRES_USER:=drp_user}"
: "${POSTGRES_DB:=drp_platform}"
: "${RAW_SCHEMA:=raw}"
: "${RAW_ORDERS_TABLE:=orders_raw}"

echo "[demo] Ensuring platform is running..."
docker compose up -d

echo "[demo] Running normal ingestion to create a fresh row..."
docker compose exec -T pipeline bash /app/scripts/run-ingest-flow.sh

echo "[demo] Corrupting latest raw row with invalid amount (negative)..."
docker compose exec -T postgres psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -c "
    UPDATE ${RAW_SCHEMA}.${RAW_ORDERS_TABLE}
    SET amount = -999.99
    WHERE id = (
      SELECT id
      FROM ${RAW_SCHEMA}.${RAW_ORDERS_TABLE}
      ORDER BY ingested_at DESC
      LIMIT 1
    );
  "

echo "[demo] Running stage+validate flow on one latest row (expect failure)..."
set +e
docker compose exec -T pipeline python - <<'PY'
from drp.orchestration.prefect.flows.stage_and_validate_orders_flow import stage_and_validate_orders_flow
stage_and_validate_orders_flow(limit=1)
PY
exit_code=$?
set -e

if [[ $exit_code -eq 0 ]]; then
  echo "[demo] Unexpected success. Validation did not fail."
  exit 1
fi

echo "[demo] Expected failure confirmed (Great Expectations quality gate failed)."
echo "[demo] Check Prefect UI and ops.pipeline_flow_audit for failure telemetry."
