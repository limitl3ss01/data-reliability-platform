#!/usr/bin/env bash
set -euo pipefail

set -a
source .env
set +a

: "${OBSERVABILITY_SCHEMA:=ops}"
: "${FLOW_AUDIT_TABLE:=pipeline_flow_audit}"

docker compose exec -T postgres psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -c "SELECT flow_name, status, started_at, duration_seconds FROM ${OBSERVABILITY_SCHEMA}.${FLOW_AUDIT_TABLE} ORDER BY started_at DESC LIMIT 20;"
