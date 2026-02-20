#!/usr/bin/env bash
set -euo pipefail

python -m drp.orchestration.prefect.flows.ingest_orders_flow
