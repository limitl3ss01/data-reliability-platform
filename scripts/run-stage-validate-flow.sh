#!/usr/bin/env bash
set -euo pipefail

python -m drp.orchestration.prefect.flows.stage_and_validate_orders_flow
