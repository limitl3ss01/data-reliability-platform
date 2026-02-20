# Monitoring Runbook

## Flow Audit Source

- Table: `${OBSERVABILITY_SCHEMA}.${FLOW_AUDIT_TABLE}` in PostgreSQL
- Producer: `FlowMonitor` instrumentation in Prefect flows
- Status values: `success`, `failed`

## Operational Checks

1. Check failed runs in last 24h:
   - Use `scripts/monitoring/report-flow-health.sh`
2. Validate latency trend:
   - Review average `duration_seconds` grouped by flow name.
3. Inspect run metadata:
   - Examine `metadata` and `error_message` columns for diagnostics.

## Alerting

- Failure alerts are enabled by `ALERT_ON_FAILURE=true`.
- Set `ALERT_WEBHOOK_URL` to route failures to Slack/Teams/webhook gateway.
- If no webhook is configured, failures are still persisted in audit table.
