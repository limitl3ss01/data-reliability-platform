-- Recent failures
SELECT
    flow_name,
    flow_run_id,
    status,
    started_at,
    ended_at,
    duration_seconds,
    error_message
FROM ops.pipeline_flow_audit
WHERE status = 'failed'
  AND started_at >= NOW() - INTERVAL '24 hours'
ORDER BY started_at DESC;

-- Success rate by flow over 24h
SELECT
    flow_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_runs,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS success_rate_pct
FROM ops.pipeline_flow_audit
WHERE started_at >= NOW() - INTERVAL '24 hours'
GROUP BY flow_name
ORDER BY flow_name;

-- P95 duration by flow over 24h
SELECT
    flow_name,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds) AS p95_duration_seconds
FROM ops.pipeline_flow_audit
WHERE started_at >= NOW() - INTERVAL '24 hours'
GROUP BY flow_name
ORDER BY flow_name;
