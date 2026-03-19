

--  All FAILED procedure runs + stalled STARTED runs


CREATE OR REPLACE VIEW bl_cn.v_failed_runs AS
SELECT
    log_id,
    run_id,
    procedure_name,
    layer,
    'FAILED'        AS issue_type,
    started_at,
    finished_at,
    duration_sec,
    error_message
FROM bl_cn.etl_log
WHERE status = 'FAILED'

UNION ALL

SELECT
    log_id,
    run_id,
    procedure_name,
    layer,
    'STALLED'       AS issue_type,
    started_at,
    NULL            AS finished_at,
    ROUND(EXTRACT(EPOCH FROM (NOW() - started_at)), 2) AS duration_sec,
    'Procedure started but no SUCCESS/FAILED after 30+ minutes' AS error_message
FROM bl_cn.etl_log
WHERE status    = 'STARTED'
  AND started_at < NOW() - INTERVAL '30 minutes'

ORDER BY started_at DESC;
