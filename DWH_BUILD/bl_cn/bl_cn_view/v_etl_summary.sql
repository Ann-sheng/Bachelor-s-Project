

--  Aggregated performance stats per procedure Shows total runs, success rate, avg duration, total rows

CREATE OR REPLACE VIEW bl_cn.v_etl_summary AS
SELECT
    layer,
    procedure_name,
    COUNT(*)                                            AS total_runs,
    COUNT(*) FILTER (WHERE status = 'SUCCESS')          AS successful_runs,
    COUNT(*) FILTER (WHERE status = 'FAILED')           AS failed_runs,
    COUNT(*) FILTER (WHERE status = 'STARTED')          AS started_not_finished,
    ROUND(COUNT(*) FILTER (WHERE status = 'SUCCESS') * 100.0 / NULLIF(COUNT(*), 0), 1)  AS success_rate_pct,
    ROUND(AVG(duration_sec) FILTER (WHERE status = 'SUCCESS'), 2) AS avg_duration_sec,
    ROUND(MAX(duration_sec) FILTER (WHERE status = 'SUCCESS'), 2) AS max_duration_sec,
    ROUND(MIN(duration_sec) FILTER (WHERE status = 'SUCCESS'), 2) AS min_duration_sec,
    COALESCE(SUM(rows_inserted) FILTER (WHERE status = 'SUCCESS'), 0) AS total_rows_inserted,
    COALESCE(SUM(rows_updated)  FILTER (WHERE status = 'SUCCESS'), 0) AS total_rows_updated,
    COALESCE(SUM(rows_skipped)  FILTER (WHERE status = 'SUCCESS'), 0) AS total_rows_skipped,
    MAX(started_at)                                     AS last_run_at,
    MAX(started_at) FILTER (WHERE status = 'SUCCESS')   AS last_success_at,
    MAX(started_at) FILTER (WHERE status = 'FAILED')    AS last_failure_at
FROM bl_cn.etl_log
GROUP BY layer, procedure_name
ORDER BY layer, procedure_name;
