
-- PURPOSE : Pipeline-level summary grouped by etl_run.run_id, Shows which procedures ran in each pipeline execution

CREATE OR REPLACE VIEW bl_cn.v_pipeline_runs AS
SELECT
    r.run_id,
    r.pipeline,
    r.run_status,
    r.run_start,
    r.run_end,
    r.run_duration_sec,
    r.run_by,
    COUNT(l.log_id)                                              AS procedures_called,
    COUNT(l.log_id) FILTER (WHERE l.status = 'SUCCESS')         AS procedures_ok,
    COUNT(l.log_id) FILTER (WHERE l.status = 'FAILED')          AS procedures_failed,
    COUNT(l.log_id) FILTER (WHERE l.status = 'STARTED')         AS procedures_still_running,
    COALESCE(SUM(l.rows_inserted) FILTER (WHERE l.status = 'SUCCESS'), 0) AS total_rows_inserted,
    COALESCE(SUM(l.rows_updated)  FILTER (WHERE l.status = 'SUCCESS'), 0) AS total_rows_updated,
    (
        SELECT procedure_name
        FROM   bl_cn.etl_log
        WHERE  run_id  = r.run_id
          AND  status  = 'SUCCESS'
        ORDER  BY duration_sec DESC NULLS LAST
        LIMIT  1
    ) AS slowest_procedure,
    CASE
        WHEN r.run_duration_sec > 0
        THEN ROUND(COALESCE(SUM(l.rows_inserted) FILTER (WHERE l.status = 'SUCCESS'), 0) / r.run_duration_sec, 0)
        ELSE NULL
    END  AS rows_per_sec
FROM bl_cn.etl_run r
LEFT JOIN bl_cn.etl_log l ON l.run_id = r.run_id
GROUP BY r.run_id, r.pipeline, r.run_status,
         r.run_start, r.run_end, r.run_duration_sec, r.run_by
ORDER BY r.run_start DESC;

