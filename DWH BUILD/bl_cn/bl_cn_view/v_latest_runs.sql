
-- PURPOSE : Most recent execution status per procedure

CREATE OR REPLACE VIEW bl_cn.v_latest_runs AS
SELECT DISTINCT ON (procedure_name)
    log_id,
    procedure_name,
    layer,
    status,
    started_at,
    finished_at,
    duration_sec,
    rows_inserted,
    rows_updated,
    rows_skipped,
    error_message,
    run_by
FROM bl_cn.etl_log
ORDER BY procedure_name, started_at DESC;
