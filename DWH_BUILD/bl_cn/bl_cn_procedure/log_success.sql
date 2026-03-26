
--  Called at SUCCESS end of every load procedure Updates log entry to SUCCESS with row counts

CREATE OR REPLACE PROCEDURE bl_cn.log_success(
    p_log_id      BIGINT,
    p_inserted    INTEGER DEFAULT 0,
    p_updated     INTEGER DEFAULT 0,
    p_skipped     INTEGER DEFAULT 0
)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE bl_cn.etl_log
    SET
        status        = 'SUCCESS',
        rows_inserted = p_inserted,
        rows_updated  = p_updated,
        rows_skipped  = p_skipped,
        finished_at   = NOW()
    WHERE log_id = p_log_id;
    
    RAISE NOTICE '[log_id=%] SUCCESS — inserted=%, updated=%, skipped=%',
        p_log_id, p_inserted, p_updated, p_skipped;
END;
$$;

