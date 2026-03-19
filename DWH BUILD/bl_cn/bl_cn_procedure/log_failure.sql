
-- Called in EXCEPTION handler of every load procedure, Updates log entry to FAILED with error text, Called BEFORE RAISE to ensure the update persists

CREATE OR REPLACE PROCEDURE bl_cn.log_failure(
    p_log_id    BIGINT,
    p_error_msg TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE bl_cn.etl_log
    SET
        status        = 'FAILED',
        error_message = p_error_msg,
        finished_at   = NOW()
    WHERE log_id = p_log_id;

    RAISE WARNING '[log_id=%] FAILED — %', p_log_id, p_error_msg;
END;
$$;

