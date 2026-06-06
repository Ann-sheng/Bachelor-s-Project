-- Master orchestration procedure for the STG_CLN ETL layer.
-- Executes all staging loads and handles run tracking, logging, and status updates.

CREATE OR REPLACE PROCEDURE stg_cln.sp_load_stg_all()
LANGUAGE plpgsql AS $$
DECLARE
    v_run_id  BIGINT;
    v_log_id  BIGINT;
    v_err_msg TEXT;
BEGIN
    -- Start ETL run record
    INSERT INTO bl_cn.etl_run (pipeline, run_status, run_start, run_by)
    VALUES ('STG_CLN', 'RUNNING', NOW(), CURRENT_USER)
    RETURNING run_id INTO v_run_id;

    PERFORM set_config('bl_cn.current_run_id', v_run_id::TEXT, FALSE);

    -- Start logging
    v_log_id := bl_cn.log_start('stg_cln.load_stg_all', 'STG_CLN');

    RAISE NOTICE '=== STG_CLN LAYER START (run_id=%) ===', v_run_id;

    -- Execute staging loads
    CALL stg_cln.load_stg_offline();
    CALL stg_cln.load_stg_online();

    -- Mark run as successful
    UPDATE bl_cn.etl_run
    SET run_status = 'SUCCESS', run_end = NOW()
    WHERE run_id = v_run_id;

    CALL bl_cn.log_success(v_log_id, 0);

    RAISE NOTICE '=== STG_CLN LAYER COMPLETE (run_id=%) ===', v_run_id;

EXCEPTION WHEN OTHERS THEN
    -- Handle failures
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;

    UPDATE bl_cn.etl_run
    SET run_status = 'FAILED', run_end = NOW(), notes = v_err_msg
    WHERE run_id = v_run_id;

    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;