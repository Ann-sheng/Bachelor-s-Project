

CREATE OR REPLACE PROCEDURE stg_cl.load_stg_all()
LANGUAGE plpgsql AS $$
DECLARE
    v_run_id  BIGINT;
    v_log_id  BIGINT;
    v_err_msg TEXT;
BEGIN
    INSERT INTO bl_cn.etl_run (pipeline, run_status, run_start, run_by)
    VALUES ('STG_CLN', 'RUNNING', NOW(), CURRENT_USER)
    RETURNING run_id INTO v_run_id;

    PERFORM set_config('bl_cn.current_run_id', v_run_id::TEXT, FALSE);

    v_log_id := bl_cn.log_start('stg_cl.load_stg_all', 'STG_CLN');

    RAISE NOTICE '=== STG_CLN LAYER START (run_id=%) ===', v_run_id;

    CALL stg_cl.load_stg_offline();
    CALL stg_cl.load_stg_online();

    UPDATE bl_cn.etl_run
    SET run_status = 'SUCCESS', run_end = NOW()
    WHERE run_id = v_run_id;

    CALL bl_cn.log_success(v_log_id, 0);

    RAISE NOTICE '=== STG_CL LAYER COMPLETE (run_id=%) ===', v_run_id;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;

    UPDATE bl_cn.etl_run
    SET run_status = 'FAILED', run_end = NOW(), notes = v_err_msg
    WHERE run_id = v_run_id;

    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;