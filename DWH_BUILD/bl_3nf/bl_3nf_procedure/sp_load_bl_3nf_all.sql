-- Master orchestration procedure for BL_3NF layer
-- Executes all dimension and fact load procedures in a controlled ETL pipeline

CREATE OR REPLACE PROCEDURE bl_3nf.sp_load_bl_3nf_all()
LANGUAGE plpgsql AS $$
DECLARE
    v_run_id  BIGINT;
    v_log_id  BIGINT;
    v_err_msg TEXT;
BEGIN
    -- Create pipeline run record
    INSERT INTO bl_cn.etl_run (pipeline, run_status, run_start, run_by)
    VALUES ('BL_3NF', 'RUNNING', NOW(), CURRENT_USER)
    RETURNING run_id INTO v_run_id;

    -- Pass run_id into session context for downstream logging
    PERFORM set_config('bl_cn.current_run_id', v_run_id::TEXT, FALSE);

    -- Start procedure-level logging
    v_log_id := bl_cn.log_start('bl_3nf.sp_load_bl_3nf_all', 'BL_3NF');

    RAISE NOTICE '=== BL_3NF LAYER START (run_id=%) ===', v_run_id;

    -- Execute all BL_3NF load procedures
    CALL bl_3nf.load_ce_suppliers();
    CALL bl_3nf.load_ce_shippings();
    CALL bl_3nf.load_ce_store_branches();
    CALL bl_3nf.load_ce_products();
    CALL bl_3nf.load_ce_customers_scd();
    CALL bl_3nf.load_ce_employees_scd();
    CALL bl_3nf.load_ce_transactions();

    -- Mark pipeline as successful
    UPDATE bl_cn.etl_run
    SET  run_status = 'SUCCESS', run_end = NOW()
    WHERE run_id = v_run_id;

    CALL bl_cn.log_success(v_log_id, 0);

    RAISE NOTICE '=== BL_3NF LAYER COMPLETE (run_id=%) ===', v_run_id;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;

    -- Mark pipeline as failed with error details
    UPDATE bl_cn.etl_run
    SET  run_status = 'FAILED', run_end = NOW(), notes = v_err_msg
    WHERE run_id = v_run_id;

    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;