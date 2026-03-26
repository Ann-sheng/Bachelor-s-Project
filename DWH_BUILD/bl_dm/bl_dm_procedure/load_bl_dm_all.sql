-- PURPOSE : Master procedure for full BL_DM layer load.
-- Mirrors sp_load_bl_3nf_all — creates a run record, propagates run_id
-- via session config, calls all dimension and fact loaders in dependency order.

CREATE OR REPLACE PROCEDURE bl_dm.sp_load_bl_dm_all()
LANGUAGE plpgsql AS $$
DECLARE
    v_run_id  BIGINT;
    v_log_id  BIGINT;
    v_err_msg TEXT;
BEGIN
    INSERT INTO bl_cn.etl_run (pipeline, run_status, run_start, run_by)
    VALUES ('BL_DM', 'RUNNING', NOW(), CURRENT_USER)
    RETURNING run_id INTO v_run_id;

    PERFORM set_config('bl_cn.current_run_id', v_run_id::TEXT, FALSE);

    v_log_id := bl_cn.log_start('bl_dm.sp_load_bl_dm_all', 'BL_DM');

    RAISE NOTICE '=== BL_DM LAYER START (run_id=%) ===', v_run_id;

    -- Date dimension must come first — fact table FKs reference it
    CALL bl_dm.load_dim_dates();

    -- SCD1 dimensions (no ordering dependency among themselves)
    CALL bl_dm.load_dm_suppliers();
    CALL bl_dm.load_dm_shippings();
    CALL bl_dm.load_dm_store_branches();

    -- dm_products depends on dm_suppliers
    CALL bl_dm.load_dm_products();

    -- SCD2 dimensions
    CALL bl_dm.load_dm_customers_scd();
    CALL bl_dm.load_dm_employees_scd();

    -- Junk dimension
    CALL bl_dm.load_dm_junk_transactions();

    -- Fact table last — depends on all dimensions above
    CALL bl_dm.load_fct_transactions_dd();

    UPDATE bl_cn.etl_run
    SET run_status = 'SUCCESS', run_end = NOW()
    WHERE run_id = v_run_id;

    CALL bl_cn.log_success(v_log_id, 0);

    RAISE NOTICE '=== BL_DM LAYER COMPLETE (run_id=%) ===', v_run_id;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;

    UPDATE bl_cn.etl_run
    SET run_status = 'FAILED', run_end = NOW(), notes = v_err_msg
    WHERE run_id = v_run_id;

    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;