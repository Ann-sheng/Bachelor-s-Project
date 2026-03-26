-- SCD Type 1 load for CE_STORE_BRANCHES via MERGE

CREATE OR REPLACE PROCEDURE bl_3nf.load_ce_store_branches()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_affected INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_3nf.load_ce_store_branches', 'BL_3NF');

    MERGE INTO bl_3nf.ce_store_branches tgt
    USING bl_3nf.fn_get_store_branches_data() src
    ON (
        tgt.store_branch_src_id = src.store_branch_src_id
        AND tgt.source_system   = src.source_system
        AND tgt.source_entity   = src.source_entity
    )
    WHEN MATCHED AND (
        tgt.store_branch_state           IS DISTINCT FROM src.store_branch_state           OR
        tgt.store_branch_city            IS DISTINCT FROM src.store_branch_city            OR
        tgt.store_branch_phone_number    IS DISTINCT FROM src.store_branch_phone_number    OR
        tgt.store_branch_operating_days  IS DISTINCT FROM src.store_branch_operating_days  OR
        tgt.store_branch_operating_hours IS DISTINCT FROM src.store_branch_operating_hours
    ) THEN UPDATE SET
        store_branch_state           = src.store_branch_state,
        store_branch_city            = src.store_branch_city,
        store_branch_phone_number    = src.store_branch_phone_number,
        store_branch_operating_days  = src.store_branch_operating_days,
        store_branch_operating_hours = src.store_branch_operating_hours,
        ta_update_dt                 = NOW()

    WHEN NOT MATCHED THEN INSERT (
        store_branch_id,
        store_branch_src_id,
        store_branch_state,
        store_branch_city,
        store_branch_phone_number,
        store_branch_operating_days,
        store_branch_operating_hours,
        ta_insert_dt,
        ta_update_dt,
        source_system,
        source_entity
    ) VALUES (
        nextval('bl_3nf.seq_ce_store_branches'),
        src.store_branch_src_id,
        src.store_branch_state,
        src.store_branch_city,
        src.store_branch_phone_number,
        src.store_branch_operating_days,
        src.store_branch_operating_hours,
        NOW(), NOW(),
        src.source_system,
        src.source_entity
    );

    GET DIAGNOSTICS v_affected = ROW_COUNT;

    CALL bl_cn.log_success(v_log_id, v_affected);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;
