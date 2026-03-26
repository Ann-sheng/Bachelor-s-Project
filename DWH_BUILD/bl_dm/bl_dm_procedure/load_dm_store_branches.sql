

--  SCD1 load from BL_3NF.CE_STORE_BRANCHES

CREATE OR REPLACE PROCEDURE bl_dm.load_dm_store_branches()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_affected INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_dm_store_branches', 'BL_DM');

    MERGE INTO bl_dm.dm_store_branches tgt
    USING (
        SELECT
            store_branch_id,
            store_branch_state,
            store_branch_city,
            store_branch_phone_number,
            store_branch_operating_days,
            store_branch_operating_hours
        FROM bl_3nf.ce_store_branches
        WHERE store_branch_id <> -1
    ) src
    ON (
        tgt.store_branch_src_id = src.store_branch_id
        AND tgt.source_system   = 'BL_3NF'
        AND tgt.source_entity   = 'CE_STORE_BRANCHES'
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
        store_branch_surr_id,
        store_branch_src_id,
        store_branch_state,
        store_branch_city,
        store_branch_phone_number,
        store_branch_operating_days,
        store_branch_operating_hours,
        ta_insert_dt, ta_update_dt,
        source_system, source_entity
    ) VALUES (
        nextval('bl_dm.sq_store_branch_surr_id'),
        src.store_branch_id,
        COALESCE(src.store_branch_state,           'n. a.'),
        COALESCE(src.store_branch_city,            'n. a.'),
        src.store_branch_phone_number,
        src.store_branch_operating_days,
        src.store_branch_operating_hours,
        NOW(), NOW(),
        'BL_3NF', 'CE_STORE_BRANCHES'
    );

    GET DIAGNOSTICS v_affected = ROW_COUNT;

    CALL bl_cn.log_success(v_log_id, v_affected);

    RAISE NOTICE '[load_dm_store_branches] Rows affected: %', v_affected;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;

