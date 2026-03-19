
-- SCD1 load from BL_3NF.CE_SUPPLIERS

CREATE OR REPLACE PROCEDURE bl_dm.load_dm_suppliers()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_affected INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_dm_suppliers', 'BL_DM');

    MERGE INTO bl_dm.dm_suppliers tgt
    USING (
        SELECT
            supplier_id,    
            supplier_name,
            supplier_email,
            supplier_number,
            supplier_primary_contact,
            supplier_location
        FROM bl_3nf.ce_suppliers
        WHERE supplier_id <> -1   
    ) src
    ON (
        tgt.supplier_src_id = src.supplier_id      
        AND tgt.source_system = 'BL_3NF'
        AND tgt.source_entity = 'CE_SUPPLIERS'
    )
    WHEN MATCHED AND (
        tgt.supplier_name            IS DISTINCT FROM src.supplier_name            OR
        tgt.supplier_email           IS DISTINCT FROM src.supplier_email           OR
        tgt.supplier_number          IS DISTINCT FROM src.supplier_number          OR
        tgt.supplier_primary_contact IS DISTINCT FROM src.supplier_primary_contact OR
        tgt.supplier_location        IS DISTINCT FROM src.supplier_location
    ) THEN UPDATE SET
        supplier_name            = src.supplier_name,
        supplier_email           = src.supplier_email,
        supplier_number          = src.supplier_number,
        supplier_primary_contact = src.supplier_primary_contact,
        supplier_location        = src.supplier_location,
        ta_update_dt             = NOW()

    WHEN NOT MATCHED THEN INSERT (
        supplier_surr_id,
        supplier_src_id,
        supplier_name,
        supplier_email,
        supplier_number,
        supplier_primary_contact,
        supplier_location,
        ta_insert_dt, ta_update_dt,
        source_system, source_entity
    ) VALUES (
        nextval('bl_dm.sq_supplier_surr_id'),
        src.supplier_id,
        COALESCE(src.supplier_name,            'n. a.'),
        src.supplier_email,
        src.supplier_number,
        src.supplier_primary_contact,
        src.supplier_location,
        NOW(), NOW(),
        'BL_3NF', 'CE_SUPPLIERS'
    );

    GET DIAGNOSTICS v_affected = ROW_COUNT;

    UPDATE bl_cn.etl_log
    SET status = 'SUCCESS', rows_inserted = v_affected, finished_at = NOW()
    WHERE log_id = v_log_id;

    RAISE NOTICE '[load_dm_suppliers] Rows affected: %', v_affected;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    UPDATE bl_cn.etl_log
    SET status = 'FAILED', error_message = v_err_msg, finished_at = NOW()
    WHERE log_id = v_log_id;
    RAISE EXCEPTION '[load_dm_suppliers] FAILED: %', v_err_msg;
END;
$$;

