-- SCD Type 1 load for CE_SUPPLIERS using MERGE
-- Updates existing supplier records and inserts new ones from source data

CREATE OR REPLACE PROCEDURE bl_3nf.load_ce_suppliers()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id      BIGINT;
    v_affected    INT := 0;
    v_err_msg     TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_3nf.load_ce_suppliers', 'BL_3NF');

    -- Merge supplier source data into target table
    MERGE INTO bl_3nf.ce_suppliers tgt
    USING bl_3nf.fn_get_suppliers_data() src
    ON (
        tgt.supplier_src_id = src.supplier_src_id
        AND tgt.source_system   = src.source_system
        AND tgt.source_entity   = src.source_entity
    )

    -- Update existing records if any supplier attributes changed
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

    -- Insert new supplier records
    WHEN NOT MATCHED THEN INSERT (
        supplier_id,
        supplier_src_id,
        supplier_name,
        supplier_email,
        supplier_number,
        supplier_primary_contact,
        supplier_location,
        ta_insert_dt,
        ta_update_dt,
        source_system,
        source_entity
    ) VALUES (
        nextval('bl_3nf.seq_ce_suppliers'),
        src.supplier_src_id,
        src.supplier_name,
        src.supplier_email,
        src.supplier_number,
        src.supplier_primary_contact,
        src.supplier_location,
        NOW(), NOW(),
        src.source_system,
        src.source_entity
    );

    GET DIAGNOSTICS v_affected = ROW_COUNT;

    -- Log successful execution
    CALL bl_cn.log_success(v_log_id, v_affected);

EXCEPTION WHEN OTHERS THEN
    -- Log failure and rethrow error
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;