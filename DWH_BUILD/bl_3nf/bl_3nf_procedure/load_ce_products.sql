

--  SCD Type 1 load for CE_PRODUCTS via MERGE


CREATE OR REPLACE PROCEDURE bl_3nf.load_ce_products()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_affected INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_3nf.load_ce_products', 'BL_3NF');

    MERGE INTO bl_3nf.ce_products tgt
    USING bl_3nf.fn_get_products_data() src
    ON (
        tgt.product_src_id = src.product_src_id
        AND tgt.source_system  = src.source_system
        AND tgt.source_entity  = src.source_entity
    )
    WHEN MATCHED AND (
        tgt.supplier_id             IS DISTINCT FROM src.supplier_id             OR
        tgt.product_category        IS DISTINCT FROM src.product_category        OR
        tgt.product_name            IS DISTINCT FROM src.product_name            OR
        tgt.product_unit_cost       IS DISTINCT FROM src.product_unit_cost       OR
        tgt.product_unit_price      IS DISTINCT FROM src.product_unit_price      OR
        tgt.product_warranty_period IS DISTINCT FROM src.product_warranty_period
    ) THEN UPDATE SET
        supplier_id             = src.supplier_id,
        product_category        = src.product_category,
        product_name            = src.product_name,
        product_unit_cost       = src.product_unit_cost,
        product_unit_price      = src.product_unit_price,
        product_warranty_period = src.product_warranty_period,
        ta_update_dt            = NOW()

    WHEN NOT MATCHED THEN INSERT (
        product_id,
        product_src_id,
        supplier_id,
        product_category,
        product_name,
        product_unit_cost,
        product_unit_price,
        product_warranty_period,
        ta_insert_dt,
        ta_update_dt,
        source_system,
        source_entity
    ) VALUES (
        nextval('bl_3nf.seq_ce_products'),
        src.product_src_id,
        src.supplier_id,
        src.product_category,
        src.product_name,
        src.product_unit_cost,
        src.product_unit_price,
        src.product_warranty_period,
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


