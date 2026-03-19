
--  SCD1 load from BL_3NF.CE_PRODUCTS


CREATE OR REPLACE PROCEDURE bl_dm.load_dm_products()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_affected INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_dm_products', 'BL_DM');

    MERGE INTO bl_dm.dm_products tgt
    USING (
        SELECT
            p.product_id,
            COALESCE(ds.supplier_surr_id, -1) AS supplier_surr_id,  
            p.product_category,
            p.product_name,
            p.product_unit_cost,
            p.product_unit_price,
            p.product_warranty_period
        FROM bl_3nf.ce_products p
        LEFT JOIN bl_dm.dm_suppliers ds
               ON ds.supplier_src_id = p.supplier_id     
              AND ds.source_system    = 'BL_3NF'
              AND ds.source_entity    = 'CE_SUPPLIERS'
        WHERE p.product_id <> -1
    ) src
    ON (
        tgt.product_src_id  = src.product_id
        AND tgt.source_system = 'BL_3NF'
        AND tgt.source_entity = 'CE_PRODUCTS'
    )
    WHEN MATCHED AND (
        tgt.supplier_surr_id        IS DISTINCT FROM src.supplier_surr_id        OR
        tgt.product_category        IS DISTINCT FROM src.product_category        OR
        tgt.product_name            IS DISTINCT FROM src.product_name            OR
        tgt.product_unit_cost       IS DISTINCT FROM src.product_unit_cost       OR
        tgt.product_unit_price      IS DISTINCT FROM src.product_unit_price      OR
        tgt.product_warranty_period IS DISTINCT FROM src.product_warranty_period
    ) THEN UPDATE SET
        supplier_surr_id        = src.supplier_surr_id,
        product_category        = src.product_category,
        product_name            = src.product_name,
        product_unit_cost       = src.product_unit_cost,
        product_unit_price      = src.product_unit_price,
        product_warranty_period = src.product_warranty_period,
        ta_update_dt            = NOW()

    WHEN NOT MATCHED THEN INSERT (
        product_surr_id,
        product_src_id,
        supplier_surr_id,
        product_category,
        product_name,
        product_unit_cost,
        product_unit_price,
        product_warranty_period,
        ta_insert_dt, ta_update_dt,
        source_system, source_entity
    ) VALUES (
        nextval('bl_dm.sq_product_surr_id'),
        src.product_id,
        src.supplier_surr_id,
        COALESCE(src.product_category, 'n. a.'),
        COALESCE(src.product_name,     'n. a.'),
        COALESCE(src.product_unit_cost,  0),
        COALESCE(src.product_unit_price, 0),
        src.product_warranty_period,       
        NOW(), NOW(),
        'BL_3NF', 'CE_PRODUCTS'
    );

    GET DIAGNOSTICS v_affected = ROW_COUNT;

    UPDATE bl_cn.etl_log
    SET status = 'SUCCESS', rows_inserted = v_affected, finished_at = NOW()
    WHERE log_id = v_log_id;

    RAISE NOTICE '[load_dm_products] Rows affected: %', v_affected;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    UPDATE bl_cn.etl_log
    SET status = 'FAILED', error_message = v_err_msg, finished_at = NOW()
    WHERE log_id = v_log_id;
    RAISE EXCEPTION '[load_dm_products] FAILED: %', v_err_msg;
END;
$$;

