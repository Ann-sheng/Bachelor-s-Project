
--  INSERT-only load for CE_SHIPPINGS (no updates)

CREATE OR REPLACE PROCEDURE bl_3nf.load_ce_shippings()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_err_msg  TEXT;
BEGIN
       v_log_id := bl_cn.log_start('bl_3nf.load_ce_shippings', 'BL_3NF');

    INSERT INTO bl_3nf.ce_shippings (
        shipping_id,
        shipping_src_id,
        shipping_method,
        shipping_carrier,
        ta_insert_dt,
        ta_update_dt,
        source_system,
        source_entity
    )
    SELECT
        nextval('bl_3nf.seq_ce_shippings'),
        src.shipping_src_id,
        src.shipping_method,
        src.shipping_carrier,
        NOW(), NOW(),
        src.source_system,
        src.source_entity
    FROM bl_3nf.fn_get_shippings_data() src
    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_shippings tgt
        WHERE tgt.shipping_src_id = src.shipping_src_id
          AND tgt.source_system   = src.source_system
          AND tgt.source_entity   = src.source_entity
    );

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    CALL bl_cn.log_success(v_log_id, v_affected);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;
