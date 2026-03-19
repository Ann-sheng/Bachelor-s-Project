


--  INSERT-only load from BL_3NF.CE_SHIPPINGS


CREATE OR REPLACE PROCEDURE bl_dm.load_dm_shippings()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_dm_shippings', 'BL_DM');

    INSERT INTO bl_dm.dm_shippings (
        shipping_surr_id,
        shipping_src_id,
        shipping_method,
        shipping_carrier,
        ta_insert_dt, ta_update_dt,
        source_system, source_entity
    )
    SELECT
        nextval('bl_dm.sq_shipping_surr_id'),
        s.shipping_id,
        s.shipping_method,
        s.shipping_carrier,
        NOW(), NOW(),
        'BL_3NF', 'CE_SHIPPINGS'
    FROM bl_3nf.ce_shippings s
    WHERE s.shipping_id <> -1
      AND NOT EXISTS (
          SELECT 1 FROM bl_dm.dm_shippings d
          WHERE d.shipping_src_id = s.shipping_id
            AND d.source_system   = 'BL_3NF'
            AND d.source_entity   = 'CE_SHIPPINGS'
      );

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    UPDATE bl_cn.etl_log
    SET status = 'SUCCESS', rows_inserted = v_inserted, finished_at = NOW()
    WHERE log_id = v_log_id;

    RAISE NOTICE '[load_dm_shippings] Inserted: %', v_inserted;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    UPDATE bl_cn.etl_log
    SET status = 'FAILED', error_message = v_err_msg, finished_at = NOW()
    WHERE log_id = v_log_id;
    RAISE EXCEPTION '[load_dm_shippings] FAILED: %', v_err_msg;
END;
$$;
