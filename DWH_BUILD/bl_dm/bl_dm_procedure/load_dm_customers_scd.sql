CREATE OR REPLACE PROCEDURE bl_dm.load_dm_customers_scd()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_updated  INT := 0;
    v_rows     INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_dm_customers_scd', 'BL_DM');

    -- close versions in DM that were closed in 3NF
    UPDATE bl_dm.dm_customers_scd d
    SET    end_dt    = c.end_dt,
           is_active = FALSE
    FROM   bl_3nf.ce_customers_scd c
    WHERE  d.customer_src_id = c.customer_id      
      AND  d.source_system   = 'BL_3NF'
      AND  d.source_entity   = 'CE_CUSTOMERS_SCD'
      AND  c.is_active       = FALSE              
      AND  d.is_active       = TRUE;              

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_updated := v_updated + v_rows;

    -- insert new/changed versions not yet in DM
    INSERT INTO bl_dm.dm_customers_scd (
        customer_surr_id,
        customer_src_id,
        customer_firstname,
        customer_lastname,
        customer_country,
        customer_city,
        customer_phone_number,
        customer_email,
        start_dt,
        end_dt,
        is_active,
        ta_insert_dt,
        source_system,
        source_entity
    )
    SELECT
        nextval('bl_dm.sq_customer_surr_id'),
        c.customer_id,
        c.customer_firstname,
        c.customer_lastname,
        c.customer_country,
        c.customer_city,
        c.customer_phone_number,
        c.customer_email,
        c.start_dt,
        c.end_dt,
        c.is_active,
        NOW(),
        'BL_3NF',
        'CE_CUSTOMERS_SCD'
    FROM bl_3nf.ce_customers_scd c
    WHERE c.customer_id <> -1
      AND NOT EXISTS (
          SELECT 1 FROM bl_dm.dm_customers_scd d
          WHERE d.customer_src_id = c.customer_id
            AND d.source_system   = 'BL_3NF'
            AND d.source_entity   = 'CE_CUSTOMERS_SCD'
      );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_inserted := v_inserted + v_rows;

    CALL bl_cn.log_success(v_log_id, v_inserted, v_updated);

    RAISE NOTICE '[load_dm_customers_scd] Inserted: %, Updated: %', v_inserted, v_updated;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;




