
--  SCD2 mirror load from BL_3NF.CE_CUSTOMERS_SCD


CREATE OR REPLACE PROCEDURE bl_dm.load_dm_customers_scd()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_dm_customers_scd', 'BL_DM');


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

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    UPDATE bl_cn.etl_log
    SET status = 'SUCCESS', rows_inserted = v_inserted, finished_at = NOW()
    WHERE log_id = v_log_id;

    RAISE NOTICE '[load_dm_customers_scd] Inserted: % new version rows', v_inserted;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    UPDATE bl_cn.etl_log
    SET status = 'FAILED', error_message = v_err_msg, finished_at = NOW()
    WHERE log_id = v_log_id;
    RAISE EXCEPTION '[load_dm_customers_scd] FAILED: %', v_err_msg;
END;
$$;
