

--  SCD Type 2 load for CE_CUSTOMERS_SCD


CREATE OR REPLACE PROCEDURE bl_3nf.load_ce_customers_scd()
LANGUAGE plpgsql AS $$
DECLARE
    rec          RECORD;
    v_log_id     BIGINT;
    v_inserted   INT := 0;
    v_updated    INT := 0;
    v_rows       INT := 0;
    v_err_msg    TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_3nf.load_ce_customers_scd', 'BL_3NF');

    FOR rec IN
        SELECT * FROM bl_3nf.fn_get_new_customers_scd()
    LOOP
        UPDATE bl_3nf.ce_customers_scd
        SET    end_dt    = CURRENT_DATE, 
               is_active = FALSE
        WHERE  customer_src_id = rec.customer_src_id
          AND  source_system   = rec.source_system
          AND  source_entity   = rec.source_entity
          AND  is_active       = TRUE;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_updated := v_updated + v_rows;

        INSERT INTO bl_3nf.ce_customers_scd (
            customer_id,
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
            customer_row_hash,
            ta_insert_dt,
            source_system,
            source_entity
        ) VALUES (
            nextval('bl_3nf.seq_ce_customers_scd'), 
            rec.customer_src_id,
            rec.customer_firstname,
            rec.customer_lastname,
            rec.customer_country,
            rec.customer_city,
            rec.customer_phone_number,
            rec.customer_email,
            CASE 
                WHEN v_rows = 0 THEN DATE '1900-01-01'  
                ELSE CURRENT_DATE                      
            END,
            '9999-12-31',
            TRUE,
            rec.customer_row_hash,
            NOW(),
            rec.source_system,
            rec.source_entity
        )
        ON CONFLICT (customer_src_id, source_system, source_entity, start_dt) 
        DO NOTHING;

        v_inserted := v_inserted + 1;
    END LOOP;

     CALL bl_cn.log_success(v_log_id, v_inserted, v_updated);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;

