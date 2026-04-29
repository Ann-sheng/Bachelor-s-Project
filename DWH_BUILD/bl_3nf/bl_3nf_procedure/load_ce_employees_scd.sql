
--  SCD Type 2 load for CE_EMPLOYEES_SCD


CREATE OR REPLACE PROCEDURE bl_3nf.load_ce_employees_scd()
LANGUAGE plpgsql AS $$
DECLARE
    rec        RECORD;
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_updated  INT := 0;
    v_rows     INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_3nf.load_ce_employees_scd', 'BL_3NF');

    FOR rec IN
        SELECT * FROM bl_3nf.fn_get_new_employees_scd()
    LOOP
        UPDATE bl_3nf.ce_employees_scd
        SET    end_dt    = CURRENT_DATE,
               is_active = FALSE
        WHERE  employee_src_id = rec.employee_src_id
          AND  source_system   = rec.source_system
          AND  source_entity   = rec.source_entity
          AND  is_active       = TRUE;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_updated := v_updated + v_rows;

        INSERT INTO bl_3nf.ce_employees_scd (
            employee_id,
            employee_src_id,
            store_branch_id,
            employee_firstname,
            employee_lastname,
            employee_title,
            employee_email,
            employee_phone_number,
            employee_salary,
            start_dt,
            end_dt,
            is_active,
            employee_row_hash,
            ta_insert_dt,
            source_system,
            source_entity
        ) VALUES (
            nextval('bl_3nf.seq_ce_employees_scd'),  
            rec.employee_src_id,
            rec.store_branch_id,
            rec.employee_firstname,
            rec.employee_lastname,
            rec.employee_title,
            rec.employee_email,
            rec.employee_phone_number,
            rec.employee_salary,
            CURRENT_DATE,
            '9999-12-31',
            TRUE,
            rec.employee_row_hash,
            NOW(),
            rec.source_system,
            rec.source_entity
        )
        ON CONFLICT (employee_src_id, source_system, source_entity, start_dt) 
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

