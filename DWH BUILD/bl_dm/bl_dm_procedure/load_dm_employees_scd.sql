
--  SCD2 mirror load from BL_3NF.CE_EMPLOYEES_SCD


CREATE OR REPLACE PROCEDURE bl_dm.load_dm_employees_scd()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_dm_employees_scd', 'BL_DM');

    INSERT INTO bl_dm.dm_employees_scd (
        employee_surr_id,
        employee_src_id,
        employee_firstname,
        employee_lastname,
        employee_title,
        employee_email,
        employee_phone_number,
        employee_salary,
        start_dt,
        end_dt,
        is_active,
        ta_insert_dt,
        source_system,
        source_entity
    )
    SELECT
        nextval('bl_dm.sq_employee_surr_id'),
        e.employee_id,          
        e.employee_firstname,
        e.employee_lastname,
        e.employee_title,
        e.employee_email,
        e.employee_phone_number,
        e.employee_salary,     
        e.start_dt,
        e.end_dt,
        e.is_active,
        NOW(),
        'BL_3NF',
        'CE_EMPLOYEES_SCD'
    FROM bl_3nf.ce_employees_scd e
    WHERE e.employee_id <> -1
      AND NOT EXISTS (
          SELECT 1 FROM bl_dm.dm_employees_scd d
          WHERE d.employee_src_id = e.employee_id
            AND d.source_system   = 'BL_3NF'
            AND d.source_entity   = 'CE_EMPLOYEES_SCD'
      );

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    UPDATE bl_cn.etl_log
    SET status = 'SUCCESS', rows_inserted = v_inserted, finished_at = NOW()
    WHERE log_id = v_log_id;

    RAISE NOTICE '[load_dm_employees_scd] Inserted: % new version rows', v_inserted;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    UPDATE bl_cn.etl_log
    SET status = 'FAILED', error_message = v_err_msg, finished_at = NOW()
    WHERE log_id = v_log_id;
    RAISE EXCEPTION '[load_dm_employees_scd] FAILED: %', v_err_msg;
END;
$$;

