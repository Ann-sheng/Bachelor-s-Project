CREATE OR REPLACE PROCEDURE bl_dm.load_dm_employees_scd()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_updated  INT := 0;
    v_rows     INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_dm_employees_scd', 'BL_DM');

    -- Step 1: close versions in DM that were closed in 3NF
    UPDATE bl_dm.dm_employees_scd d
    SET    end_dt    = e.end_dt,
           is_active = FALSE
    FROM   bl_3nf.ce_employees_scd e
    WHERE  d.employee_src_id = e.employee_id
      AND  d.source_system   = 'BL_3NF'
      AND  d.source_entity   = 'CE_EMPLOYEES_SCD'
      AND  e.is_active       = FALSE
      AND  d.is_active       = TRUE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_updated := v_updated + v_rows;

    -- Step 2: insert new/changed versions not yet in DM
    INSERT INTO bl_dm.dm_employees_scd (
        employee_surr_id,
        employee_src_id,
        employee_firstname,
        employee_lastname,
        employee_title,
        employee_email,
        employee_phone_number,
        employee_salary,
        store_branch_state, 
        store_branch_city,
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
        COALESCE(sb.store_branch_state, 'n. a.'),  
        COALESCE(sb.store_branch_city,  'n. a.'),  
        e.start_dt,
        e.end_dt,
        e.is_active,
        NOW(),
        'BL_3NF',
        'CE_EMPLOYEES_SCD'
    FROM bl_3nf.ce_employees_scd e
    LEFT JOIN bl_3nf.ce_store_branches sb     
           ON sb.store_branch_id = e.store_branch_id
    WHERE e.employee_id <> -1
    WHERE e.employee_id <> -1
      AND NOT EXISTS (
          SELECT 1 FROM bl_dm.dm_employees_scd d
          WHERE d.employee_src_id = e.employee_id
            AND d.source_system   = 'BL_3NF'
            AND d.source_entity   = 'CE_EMPLOYEES_SCD'
      );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_inserted := v_inserted + v_rows;

    CALL bl_cn.log_success(v_log_id, v_inserted, v_updated);

    RAISE NOTICE '[load_dm_employees_scd] Inserted: %, Updated: %', v_inserted, v_updated;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;