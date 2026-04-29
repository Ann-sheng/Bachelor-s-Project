

CREATE INDEX IF NOT EXISTS idx_dm_emp_active  ON bl_dm.dm_employees_scd (employee_src_id, is_active);

CREATE INDEX IF NOT EXISTS idx_dm_emp_active_only ON bl_dm.dm_employees_scd (employee_src_id)
    WHERE is_active = TRUE;


-- dm_employees_scd lookup during fact load  
CREATE INDEX IF NOT EXISTS idx_dm_employees_scd_lookup
    ON bl_dm.dm_employees_scd (employee_src_id, source_system, source_entity, start_dt, end_dt);


