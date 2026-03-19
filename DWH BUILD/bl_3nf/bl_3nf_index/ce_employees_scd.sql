
-- Fast lookup for active version by source ID
CREATE INDEX IF NOT EXISTS idx_ce_employees_src_active ON bl_3nf.ce_employees_scd (employee_src_id, source_system, is_active);

-- Fast hash comparison during change detection
CREATE INDEX IF NOT EXISTS idx_ce_employees_hash ON bl_3nf.ce_employees_scd (row_hash);

-- Fast lookup  by store ID
CREATE INDEX IF NOT EXISTS idx_ce_employees_branch ON bl_3nf.ce_employees_scd (store_branch_id);

-- Date range queries (SCD2 point-in-time lookups)
CREATE INDEX IF NOT EXISTS idx_ce_employees_dates ON bl_3nf.ce_employees_scd (start_dt, end_dt);




