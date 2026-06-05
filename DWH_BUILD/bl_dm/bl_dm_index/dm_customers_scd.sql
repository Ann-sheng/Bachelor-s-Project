

-- Fast lookup for active version
CREATE INDEX IF NOT EXISTS idx_dm_cust_active ON bl_dm.dm_customers_scd (customer_src_id, is_active);

-- Index for filtering only active customer records
CREATE INDEX IF NOT EXISTS idx_dm_cust_active_only ON bl_dm.dm_customers_scd (customer_src_id)
    WHERE is_active = TRUE;

-- Supports fast joins during fact table load from SCD customer dimension
CREATE INDEX IF NOT EXISTS idx_dm_customers_scd_lookup
    ON bl_dm.dm_customers_scd (customer_src_id, source_system, source_entity, start_dt, end_dt);

