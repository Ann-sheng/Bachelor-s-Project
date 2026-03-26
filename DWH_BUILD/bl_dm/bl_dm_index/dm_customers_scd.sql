

-- Fast lookup for active version
CREATE INDEX IF NOT EXISTS idx_dm_cust_active ON bl_dm.dm_customers_scd (customer_src_id, is_active);

-- Partial index: only active rows
CREATE INDEX IF NOT EXISTS idx_dm_cust_active_only ON bl_dm.dm_customers_scd (customer_src_id)
    WHERE is_active = TRUE;


