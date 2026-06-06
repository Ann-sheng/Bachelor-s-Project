

-- Fast lookup for active version by source ID
CREATE INDEX IF NOT EXISTS idx_ce_customers_src_active  ON bl_3nf.ce_customers_scd (customer_src_id, source_system, is_active);

-- Fast hash comparison during change detection
CREATE INDEX IF NOT EXISTS idx_ce_customers_hash  ON bl_3nf.ce_customers_scd (customer_row_hash);

-- Date range queries (SCD2 point-in-time lookups)
CREATE INDEX IF NOT EXISTS idx_ce_customers_dates  ON bl_3nf.ce_customers_scd (start_dt, end_dt);

SELECT column_name FROM information_schema.columns WHERE table_name = 'ce_customers_scd' AND column_name = 'customer_row_hash';