
-- Fast lookup for active version by source ID
CREATE INDEX IF NOT EXISTS idx_ce_suppliers_src ON bl_3nf.ce_suppliers (supplier_src_id, source_system);