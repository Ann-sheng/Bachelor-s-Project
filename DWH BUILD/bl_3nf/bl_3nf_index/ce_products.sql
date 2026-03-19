
-- Fast lookup for active version by source ID
CREATE INDEX IF NOT EXISTS idx_ce_products_src ON bl_3nf.ce_products (product_src_id, source_system);

-- Fast lookup  by supplier ID
CREATE INDEX IF NOT EXISTS idx_ce_products_supplier ON bl_3nf.ce_products (supplier_id);

