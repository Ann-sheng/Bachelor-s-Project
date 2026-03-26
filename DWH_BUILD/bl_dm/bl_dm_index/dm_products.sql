

CREATE INDEX IF NOT EXISTS idx_dm_products_src ON bl_dm.dm_products (product_src_id);

CREATE INDEX IF NOT EXISTS idx_dm_products_supplier ON bl_dm.dm_products (supplier_surr_id);

