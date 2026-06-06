
-- Helps fast lookup of products by source system identifier
CREATE INDEX IF NOT EXISTS idx_dm_products_src ON bl_dm.dm_products (product_src_id);

