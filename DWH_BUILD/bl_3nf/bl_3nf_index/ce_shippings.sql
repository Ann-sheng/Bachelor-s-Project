
-- Fast lookup for active version by source ID
CREATE INDEX IF NOT EXISTS idx_ce_shippings_src ON bl_3nf.ce_shippings (shipping_src_id, source_system);
