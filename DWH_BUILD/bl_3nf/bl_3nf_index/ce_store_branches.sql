
-- Fast lookup for active version by source ID
CREATE INDEX IF NOT EXISTS idx_ce_branches_src ON bl_3nf.ce_store_branches (store_branch_src_id, source_system);

