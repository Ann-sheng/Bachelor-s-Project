
CREATE INDEX IF NOT EXISTS idx_reject_source ON stg_cln.reject_sales (source_system, rejected_at);

CREATE INDEX IF NOT EXISTS idx_reject_reason ON stg_cln.reject_sales USING GIN (raw_data);  
