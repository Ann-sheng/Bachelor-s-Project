
-- Speeds up filtering and monitoring ETL runs by status and start time
CREATE INDEX IF NOT EXISTS idx_etl_run_status ON bl_cn.etl_run (run_status, run_start DESC);




