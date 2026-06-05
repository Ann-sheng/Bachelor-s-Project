

-- Speeds up filtering and auditing by procedure execution history
CREATE INDEX IF NOT EXISTS idx_etl_log_procedure ON bl_cn.etl_log (procedure_name, started_at DESC);

-- Speeds up filtering logs by execution status (STARTED/SUCCESS/FAILED)
CREATE INDEX IF NOT EXISTS idx_etl_log_status ON bl_cn.etl_log (status, started_at DESC);

-- Speeds up lookup of all log entries for a specific ETL run
CREATE INDEX IF NOT EXISTS idx_etl_log_run ON bl_cn.etl_log (run_id, started_at DESC);

-- Speeds up filtering logs by ETL layer
CREATE INDEX IF NOT EXISTS idx_etl_log_layer ON bl_cn.etl_log (layer, started_at DESC);



