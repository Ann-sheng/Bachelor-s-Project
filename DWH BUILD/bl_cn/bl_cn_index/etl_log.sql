


CREATE INDEX IF NOT EXISTS idx_etl_log_procedure ON bl_cn.etl_log (procedure_name, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_log_status ON bl_cn.etl_log (status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_log_run ON bl_cn.etl_log (run_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_log_layer ON bl_cn.etl_log (layer, started_at DESC);



