


CREATE INDEX IF NOT EXISTS idx_file_log_source ON sa_audit.file_load_log (source, load_type, status);

CREATE INDEX IF NOT EXISTS idx_file_log_started  ON sa_audit.file_load_log (started_at DESC);

