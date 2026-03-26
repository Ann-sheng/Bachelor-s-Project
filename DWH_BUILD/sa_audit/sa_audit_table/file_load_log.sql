-- Audit schema for tracking which Parquet files have  been loaded into staging tables.


CREATE TABLE IF NOT EXISTS sa_audit.file_load_log (
    log_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_key        VARCHAR(500)    NOT NULL,
    source          VARCHAR(20)     NOT NULL,
    load_type       VARCHAR(20)     NOT NULL,
    batch_id        VARCHAR(50),
    file_size_bytes BIGINT,
    rows_loaded     INTEGER,
    started_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMP,
    duration_sec    NUMERIC(10,2)
        GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (finished_at - started_at))) STORED,
    status          VARCHAR(20)     NOT NULL DEFAULT 'STARTED',
    error_message   TEXT
);

