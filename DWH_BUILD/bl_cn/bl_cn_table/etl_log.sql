
-- PURPOSE : Procedure-level execution log

CREATE TABLE IF NOT EXISTS bl_cn.etl_log (
    log_id           BIGINT      PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    run_id           BIGINT      REFERENCES bl_cn.etl_run(run_id),
    procedure_name   VARCHAR(100) NOT NULL,
    layer            VARCHAR(20)  NOT NULL,
    status           VARCHAR(20)  NOT NULL DEFAULT 'STARTED',
    rows_inserted    INTEGER,
    rows_updated     INTEGER,
    rows_skipped     INTEGER,
    started_at       TIMESTAMP    NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMP,
    duration_sec     NUMERIC(10,2) GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (finished_at - started_at))) STORED,
    error_message    TEXT,
    run_by           VARCHAR(50)  NOT NULL DEFAULT CURRENT_USER,

    CONSTRAINT chk_etl_log_status CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED')),
    CONSTRAINT chk_etl_log_layer CHECK (layer IN ('SA', 'STG_CLN', 'BL_3NF', 'BL_DM', 'BL_CN'))
);
