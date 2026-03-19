
--  Pipeline-level run tracking


CREATE TABLE IF NOT EXISTS bl_cn.etl_run (
    run_id       BIGINT          PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pipeline     VARCHAR(100)    NOT NULL,  
    run_status   VARCHAR(20)     NOT NULL DEFAULT 'RUNNING',
    run_start    TIMESTAMP       NOT NULL DEFAULT NOW(),
    run_end      TIMESTAMP,
    run_duration_sec NUMERIC(10,2) GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (run_end - run_start))) STORED,
    run_by       VARCHAR(50)     NOT NULL DEFAULT CURRENT_USER,
    notes        TEXT,

    CONSTRAINT chk_etl_run_status CHECK (run_status IN ('RUNNING', 'SUCCESS', 'FAILED'))
);

