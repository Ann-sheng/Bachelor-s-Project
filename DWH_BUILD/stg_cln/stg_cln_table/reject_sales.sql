
-- Unified reject table for ALL sources, Any record failing validation lands here

CREATE TABLE IF NOT EXISTS stg_cln.reject_sales (
    reject_id       BIGSERIAL       PRIMARY KEY,
    source_system   VARCHAR(10)     NOT NULL,   
    source_table    VARCHAR(50)     NOT NULL,  
    reject_reason   TEXT            NOT NULL,   
    raw_data        JSONB           NOT NULL,  
    rejected_at     TIMESTAMP       DEFAULT NOW()
);

