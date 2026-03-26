
-- PURPOSE : Customer entity - SCD Type 2

CREATE TABLE IF NOT EXISTS bl_3nf.ce_customers_scd (
    customer_id           BIGINT      PRIMARY KEY
                          DEFAULT nextval('bl_3nf.seq_ce_customers_scd'),
    customer_src_id       VARCHAR(15) NOT NULL,
    customer_firstname    VARCHAR(50) NOT NULL,
    customer_lastname     VARCHAR(50) NOT NULL,
    customer_country      VARCHAR(50) NOT NULL DEFAULT 'n. a.',
    customer_city         VARCHAR(50) NOT NULL DEFAULT 'n. a.',
    customer_phone_number VARCHAR(20),
    customer_email        VARCHAR(100),
    start_dt              DATE        NOT NULL DEFAULT CURRENT_DATE,
    end_dt                DATE        NOT NULL DEFAULT '9999-12-31',
    is_active             BOOLEAN     NOT NULL DEFAULT TRUE,
    row_hash              VARCHAR(32) NOT NULL DEFAULT 'n. a.',
    ta_insert_dt          TIMESTAMP   NOT NULL DEFAULT NOW(),
    source_system         VARCHAR(20) NOT NULL,
    source_entity         VARCHAR(50) NOT NULL,
    CONSTRAINT uq_ce_customers_version UNIQUE (customer_src_id, source_system, source_entity, start_dt)
);
