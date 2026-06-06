


-- PURPOSE : Customer dimension - SCD Type 2


CREATE TABLE IF NOT EXISTS bl_dm.dm_customers_scd (
    customer_surr_id      BIGINT       PRIMARY KEY
                          DEFAULT nextval('bl_dm.sq_customer_surr_id'),
    customer_src_id       BIGINT       NOT NULL,  
    customer_firstname    VARCHAR(50)  NOT NULL,
    customer_lastname     VARCHAR(50)  NOT NULL,
    customer_country      VARCHAR(50)  NOT NULL DEFAULT 'n. a.',
    customer_city         VARCHAR(50)  NOT NULL DEFAULT 'n. a.',
    customer_phone_number VARCHAR(20),
    customer_email        VARCHAR(100),
    start_dt              DATE         NOT NULL DEFAULT CURRENT_DATE,
    end_dt                DATE         NOT NULL DEFAULT '9999-12-31',
    is_active             BOOLEAN      NOT NULL DEFAULT TRUE,
    ta_insert_dt          TIMESTAMP    NOT NULL DEFAULT NOW(),
    source_system         VARCHAR(20)  NOT NULL DEFAULT 'BL_3NF',
    source_entity         VARCHAR(50)  NOT NULL DEFAULT 'CE_CUSTOMERS_SCD',

    CONSTRAINT uq_dm_customers_src UNIQUE (customer_src_id, source_system, source_entity)
);
