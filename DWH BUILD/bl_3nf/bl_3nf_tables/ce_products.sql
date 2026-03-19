
--  Product entity - SCD Type 1

CREATE TABLE IF NOT EXISTS bl_3nf.ce_products (
    product_id              BIGINT          PRIMARY KEY
                            DEFAULT nextval('bl_3nf.seq_ce_products'),
    product_src_id          VARCHAR(15)     NOT NULL,
    supplier_id             BIGINT          NOT NULL
                            REFERENCES bl_3nf.ce_suppliers(supplier_id),
    product_category        VARCHAR(100)    NOT NULL,
    product_name            VARCHAR(100)    NOT NULL,
    product_unit_cost       NUMERIC(12,2)   NOT NULL,
    product_unit_price      NUMERIC(12,2)   NOT NULL,
    product_warranty_period INTEGER, 
    ta_insert_dt            TIMESTAMP       NOT NULL DEFAULT NOW(),
    ta_update_dt            TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_system           VARCHAR(20)     NOT NULL,
    source_entity           VARCHAR(50)     NOT NULL,

    CONSTRAINT uq_ce_products UNIQUE (product_src_id, source_system, source_entity)
);