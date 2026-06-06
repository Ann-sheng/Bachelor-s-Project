


--  Product dimension - SCD Type 1


CREATE TABLE IF NOT EXISTS bl_dm.dm_products (
    product_surr_id          BIGINT          PRIMARY KEY
                             DEFAULT nextval('bl_dm.sq_product_surr_id'),
    product_src_id           BIGINT          NOT NULL,  
    product_category         VARCHAR(100)    NOT NULL,
    product_name             VARCHAR(200)    NOT NULL,
    product_unit_cost        NUMERIC(12,2)   NOT NULL,
    product_unit_price       NUMERIC(12,2)   NOT NULL,
    product_warranty_period  INTEGER,                    
    ta_insert_dt             TIMESTAMP       NOT NULL DEFAULT NOW(),
    ta_update_dt             TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_system            VARCHAR(20)     NOT NULL DEFAULT 'BL_3NF',
    source_entity            VARCHAR(50)     NOT NULL DEFAULT 'CE_PRODUCTS',

    CONSTRAINT uq_dm_products UNIQUE (product_src_id, source_system, source_entity)
);

