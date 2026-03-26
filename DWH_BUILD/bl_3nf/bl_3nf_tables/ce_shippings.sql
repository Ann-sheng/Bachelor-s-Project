
--  Shipping method/carrier entity - SCD Type 1

CREATE TABLE IF NOT EXISTS bl_3nf.ce_shippings (
    shipping_id              BIGINT          PRIMARY KEY
                             DEFAULT nextval('bl_3nf.seq_ce_shippings'),
    shipping_src_id          VARCHAR(15)     NOT NULL,
    shipping_method          VARCHAR(100),
    shipping_carrier         VARCHAR(100),
    ta_insert_dt             TIMESTAMP       NOT NULL DEFAULT NOW(),
    ta_update_dt             TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_system            VARCHAR(20)     NOT NULL,
    source_entity            VARCHAR(50)     NOT NULL,

    CONSTRAINT uq_ce_shippings UNIQUE (shipping_src_id, source_system, source_entity)
);