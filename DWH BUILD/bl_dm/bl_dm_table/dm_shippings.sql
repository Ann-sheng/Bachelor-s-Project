

-- Shipping method/carrier dimension

CREATE TABLE IF NOT EXISTS bl_dm.dm_shippings (
    shipping_surr_id   BIGINT       PRIMARY KEY
                       DEFAULT nextval('bl_dm.sq_shipping_surr_id'),
    shipping_src_id    BIGINT       NOT NULL,  
    shipping_method    VARCHAR(100),
    shipping_carrier   VARCHAR(100),
    ta_insert_dt       TIMESTAMP    NOT NULL DEFAULT NOW(),
    ta_update_dt       TIMESTAMP    NOT NULL DEFAULT NOW(),
    source_system      VARCHAR(20)  NOT NULL DEFAULT 'BL_3NF',
    source_entity      VARCHAR(50)  NOT NULL DEFAULT 'CE_SHIPPINGS',

    CONSTRAINT uq_dm_shippings UNIQUE (shipping_src_id, source_system, source_entity)
);
