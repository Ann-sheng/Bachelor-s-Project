
--  Supplier dimension - SCD Type 1 (changes overwrite)

CREATE TABLE IF NOT EXISTS bl_dm.dm_suppliers (
    supplier_surr_id         BIGINT          PRIMARY KEY
                             DEFAULT nextval('bl_dm.sq_supplier_surr_id'),
    supplier_src_id          BIGINT          NOT NULL, 
    supplier_name            VARCHAR(100)    NOT NULL,
    supplier_email           VARCHAR(100),
    supplier_number          VARCHAR(20),
    supplier_primary_contact VARCHAR(100),
    supplier_location        VARCHAR(100),
    ta_insert_dt             TIMESTAMP       NOT NULL DEFAULT NOW(),
    ta_update_dt             TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_system            VARCHAR(20)     NOT NULL DEFAULT 'BL_3NF',
    source_entity            VARCHAR(50)     NOT NULL DEFAULT 'CE_SUPPLIERS',

    CONSTRAINT uq_dm_suppliers UNIQUE (supplier_src_id, source_system, source_entity)
);
