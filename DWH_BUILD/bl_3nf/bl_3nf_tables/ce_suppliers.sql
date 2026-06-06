
-- Supplier dimension entity - SCD Type 1

CREATE TABLE IF NOT EXISTS bl_3nf.ce_suppliers (
    supplier_id              BIGINT          PRIMARY KEY
                             DEFAULT nextval('bl_3nf.seq_ce_suppliers'),
    supplier_src_id          VARCHAR(15)     NOT NULL,
    supplier_name            VARCHAR(100)    NOT NULL,
    supplier_email           VARCHAR(100),
    supplier_number          VARCHAR(20),
    supplier_primary_contact VARCHAR(100),
    supplier_location        VARCHAR(100),
    ta_insert_dt             TIMESTAMP       NOT NULL DEFAULT NOW(),
    ta_update_dt             TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_system            VARCHAR(20)     NOT NULL,
    source_entity            VARCHAR(50)     NOT NULL,

    CONSTRAINT uq_ce_suppliers UNIQUE (supplier_src_id, source_system, source_entity)
);