
-- Physical store branch entity - SCD Type 1

CREATE TABLE IF NOT EXISTS bl_3nf.ce_store_branches (
    store_branch_id              BIGINT      PRIMARY KEY
                                 DEFAULT nextval('bl_3nf.seq_ce_store_branches'),
    store_branch_src_id          VARCHAR(15) NOT NULL,
    store_branch_state           VARCHAR(50),
    store_branch_city            VARCHAR(50),
    store_branch_phone_number    VARCHAR(20),
    store_branch_operating_days  VARCHAR(50),
    store_branch_operating_hours VARCHAR(50),
    ta_insert_dt                 TIMESTAMP   NOT NULL DEFAULT NOW(),
    ta_update_dt                 TIMESTAMP   NOT NULL DEFAULT NOW(),
    source_system                VARCHAR(20) NOT NULL,
    source_entity                VARCHAR(50) NOT NULL,

    CONSTRAINT uq_ce_store_branches UNIQUE (store_branch_src_id, source_system, source_entity)
);
