

--  Physical store branch dimension - SCD Type 1


CREATE TABLE IF NOT EXISTS bl_dm.dm_store_branches (
    store_branch_surr_id          BIGINT      PRIMARY KEY
                                  DEFAULT nextval('bl_dm.sq_store_branch_surr_id'),
    store_branch_src_id           BIGINT      NOT NULL,
    store_branch_state            VARCHAR(50),
    store_branch_city             VARCHAR(50),
    store_branch_phone_number     VARCHAR(20),
    store_branch_operating_days   VARCHAR(50),
    store_branch_operating_hours  VARCHAR(50),
    ta_insert_dt                  TIMESTAMP   NOT NULL DEFAULT NOW(),
    ta_update_dt                  TIMESTAMP   NOT NULL DEFAULT NOW(),
    source_system                 VARCHAR(20) NOT NULL DEFAULT 'BL_3NF',
    source_entity                 VARCHAR(50) NOT NULL DEFAULT 'CE_STORE_BRANCHES',

    CONSTRAINT uq_dm_store_branches UNIQUE (store_branch_src_id, source_system, source_entity)
);
