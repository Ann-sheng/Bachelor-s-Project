
-- Employee dimension - SCD Type 2


CREATE TABLE IF NOT EXISTS bl_dm.dm_employees_scd (
    employee_surr_id      BIGINT          PRIMARY KEY
                          DEFAULT nextval('bl_dm.sq_employee_surr_id'),
    employee_src_id       BIGINT          NOT NULL,  
    employee_firstname    VARCHAR(50)     NOT NULL,
    employee_lastname     VARCHAR(50)     NOT NULL,
    employee_title        VARCHAR(50),
    employee_email        VARCHAR(100),
    employee_phone_number VARCHAR(20),
    employee_salary       NUMERIC(12,2),  
    store_branch_state    VARCHAR(50)     NOT NULL DEFAULT 'n. a.',
    store_branch_city     VARCHAR(50)     NOT NULL DEFAULT 'n. a.',    
    start_dt              DATE            NOT NULL DEFAULT CURRENT_DATE,
    end_dt                DATE            NOT NULL DEFAULT '9999-12-31',
    is_active             BOOLEAN         NOT NULL DEFAULT TRUE,
    ta_insert_dt          TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_system         VARCHAR(20)     NOT NULL DEFAULT 'BL_3NF',
    source_entity         VARCHAR(50)     NOT NULL DEFAULT 'CE_EMPLOYEES_SCD',

    CONSTRAINT uq_dm_employees_src UNIQUE (employee_src_id, source_system, source_entity)
);

