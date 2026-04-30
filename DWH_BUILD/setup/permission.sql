-- PURPOSE : Grant schema/table/routine/sequence privileges to roles.



-- Lock down PUBLIC defaults

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL    ON SCHEMA public FROM PUBLIC;
REVOKE CONNECT ON DATABASE "dnd_sales" FROM PUBLIC;
REVOKE ALL ON SCHEMA src      FROM PUBLIC;
REVOKE ALL ON SCHEMA stg_cln  FROM PUBLIC;
REVOKE ALL ON SCHEMA bl_3nf   FROM PUBLIC;
REVOKE ALL ON SCHEMA bl_dm    FROM PUBLIC;
REVOKE ALL ON SCHEMA bl_cn    FROM PUBLIC;
REVOKE ALL ON SCHEMA sa_audit FROM PUBLIC;


-- dwh_admin: full access on all schemas
GRANT ALL ON SCHEMA src, stg_cln, bl_3nf, bl_dm, bl_cn, sa_audit TO dwh_admin;
GRANT CONNECT ON DATABASE "dnd_sales" TO dwh_admin;

-- dwh_etl: pipeline execution
GRANT USAGE   ON SCHEMA src, stg_cln, bl_3nf, bl_dm, bl_cn, sa_audit TO dwh_etl;
GRANT CONNECT ON DATABASE "dnd_sales" TO dwh_etl;

-- dwh_analyst: DM + monitoring views
GRANT USAGE   ON SCHEMA bl_dm, bl_cn TO dwh_analyst;
GRANT CONNECT ON DATABASE "dnd_sales" TO dwh_analyst;

-- dwh_reporter: DM only
GRANT USAGE   ON SCHEMA bl_dm TO dwh_reporter;
GRANT CONNECT ON DATABASE "dnd_sales" TO dwh_reporter;

-- Table-level grants on existing objects
GRANT SELECT, INSERT, UPDATE           ON ALL TABLES IN SCHEMA src      TO dwh_etl;
GRANT SELECT, INSERT, UPDATE           ON ALL TABLES IN SCHEMA sa_audit TO dwh_etl;
GRANT SELECT, INSERT, UPDATE, TRUNCATE, DELETE ON ALL TABLES IN SCHEMA stg_cln  TO dwh_etl;
GRANT SELECT, INSERT, UPDATE           ON ALL TABLES IN SCHEMA bl_3nf   TO dwh_etl;
GRANT SELECT, INSERT, UPDATE           ON ALL TABLES IN SCHEMA bl_dm    TO dwh_etl;
GRANT SELECT, INSERT, UPDATE           ON ALL TABLES IN SCHEMA  bl_cn TO dwh_etl;
GRANT SELECT, INSERT, UPDATE           ON ALL TABLES IN SCHEMA bl_cn TO dwh_etl;

-- Analyst: all DM tables
GRANT SELECT ON bl_dm.dm_suppliers,
                bl_dm.dm_shippings, 
                bl_dm.dm_store_branches,
                bl_dm.dm_products, 
                bl_dm.dm_customers_scd, 
                bl_dm.dm_employees_scd,
                bl_dm.dm_junk_transactions, 
                bl_dm.dim_dates,
                bl_dm.fct_transactions_dd 
            TO dwh_analyst;

-- Reporter: same
GRANT SELECT ON bl_dm.dm_suppliers, 
                bl_dm.dm_shippings, 
                bl_dm.dm_store_branches,
                bl_dm.dm_products, 
                bl_dm.dm_customers_scd, 
                bl_dm.dm_employees_scd,
                bl_dm.dm_junk_transactions, 
                bl_dm.dim_dates,
                bl_dm.fct_transactions_dd 
            TO dwh_reporter;





-- Salary masking view Analyst and reporter see NULL salary — base table access revoked after.

CREATE OR REPLACE VIEW bl_dm.v_employees_public AS
SELECT
    employee_surr_id, employee_src_id,
    employee_firstname, employee_lastname,
    employee_title, employee_email, employee_phone_number,
    NULL::NUMERIC(12,2) AS employee_salary, 
    start_dt, end_dt, is_active,
    ta_insert_dt, source_system, source_entity
FROM bl_dm.dm_employees_scd;

GRANT SELECT ON bl_dm.v_employees_public TO dwh_analyst;
GRANT SELECT ON bl_dm.v_employees_public TO dwh_reporter;


REVOKE SELECT ON bl_dm.dm_employees_scd FROM dwh_analyst;
REVOKE SELECT ON bl_dm.dm_employees_scd FROM dwh_reporter;

-- Sequence privileges
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bl_3nf TO dwh_etl;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bl_dm  TO dwh_etl;
GRANT ALL   ON ALL SEQUENCES IN SCHEMA bl_3nf, bl_dm, bl_cn TO dwh_admin;

-- Routine EXECUTE privileges
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA bl_3nf, bl_dm, bl_cn TO dwh_etl;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA bl_3nf, bl_dm, bl_cn TO dwh_admin;

-- Default privileges — future objects inherit the same pattern
ALTER DEFAULT PRIVILEGES IN SCHEMA src GRANT SELECT, INSERT, UPDATE ON TABLES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA src GRANT ALL ON TABLES TO dwh_admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA stg_cln GRANT SELECT, INSERT, UPDATE, TRUNCATE, DELETE ON TABLES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA stg_cln GRANT ALL ON TABLES TO dwh_admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT SELECT, INSERT, UPDATE ON TABLES    TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT USAGE                  ON SEQUENCES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT EXECUTE                ON ROUTINES  TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT ALL ON TABLES TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT ALL ON  SEQUENCES TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT ALL ON  ROUTINES TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT SELECT, INSERT, UPDATE ON TABLES    TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT SELECT                 ON TABLES    TO dwh_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT SELECT                 ON TABLES    TO dwh_reporter;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm  GRANT USAGE                  ON SEQUENCES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT EXECUTE                ON ROUTINES  TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT ALL ON TABLES TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT ALL ON  SEQUENCES TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT ALL ON  ROUTINES TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT SELECT, INSERT, UPDATE ON TABLES   TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT SELECT                 ON TABLES   TO dwh_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT EXECUTE                ON ROUTINES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT ALL ON TABLES TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT ALL ON  ROUTINES TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA sa_audit GRANT SELECT, INSERT, UPDATE ON TABLES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA sa_audit GRANT ALL ON TABLES TO dwh_admin;