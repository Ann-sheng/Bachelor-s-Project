-- Revoke PUBLIC defaults 

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

REVOKE CONNECT ON DATABASE "D&D_SALES" FROM PUBLIC;

REVOKE ALL ON SCHEMA ext_source FROM PUBLIC;
REVOKE ALL ON SCHEMA src FROM PUBLIC;
REVOKE ALL ON SCHEMA stg_cln FROM PUBLIC;
REVOKE ALL ON SCHEMA bl_3nf FROM PUBLIC;
REVOKE ALL ON SCHEMA bl_dm FROM PUBLIC;
REVOKE ALL ON SCHEMA bl_cn FROM PUBLIC;



-- dwh_admin: full access on everything
GRANT ALL ON SCHEMA ext_source TO dwh_admin;
GRANT ALL ON SCHEMA src TO dwh_admin;
GRANT ALL ON SCHEMA stg_cln TO dwh_admin;
GRANT ALL ON SCHEMA bl_3nf TO dwh_admin;
GRANT ALL ON SCHEMA bl_dm TO dwh_admin;
GRANT ALL ON SCHEMA bl_cn TO dwh_admin;

GRANT CONNECT ON DATABASE "D&D_SALES" TO dwh_admin;

-- dwh_etl: full pipeline execution
GRANT USAGE ON SCHEMA ext_source TO dwh_etl;
GRANT USAGE ON SCHEMA src TO dwh_etl;
GRANT USAGE ON SCHEMA stg_cln TO dwh_etl;
GRANT USAGE ON SCHEMA bl_3nf TO dwh_etl;
GRANT USAGE ON SCHEMA bl_dm TO dwh_etl;
GRANT USAGE ON SCHEMA bl_cn TO dwh_etl;

GRANT CONNECT ON DATABASE "D&D_SALES" TO dwh_etl;

-- dwh_analyst: DM + monitoring only
GRANT USAGE ON SCHEMA bl_dm TO dwh_analyst;
GRANT USAGE ON SCHEMA bl_cn TO dwh_analyst;

GRANT CONNECT ON DATABASE "D&D_SALES" TO dwh_analyst;

-- dwh_reporter: DM only
GRANT USAGE ON SCHEMA bl_dm TO dwh_reporter;

GRANT CONNECT ON DATABASE "D&D_SALES" TO dwh_reporter;

--EXT schema: ETL reads only 
GRANT SELECT ON ALL TABLES IN SCHEMA ext_source TO dwh_etl;

--SRC schema: ETL reads + writes; 
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA src TO dwh_etl;

--STG_CLN schema: ETL reads + writes; 
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA stg_cln TO dwh_etl;

--BL_3NF: ETL reads + writes; nobody else
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bl_3nf TO dwh_etl;


--BL_DM: ETL writes; Analyst + Reporter read
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bl_dm TO dwh_etl;


-- Analyst: read all DM dimension + fact tables
GRANT SELECT ON bl_dm.dm_suppliers TO dwh_analyst;
GRANT SELECT ON bl_dm.dm_shippings TO dwh_analyst;
GRANT SELECT ON bl_dm.dm_store_branches TO dwh_analyst;
GRANT SELECT ON bl_dm.dm_products TO dwh_analyst;
GRANT SELECT ON bl_dm.dm_customers_scd  TO dwh_analyst;
GRANT SELECT ON bl_dm.dm_junk_transactions TO dwh_analyst;
GRANT SELECT ON bl_dm.dim_dates TO dwh_analyst;
GRANT SELECT ON bl_dm.fct_transactions_dd TO dwh_analyst;

-- Reporter: read DM tables
GRANT SELECT ON bl_dm.dm_suppliers TO dwh_reporter;
GRANT SELECT ON bl_dm.dm_shippings TO dwh_reporter;
GRANT SELECT ON bl_dm.dm_store_branches TO dwh_reporter;
GRANT SELECT ON bl_dm.dm_products TO dwh_reporter;
GRANT SELECT ON bl_dm.dm_customers_scd TO dwh_reporter;
GRANT SELECT ON bl_dm.dm_junk_transactions TO dwh_reporter;
GRANT SELECT ON bl_dm.dim_dates TO dwh_reporter;
GRANT SELECT ON bl_dm.fct_transactions_dd TO dwh_reporter;

-- ETL can insert/update log entries (both tables)
GRANT SELECT, INSERT, UPDATE ON bl_cn.etl_log TO dwh_etl;
GRANT SELECT, INSERT, UPDATE ON bl_cn.etl_run TO dwh_etl;

-- Analyst can monitor pipeline health (views only, not base tables)
GRANT SELECT ON bl_cn.v_latest_runs TO dwh_analyst;
GRANT SELECT ON bl_cn.v_etl_summary TO dwh_analyst;
GRANT SELECT ON bl_cn.v_failed_runs TO dwh_analyst;
GRANT SELECT ON bl_cn.v_pipeline_runs TO dwh_analyst;
-- Reporter has no visibility into ETL internals



--  Sensitive column masking — employee salary (employee_salary is NUMERIC(12,2) and should not be visible to analysts or BI tools)
CREATE OR REPLACE VIEW bl_dm.v_employees_public AS
SELECT
    employee_surr_id,
    employee_src_id,
    employee_firstname,
    employee_lastname,
    employee_title,
    employee_email,
    employee_phone_number,
    NULL::NUMERIC(12,2)  AS employee_salary,
    start_dt,
    end_dt,
    is_active,
    ta_insert_dt,
    source_system,
    source_entity
FROM bl_dm.dm_employees_scd;

-- Grant masked view to analyst and reporter
GRANT SELECT ON bl_dm.v_employees_public TO dwh_analyst;
GRANT SELECT ON bl_dm.v_employees_public TO dwh_reporter;


-- Sequence privileges
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bl_3nf TO dwh_etl;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bl_dm TO dwh_etl;
GRANT ALL ON ALL SEQUENCES IN SCHEMA bl_3nf TO dwh_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA bl_dm TO dwh_admin;
GRANT ALL ON ALL SEQUENCES IN SCHEMA bl_cn TO dwh_admin;


--  Routine EXECUTE privileges (Covers functions and procedures)
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA bl_3nf TO dwh_etl;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA bl_3nf TO dwh_admin;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA bl_dm TO dwh_etl;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA bl_dm TO dwh_admin;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA bl_cn TO dwh_etl;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA bl_cn TO dwh_admin;


--Default privileges (Covers objects created IN THE FUTURE.)
ALTER DEFAULT PRIVILEGES IN SCHEMA ext_source GRANT SELECT ON TABLES TO dwh_etl;

ALTER DEFAULT PRIVILEGES IN SCHEMA src GRANT SELECT, INSERT, UPDATE ON TABLES TO dwh_etl;

ALTER DEFAULT PRIVILEGES IN SCHEMA stg_cln GRANT SELECT, INSERT, UPDATE ON TABLES TO dwh_etl;


ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT SELECT, INSERT, UPDATE ON TABLES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT USAGE ON SEQUENCES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT EXECUTE ON ROUTINES  TO dwh_etl;


ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT SELECT, INSERT, UPDATE ON TABLES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT SELECT ON TABLES TO dwh_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT SELECT ON TABLES TO dwh_reporter;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT USAGE ON SEQUENCES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT EXECUTE ON ROUTINES TO dwh_etl;


ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT SELECT, INSERT, UPDATE ON TABLES TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT SELECT ON TABLES    TO dwh_analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT EXECUTE  ON ROUTINES  TO dwh_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT EXECUTE ON ROUTINES  TO dwh_admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA ext_source GRANT ALL ON TABLES    TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA src  GRANT ALL ON TABLES    TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA stg_cln  GRANT ALL ON TABLES    TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT ALL ON TABLES    TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT ALL ON SEQUENCES TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_3nf GRANT ALL ON ROUTINES  TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT ALL ON TABLES    TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT ALL ON SEQUENCES TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_dm GRANT ALL ON ROUTINES  TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT ALL ON TABLES    TO dwh_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA bl_cn GRANT ALL ON ROUTINES  TO dwh_admin;


