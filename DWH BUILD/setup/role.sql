--  Permission group roles (no LOGIN)
-- Full control: schema design, object creation, privilege management
CREATE ROLE dwh_admin
    NOLOGIN
    NOINHERIT
    CREATEROLE
    CREATEDB;

-- ETL pipeline operator: reads source, writes to BL_3NF + BL_DM
CREATE ROLE dwh_etl
    NOLOGIN
    INHERIT;


-- Business analyst: read-only on DM layer + monitoring logs
CREATE ROLE dwh_analyst
    NOLOGIN
    INHERIT;


-- BI tool service account: narrower than analyst, views only
CREATE ROLE dwh_reporter
    NOLOGIN
    INHERIT;


--  User accounts (LOGIN, connection limits)
-- DBA admin user
CREATE USER dba_admin
    LOGIN
    PASSWORD '_dba_pw_1!'
    NOSUPERUSER          
    CREATEROLE
    CREATEDB
    CONNECTION LIMIT 5;

GRANT dwh_admin TO dba_admin;


-- ETL pipeline service account (used by the scheduled job)
CREATE USER svc_etl
    LOGIN
    PASSWORD 'etl_service_pw_2!'
    NOSUPERUSER
    NOCREATEROLE
    NOCREATEDB
    CONNECTION LIMIT 3;      

GRANT dwh_etl TO svc_etl;



-- Business analyst user (example human account)
CREATE USER analyst_user
    LOGIN
    PASSWORD 'analyst_pw_3!'
    NOSUPERUSER
    NOCREATEROLE
    NOCREATEDB
    CONNECTION LIMIT 10;

GRANT dwh_analyst TO analyst_user;


-- BI tool service account (used by Tableau / Power BI / Metabase)
CREATE USER svc_bi_tool
    LOGIN
    PASSWORD 'bi_tool_pw_4!'
    NOSUPERUSER
    NOCREATEROLE
    NOCREATEDB
    CONNECTION LIMIT 20;     

GRANT dwh_reporter TO svc_bi_tool;