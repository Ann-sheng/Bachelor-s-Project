
--  Role and user CREATION is in  bootstrap/roles.sql. This file handles only DB-level role configuration that must run inside the target database after it exists
-- specifically lock_timeout  and statement_timeout settings for each service account.


-- Prevent long-running analyst queries from holding locks indefinitely.
ALTER ROLE analyst_user IN DATABASE "dnd_sales"
    SET lock_timeout      = '30s';
ALTER ROLE analyst_user IN DATABASE "dnd_sales"
    SET statement_timeout = '5min';

-- BI tool: tighter statement timeout to protect from runaway reports.
ALTER ROLE svc_bi_tool IN DATABASE "dnd_sales"
    SET lock_timeout      = '10s';
ALTER ROLE svc_bi_tool IN DATABASE "dnd_sales"
    SET statement_timeout = '2min';

-- ETL: no statement timeout (long loads expected), but cap lock waits.
ALTER ROLE svc_etl IN DATABASE "dnd_sales"
    SET lock_timeout      = '2min';
