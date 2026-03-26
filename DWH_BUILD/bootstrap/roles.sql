
--  Create permission group roles and login users.


-- Permission group roles  
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dwh_admin') THEN
        CREATE ROLE dwh_admin NOLOGIN NOINHERIT CREATEROLE CREATEDB;
        RAISE NOTICE 'Role dwh_admin created.';
    ELSE
        RAISE NOTICE 'Role dwh_admin already exists — skipping.';
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dwh_etl') THEN
        CREATE ROLE dwh_etl NOLOGIN INHERIT;
        RAISE NOTICE 'Role dwh_etl created.';
    ELSE
        RAISE NOTICE 'Role dwh_etl already exists — skipping.';
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dwh_analyst') THEN
        CREATE ROLE dwh_analyst NOLOGIN INHERIT;
        RAISE NOTICE 'Role dwh_analyst created.';
    ELSE
        RAISE NOTICE 'Role dwh_analyst already exists — skipping.';
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dwh_reporter') THEN
        CREATE ROLE dwh_reporter NOLOGIN INHERIT;
        RAISE NOTICE 'Role dwh_reporter created.';
    ELSE
        RAISE NOTICE 'Role dwh_reporter already exists — skipping.';
    END IF;
END $$;



-- Login users
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dba_admin') THEN
        CREATE USER dba_admin
            LOGIN
            NOSUPERUSER
            CREATEROLE
            CREATEDB
            CONNECTION LIMIT 5;
        RAISE NOTICE 'User dba_admin created. Set password with: \password dba_admin';
    ELSE
        RAISE NOTICE 'User dba_admin already exists — skipping creation.';
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'svc_etl') THEN
        CREATE USER svc_etl
            LOGIN
            NOSUPERUSER
            NOCREATEROLE
            NOCREATEDB
            CONNECTION LIMIT 3;
        RAISE NOTICE 'User svc_etl created. Set password with: \password svc_etl';
    ELSE
        RAISE NOTICE 'User svc_etl already exists — skipping creation.';
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyst_user') THEN
        CREATE USER analyst_user
            LOGIN
            NOSUPERUSER
            NOCREATEROLE
            NOCREATEDB
            CONNECTION LIMIT 10;
        RAISE NOTICE 'User analyst_user created. Set password with: \password analyst_user';
    ELSE
        RAISE NOTICE 'User analyst_user already exists — skipping creation.';
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'svc_bi_tool') THEN
        CREATE USER svc_bi_tool
            LOGIN
            NOSUPERUSER
            NOCREATEROLE
            NOCREATEDB
            CONNECTION LIMIT 20;
        RAISE NOTICE 'User svc_bi_tool created. Set password with: \password svc_bi_tool';
    ELSE
        RAISE NOTICE 'User svc_bi_tool already exists — skipping creation.';
    END IF;
END $$;



-- Assign group roles to users
GRANT dwh_admin    TO dba_admin;
GRANT dwh_etl      TO svc_etl;
GRANT dwh_analyst  TO analyst_user;
GRANT dwh_reporter TO svc_bi_tool;
