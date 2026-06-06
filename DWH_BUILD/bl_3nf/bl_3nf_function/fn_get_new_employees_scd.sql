-- Extracts new or changed employee records for SCD Type 2 processing
-- Combines offline and online sources and returns only latest changes per employee

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_new_employees_scd()
RETURNS TABLE (
    employee_src_id       VARCHAR(15),
    store_branch_id       BIGINT,
    employee_firstname    VARCHAR(50),
    employee_lastname     VARCHAR(50),
    employee_title        VARCHAR(50),
    employee_email        VARCHAR(100),
    employee_phone_number VARCHAR(20),
    employee_salary       NUMERIC(12,2),
    employee_row_hash     VARCHAR(32),
    source_system         VARCHAR(20),
    source_entity         VARCHAR(50)
)
LANGUAGE sql STABLE AS $$

-- Combine employee data from both staging sources
WITH unified AS (

    SELECT
        e.employee_id,
        COALESCE(sb.store_branch_id, -1) AS store_branch_id,  -- only offline has this join
        e.employee_firstname,
        e.employee_lastname,
        e.employee_title,
        e.employee_email,
        e.employee_phone_number,
        e.employee_salary,
        e.employee_row_hash,
        e.stg_insert_dt,
        'OFFLINE'       AS source_system,
        'SALES_OFFLINE' AS source_entity
    FROM stg_cln.sales_offline e
    LEFT JOIN bl_3nf.ce_store_branches sb
           ON sb.store_branch_src_id = e.store_branch_id
          AND sb.source_system       = 'OFFLINE'
          AND sb.source_entity       = 'SALES_OFFLINE'

    UNION ALL

    SELECT
        e.employee_id,
        -1::BIGINT      AS store_branch_id,
        e.employee_firstname,
        e.employee_lastname,
        e.employee_title,
        e.employee_email,
        e.employee_phone_number,
        e.employee_salary,
        e.employee_row_hash,
        e.stg_insert_dt,
        'ONLINE'        AS source_system,
        'SALES_ONLINE'  AS source_entity
    FROM stg_cln.sales_online e
),

-- Keep only latest record per employee and source system
latest AS (
    SELECT DISTINCT ON (employee_id, source_system, source_entity)
        *
    FROM unified
    WHERE employee_id IS NOT NULL
    ORDER BY employee_id, source_system, source_entity, stg_insert_dt DESC
)

-- Return only new or changed employee records for SCD processing
SELECT
    COALESCE(l.employee_id,        'n. a.')::VARCHAR(15),
    l.store_branch_id                      ::BIGINT,
    COALESCE(l.employee_firstname, 'n. a.')::VARCHAR(50),
    COALESCE(l.employee_lastname,  'n. a.')::VARCHAR(50),
    l.employee_title                       ::VARCHAR(50),
    l.employee_email                       ::VARCHAR(100),
    l.employee_phone_number                ::VARCHAR(20),
    l.employee_salary                      ::NUMERIC(12,2),
    l.employee_row_hash                    ::VARCHAR(32),
    l.source_system                        ::VARCHAR(20),
    l.source_entity                        ::VARCHAR(50)
FROM latest l
WHERE
    -- New employees not yet in SCD table
    NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_employees_scd c
        WHERE c.employee_src_id = l.employee_id
          AND c.source_system   = l.source_system
          AND c.source_entity   = l.source_entity
    )

    -- Or active employees with changed attributes (hash difference)
    OR EXISTS (
        SELECT 1 FROM bl_3nf.ce_employees_scd c
        WHERE c.employee_src_id = l.employee_id
          AND c.source_system   = l.source_system
          AND c.source_entity   = l.source_entity
          AND c.is_active       = TRUE
          AND c.employee_row_hash IS DISTINCT FROM l.employee_row_hash
    );

$$;