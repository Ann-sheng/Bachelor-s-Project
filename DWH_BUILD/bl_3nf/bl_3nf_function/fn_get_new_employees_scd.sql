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

WITH offline AS (
    SELECT DISTINCT ON (e.employee_id)
        COALESCE(e.employee_id,        'n. a.')::VARCHAR(15),
        COALESCE(sb.store_branch_id,   -1)     ::BIGINT,
        COALESCE(e.employee_firstname, 'n. a.')::VARCHAR(50),
        COALESCE(e.employee_lastname,  'n. a.')::VARCHAR(50),
        e.employee_title                       ::VARCHAR(50),
        e.employee_email                       ::VARCHAR(100),
        e.employee_phone_number                ::VARCHAR(20),
        e.employee_salary                      ::NUMERIC(12,2),
        e.employee_row_hash                    ::VARCHAR(32),
        'OFFLINE'                              ::VARCHAR(20),
        'SALES_OFFLINE'                        ::VARCHAR(50)
    FROM stg_cln.sales_offline e
    LEFT JOIN bl_3nf.ce_store_branches sb
           ON sb.store_branch_src_id = e.store_branch_id
          AND sb.source_system       = 'OFFLINE'
          AND sb.source_entity       = 'SALES_OFFLINE'
    WHERE e.employee_id IS NOT NULL
      AND (
          NOT EXISTS (
              SELECT 1 FROM bl_3nf.ce_employees_scd c
              WHERE c.employee_src_id = e.employee_id
                AND c.source_system   = 'OFFLINE'
                AND c.source_entity   = 'SALES_OFFLINE'
          )
          OR EXISTS (
              SELECT 1 FROM bl_3nf.ce_employees_scd c
              WHERE c.employee_src_id = e.employee_id
                AND c.source_system   = 'OFFLINE'
                AND c.source_entity   = 'SALES_OFFLINE'
                AND c.is_active       = TRUE
                AND c.employee_row_hash IS DISTINCT FROM e.employee_row_hash
          )
      )
    ORDER BY e.employee_id, e.stg_insert_dt DESC
),

online AS (
    SELECT DISTINCT ON (e.employee_id)
        COALESCE(e.employee_id,        'n. a.')::VARCHAR(15),
        -1                                     ::BIGINT,
        COALESCE(e.employee_firstname, 'n. a.')::VARCHAR(50),
        COALESCE(e.employee_lastname,  'n. a.')::VARCHAR(50),
        e.employee_title                       ::VARCHAR(50),
        e.employee_email                       ::VARCHAR(100),
        e.employee_phone_number                ::VARCHAR(20),
        e.employee_salary                      ::NUMERIC(12,2),
        e.employee_row_hash                    ::VARCHAR(32),
        'ONLINE'                               ::VARCHAR(20),
        'SALES_ONLINE'                         ::VARCHAR(50)
    FROM stg_cln.sales_online e
    WHERE e.employee_id IS NOT NULL
      AND (
          NOT EXISTS (
              SELECT 1 FROM bl_3nf.ce_employees_scd c
              WHERE c.employee_src_id = e.employee_id
                AND c.source_system   = 'ONLINE'
                AND c.source_entity   = 'SALES_ONLINE'
          )
          OR EXISTS (
              SELECT 1 FROM bl_3nf.ce_employees_scd c
              WHERE c.employee_src_id = e.employee_id
                AND c.source_system   = 'ONLINE'
                AND c.source_entity   = 'SALES_ONLINE'
                AND c.is_active       = TRUE
                AND c.employee_row_hash IS DISTINCT FROM e.employee_row_hash
          )
      )
    ORDER BY e.employee_id, e.stg_insert_dt DESC
)

SELECT * FROM offline
UNION ALL
SELECT * FROM online;

$$;