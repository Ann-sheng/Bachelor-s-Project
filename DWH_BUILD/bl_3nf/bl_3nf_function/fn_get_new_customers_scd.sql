-- Return customers that are NEW or CHANGED since last load.

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_new_customers_scd()
RETURNS TABLE (
    customer_src_id       VARCHAR(15),
    customer_firstname    VARCHAR(50),
    customer_lastname     VARCHAR(50),
    customer_country      VARCHAR(50),
    customer_city         VARCHAR(50),
    customer_phone_number VARCHAR(20),
    customer_email        VARCHAR(100),
    row_hash              VARCHAR(32),
    source_system         VARCHAR(20),
    source_entity         VARCHAR(50)
)
LANGUAGE sql STABLE AS $$
    -- Offline customers
    SELECT DISTINCT ON (s.customer_id)
        COALESCE(s.customer_id,        'n. a.') ::VARCHAR(15),
        COALESCE(s.customer_firstname, 'n. a.') ::VARCHAR(50),
        COALESCE(s.customer_lastname,  'n. a.') ::VARCHAR(50),
        'n. a.'                                 ::VARCHAR(50),   
        'n. a.'                                 ::VARCHAR(50),   
        NULL                                    ::VARCHAR(20),   
        s.customer_email                        ::VARCHAR(100),
        s.customer_row_hash                     ::VARCHAR(32),
        'OFFLINE'                               ::VARCHAR(20),
        'SALES_OFFLINE'                         ::VARCHAR(50)
    FROM stg_cln.sales_offline s
    -- Only return rows where the hash has changed or the customer is brand new
    WHERE s.customer_id IS NOT NULL
      AND (
          NOT EXISTS (
              SELECT 1 FROM bl_3nf.ce_customers_scd c
              WHERE c.customer_src_id = s.customer_id
                AND c.source_system   = 'OFFLINE'
                AND c.source_entity   = 'SALES_OFFLINE'
          )
          OR EXISTS (
              SELECT 1 FROM bl_3nf.ce_customers_scd c
              WHERE c.customer_src_id = s.customer_id
                AND c.source_system   = 'OFFLINE'
                AND c.source_entity   = 'SALES_OFFLINE'
                AND c.is_active       = TRUE
                AND c.row_hash IS DISTINCT FROM s.customer_row_hash
          )
      )
    ORDER BY s.customer_id, s.stg_insert_dt DESC

    UNION ALL

    -- Online customers
    SELECT DISTINCT ON (s.customer_id)
        COALESCE(s.customer_id,        'n. a.') ::VARCHAR(15),
        COALESCE(s.customer_firstname, 'n. a.') ::VARCHAR(50),
        COALESCE(s.customer_lastname,  'n. a.') ::VARCHAR(50),
        COALESCE(s.customer_country,   'n. a.') ::VARCHAR(50),
        COALESCE(s.customer_city,      'n. a.') ::VARCHAR(50),
        s.customer_phone_number                 ::VARCHAR(20),
        s.customer_email                        ::VARCHAR(100),
        s.customer_row_hash                     ::VARCHAR(32),
        'ONLINE'                                ::VARCHAR(20),
        'SALES_ONLINE'                          ::VARCHAR(50)
    FROM stg_cln.sales_online s
    WHERE s.customer_id IS NOT NULL
      AND (
          NOT EXISTS (
              SELECT 1 FROM bl_3nf.ce_customers_scd c
              WHERE c.customer_src_id = s.customer_id
                AND c.source_system   = 'ONLINE'
                AND c.source_entity   = 'SALES_ONLINE'
          )
          OR EXISTS (
              SELECT 1 FROM bl_3nf.ce_customers_scd c
              WHERE c.customer_src_id = s.customer_id
                AND c.source_system   = 'ONLINE'
                AND c.source_entity   = 'SALES_ONLINE'
                AND c.is_active       = TRUE
                AND c.row_hash IS DISTINCT FROM s.customer_row_hash
          )
      )
    ORDER BY s.customer_id, s.stg_insert_dt DESC
$$;
