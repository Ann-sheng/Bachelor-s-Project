
--  Return customers that are NEW or CHANGED since last load, Detects change via row_hash comparison, Returns BOTH new customers AND changed customers

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
    -- Offline customers (no geo data → fill with 'n. a.')
    SELECT DISTINCT ON (customer_id)
        COALESCE(customer_id,        'n. a.') ::VARCHAR(15),
        COALESCE(customer_firstname, 'n. a.') ::VARCHAR(50),
        COALESCE(customer_lastname,  'n. a.') ::VARCHAR(50),
        'n. a.'                               ::VARCHAR(50),  
        'n. a.'                               ::VARCHAR(50),  
        customer_email                        ::VARCHAR(20),  
        customer_email                        ::VARCHAR(100), 
        customer_row_hash                     ::VARCHAR(32),
        'OFFLINE'                             ::VARCHAR(20),
        'SALES_OFFLINE'                       ::VARCHAR(50)
    FROM stg_cln.sales_offline
    WHERE customer_id IS NOT NULL
    ORDER BY customer_id, stg_insert_dt DESC

    UNION ALL

    SELECT DISTINCT ON (customer_id)
        COALESCE(customer_id,           'n. a.') ::VARCHAR(15),
        COALESCE(customer_firstname,    'n. a.') ::VARCHAR(50),
        COALESCE(customer_lastname,     'n. a.') ::VARCHAR(50),
        COALESCE(customer_country,      'n. a.') ::VARCHAR(50),
        COALESCE(customer_city,         'n. a.') ::VARCHAR(50),
        customer_phone_number                    ::VARCHAR(20),
        customer_email                           ::VARCHAR(100),
        customer_row_hash                        ::VARCHAR(32),
        'ONLINE'                                 ::VARCHAR(20),
        'SALES_ONLINE'                           ::VARCHAR(50)
    FROM stg_cln.sales_online
    WHERE customer_id IS NOT NULL
    ORDER BY customer_id, stg_insert_dt DESC
$$;

