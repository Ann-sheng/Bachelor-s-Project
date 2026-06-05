-- Extracts new or changed customer records for SCD Type 2 processing
-- Unifies offline and online sources and returns only latest changes per customer

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_new_customers_scd()
RETURNS TABLE (
    customer_src_id       VARCHAR(15),
    customer_firstname    VARCHAR(50),
    customer_lastname     VARCHAR(50),
    customer_country      VARCHAR(50),
    customer_city         VARCHAR(50),
    customer_phone_number VARCHAR(20),
    customer_email        VARCHAR(100),
    customer_row_hash     VARCHAR(32),
    source_system         VARCHAR(20),
    source_entity         VARCHAR(50)
)
LANGUAGE sql STABLE AS $$

-- Combine customer data from both staging sources
WITH unified AS (

    SELECT
        customer_id,
        customer_firstname,
        customer_lastname,
        NULL::VARCHAR(50) AS customer_country,
        NULL::VARCHAR(50) AS customer_city,
        NULL::VARCHAR(20) AS customer_phone_number,
        customer_email,
        customer_row_hash,
        stg_insert_dt,
        'OFFLINE' AS source_system,
        'SALES_OFFLINE' AS source_entity
    FROM stg_cln.sales_offline

    UNION ALL

    SELECT
        customer_id,
        customer_firstname,
        customer_lastname,
        customer_country,
        customer_city,
        customer_phone_number,
        customer_email,
        customer_row_hash,
        stg_insert_dt,
        'ONLINE',
        'SALES_ONLINE'
    FROM stg_cln.sales_online
),

-- Keep only latest record per customer and source
latest AS (
    SELECT DISTINCT ON (customer_id, source_system, source_entity)
        *
    FROM unified
    WHERE customer_id IS NOT NULL
    ORDER BY customer_id, source_system, source_entity, stg_insert_dt DESC
)

-- Return only new or changed records compared to SCD table
SELECT
    COALESCE(l.customer_id,        'n. a.')::VARCHAR(15),
    COALESCE(l.customer_firstname, 'n. a.')::VARCHAR(50),
    COALESCE(l.customer_lastname,  'n. a.')::VARCHAR(50),
    COALESCE(l.customer_country,   'n. a.')::VARCHAR(50),
    COALESCE(l.customer_city,      'n. a.')::VARCHAR(50),
    l.customer_phone_number::VARCHAR(20),
    l.customer_email::VARCHAR(100),
    l.customer_row_hash::VARCHAR(32),
    l.source_system::VARCHAR(20),
    l.source_entity::VARCHAR(50)
FROM latest l
WHERE
    -- New customers not yet in SCD table
    NOT EXISTS (
        SELECT 1 FROM bl_3nf.ce_customers_scd c
        WHERE c.customer_src_id = l.customer_id
          AND c.source_system   = l.source_system
          AND c.source_entity   = l.source_entity
    )

    -- Or existing customers with changed attributes (hash difference)
    OR EXISTS (
        SELECT 1 FROM bl_3nf.ce_customers_scd c
        WHERE c.customer_src_id = l.customer_id
          AND c.source_system   = l.source_system
          AND c.source_entity   = l.source_entity
          AND c.is_active       = TRUE
          AND c.customer_row_hash IS DISTINCT FROM l.customer_row_hash
    );

$$;