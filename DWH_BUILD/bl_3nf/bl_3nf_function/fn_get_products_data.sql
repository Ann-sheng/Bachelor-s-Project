
--  Extract deduplicated products joined with supplier surrogate key


CREATE OR REPLACE FUNCTION bl_3nf.fn_get_products_data()
RETURNS TABLE (
    product_src_id          VARCHAR(15),
    supplier_id             BIGINT,
    product_category        VARCHAR(100),
    product_name            VARCHAR(100),
    product_unit_cost       NUMERIC(12,2),
    product_unit_price      NUMERIC(12,2),
    product_warranty_period INTEGER,
    source_system           VARCHAR(20),
    source_entity           VARCHAR(50)
)
LANGUAGE sql STABLE AS $$
    SELECT DISTINCT ON (p.product_id, p.src_system)
        COALESCE(p.product_id,       'n. a.') ::VARCHAR(15),
        COALESCE(s.supplier_id,      -1)      ::BIGINT,
        COALESCE(p.product_category, 'n. a.') ::VARCHAR(100),
        COALESCE(p.product_name,     'n. a.') ::VARCHAR(100),
        COALESCE(p.product_unit_cost,  0)     ::NUMERIC(12,2),
        COALESCE(p.product_unit_price, 0)     ::NUMERIC(12,2),
        p.product_warranty_period             ::INTEGER,    
        'OFFLINE'::VARCHAR(20),
        'SALES_OFFLINE'::VARCHAR(50)
    FROM stg_cln.sales_offline p
    LEFT JOIN bl_3nf.ce_suppliers s
           ON s.supplier_src_id = p.supplier_id
          AND s.source_system   = 'OFFLINE'
          AND s.source_entity   = 'SALES_OFFLINE'
    ORDER BY p.product_id, p.src_system, p.stg_insert_dt DESC

    UNION ALL

    SELECT DISTINCT ON (p.product_id, p.src_system)
        COALESCE(p.product_id,       'n. a.') ::VARCHAR(15),
        COALESCE(s.supplier_id,      -1)      ::BIGINT,
        COALESCE(p.product_category, 'n. a.') ::VARCHAR(100),
        COALESCE(p.product_name,     'n. a.') ::VARCHAR(100),
        COALESCE(p.product_unit_cost,  0)     ::NUMERIC(12,2),
        COALESCE(p.product_unit_price, 0)     ::NUMERIC(12,2),
        p.product_warranty_period             ::INTEGER,
        'ONLINE'::VARCHAR(20),
        'SALES_ONLINE'::VARCHAR(50)
    FROM stg_cln.sales_online p
    LEFT JOIN bl_3nf.ce_suppliers s
           ON s.supplier_src_id = p.supplier_id
          AND s.source_system   = 'ONLINE'
          AND s.source_entity   = 'SALES_ONLINE'
    ORDER BY p.product_id, p.src_system, p.stg_insert_dt DESC;
$$;

