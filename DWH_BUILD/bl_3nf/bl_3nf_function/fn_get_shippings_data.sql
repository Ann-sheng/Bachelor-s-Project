

-- Extract deduplicated shipping records from stg_cln, Online source only (offline has no shipping)


CREATE OR REPLACE FUNCTION bl_3nf.fn_get_shippings_data()
RETURNS TABLE (
    shipping_src_id   VARCHAR(15),
    shipping_method   VARCHAR(100),
    shipping_carrier  VARCHAR(100),
    source_system     VARCHAR(20),
    source_entity     VARCHAR(50)
)
LANGUAGE sql STABLE AS $$
    SELECT DISTINCT ON (shipping_id)
        COALESCE(shipping_id,      'n. a.') ::VARCHAR(15),
        COALESCE(shipping_method,  'n. a.') ::VARCHAR(100),
        COALESCE(shipping_carrier, 'n. a.') ::VARCHAR(100),
        'ONLINE'::VARCHAR(20),
        'SALES_ONLINE'::VARCHAR(50)
    FROM stg_cln.sales_online
    WHERE shipping_id IS NOT NULL
    ORDER BY shipping_id, stg_insert_dt DESC;
$$;
