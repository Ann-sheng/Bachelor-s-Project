
--  Extract deduplicated supplier records from stg_cln, Combines offline + online sources

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_suppliers_data()
RETURNS TABLE (
    supplier_src_id          VARCHAR(15),
    supplier_name            VARCHAR(100),
    supplier_email           VARCHAR(100),
    supplier_number          VARCHAR(20),
    supplier_primary_contact VARCHAR(100),
    supplier_location        VARCHAR(100),
    source_system            VARCHAR(20),
    source_entity            VARCHAR(50)
)
LANGUAGE sql STABLE AS $$
    SELECT DISTINCT ON (supplier_id, src_system)
        COALESCE(supplier_id,              'n. a.') ::VARCHAR(15),
        COALESCE(supplier_name,            'n. a.') ::VARCHAR(100),
        supplier_email,                            
        supplier_number,                       
        supplier_primary_contact,              
        supplier_location,                      
        'OFFLINE'::VARCHAR(20),
        'SALES_OFFLINE'::VARCHAR(50)
    FROM stg_cln.sales_offline
    ORDER BY supplier_id, src_system, stg_insert_dt DESC

    UNION ALL

    SELECT DISTINCT ON (supplier_id, src_system)
        COALESCE(supplier_id,              'n. a.') ::VARCHAR(15),
        COALESCE(supplier_name,            'n. a.') ::VARCHAR(100),
        supplier_email,
        supplier_number,
        supplier_primary_contact,
        supplier_location,
        'ONLINE'::VARCHAR(20),
        'SALES_ONLINE'::VARCHAR(50)
    FROM stg_cln.sales_online
    ORDER BY supplier_id, src_system, stg_insert_dt DESC;
$$;