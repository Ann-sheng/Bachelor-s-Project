
-- PURPOSE : Extract deduplicated store branch records from stg_cln, Offline source only

CREATE OR REPLACE FUNCTION bl_3nf.fn_get_store_branches_data()
RETURNS TABLE (
    store_branch_src_id          VARCHAR(15),
    store_branch_state           VARCHAR(50),
    store_branch_city            VARCHAR(50),
    store_branch_phone_number    VARCHAR(20),
    store_branch_operating_days  VARCHAR(50),
    store_branch_operating_hours VARCHAR(50),
    source_system                VARCHAR(20),
    source_entity                VARCHAR(50)
)
LANGUAGE sql STABLE AS $$
    SELECT DISTINCT ON (store_branch_id)
        COALESCE(store_branch_id,              'n. a.') ::VARCHAR(15),
        COALESCE(store_branch_state,           'n. a.') ::VARCHAR(50),
        COALESCE(store_branch_city,            'n. a.') ::VARCHAR(50),
        store_branch_phone_number,             
        store_branch_operating_days,          
        store_branch_operating_hours,      
        'OFFLINE'::VARCHAR(20),
        'SALES_OFFLINE'::VARCHAR(50)
    FROM stg_cln.sales_offline
    WHERE store_branch_id IS NOT NULL
    ORDER BY store_branch_id, stg_insert_dt DESC;
$$;

