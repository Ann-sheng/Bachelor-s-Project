-- Procedure to profile data before loading

CREATE OR REPLACE PROCEDURE ext_source.profile_sales_online()
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE '=== ONLINE SOURCE PROFILE ===';
    
    RAISE NOTICE 'Total rows: %',
        (SELECT COUNT(*) FROM ext_source.ext_sales_online);
    
    RAISE NOTICE 'NULL transaction_id : %',
        (SELECT COUNT(*) FROM ext_source.ext_sales_online 
         WHERE transaction_id IS NULL);
         
    RAISE NOTICE 'NULL customer_id    : %',
        (SELECT COUNT(*) FROM ext_source.ext_sales_online 
         WHERE customer_id IS NULL);
         
    RAISE NOTICE 'NULL sales_amount   : %',
        (SELECT COUNT(*) FROM ext_source.ext_sales_online 
         WHERE sales_amount IS NULL);
    
    RAISE NOTICE 'Date range: % to %',
        (SELECT MIN(transaction_date) FROM ext_source.ext_sales_online),
        (SELECT MAX(transaction_date) FROM ext_source.ext_sales_online);
    
    RAISE NOTICE 'Duplicate transaction_ids: %',
        (SELECT COUNT(*) FROM (
            SELECT transaction_id
            FROM ext_source.ext_sales_online
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        ) dups);
END;
$$;