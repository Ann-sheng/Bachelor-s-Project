-- Procedure to profile data before loading

CREATE OR REPLACE PROCEDURE ext_source.profile_sales_offline()
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE '=== OFFLINE SOURCE PROFILE ===';
    
    RAISE NOTICE 'Total rows: %',
        (SELECT COUNT(*) FROM ext_source.ext_sales_offline);
    
    RAISE NOTICE 'NULL transaction_id : %',
        (SELECT COUNT(*) FROM ext_source.ext_sales_offline 
         WHERE transaction_id IS NULL);
         
    RAISE NOTICE 'NULL customer_id    : %',
        (SELECT COUNT(*) FROM ext_source.ext_sales_offline 
         WHERE customer_id IS NULL);
         
    RAISE NOTICE 'NULL sales_amount   : %',
        (SELECT COUNT(*) FROM ext_source.ext_sales_offline 
         WHERE sales_amount IS NULL);
    
    RAISE NOTICE 'Date range: % to %',
        (SELECT MIN(transaction_date) FROM ext_source.ext_sales_offline),
        (SELECT MAX(transaction_date) FROM ext_source.ext_sales_offline);
    
    RAISE NOTICE 'Duplicate transaction_ids: %',
        (SELECT COUNT(*) FROM (
            SELECT transaction_id
            FROM ext_source.ext_sales_offline
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        ) dups);
END;
$$;