-- Incremental load of FCT_TRANSACTIONS_DD from BL_3NF.

CREATE OR REPLACE PROCEDURE bl_dm.load_fct_transactions_dd()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_fct_transactions_dd', 'BL_DM');

    INSERT INTO bl_dm.fct_transactions_dd (
        transaction_src_id,
        product_surr_id, 
        customer_surr_id, 
        employee_surr_id,
        store_branch_surr_id, 
        shipping_surr_id, 
        junk_surr_id,
        event_date_key, 
        shipped_date_key, 
        delivery_date_key,
        transaction_quantity_sold, 
        transaction_discount_pct,
        transaction_sale_amount,
        transaction_gross_profit, 
        transaction_profit_margin,
        ta_insert_dt, ta_update_dt
    )
    SELECT
        t.transaction_id,
        COALESCE(dp.product_surr_id,       -1),
        COALESCE(dc.customer_surr_id,      -1),
        COALESCE(de.employee_surr_id,      -1),
        COALESCE(dsb.store_branch_surr_id, -1),
        COALESCE(dsh.shipping_surr_id,     -1),
        COALESCE(dj.junk_surr_id,          -1),
        COALESCE(dd_event.date_key,        -1),
        COALESCE(dd_ship.date_key,         -1),
        COALESCE(dd_deliv.date_key,        -1),
        COALESCE(t.transaction_quantity_sold, 0),
        COALESCE(t.transaction_discount_pct,  0),
        COALESCE(t.transaction_sale_amount,   0),
        CASE
            WHEN dp.product_surr_id IS NULL OR dp.product_surr_id = -1 THEN NULL
            ELSE t.transaction_sale_amount
                 - (t.transaction_quantity_sold * dp.product_unit_cost)
        END,
        CASE
            WHEN dp.product_surr_id IS NULL OR dp.product_surr_id = -1 THEN NULL
            WHEN t.transaction_sale_amount <> 0
            THEN (t.transaction_sale_amount
                  - (t.transaction_quantity_sold * dp.product_unit_cost))
                 / t.transaction_sale_amount
            ELSE NULL
        END,
        NOW(), NOW()
    FROM bl_3nf.ce_transactions t
    LEFT JOIN bl_dm.dm_products dp
           ON dp.product_src_id = t.product_id
          AND dp.source_system  = 'BL_3NF'
          AND dp.source_entity  = 'CE_PRODUCTS'
    LEFT JOIN bl_dm.dm_customers_scd dc
           ON dc.customer_src_id = t.customer_id
          AND dc.source_system   = 'BL_3NF'
          AND dc.source_entity   = 'CE_CUSTOMERS_SCD'
    LEFT JOIN bl_dm.dm_employees_scd de
           ON de.employee_src_id = t.employee_id
          AND de.source_system   = 'BL_3NF'
          AND de.source_entity   = 'CE_EMPLOYEES_SCD'
    LEFT JOIN bl_dm.dm_store_branches dsb
           ON dsb.store_branch_src_id = t.store_branch_id
          AND dsb.source_system       = 'BL_3NF'
          AND dsb.source_entity       = 'CE_STORE_BRANCHES'
    LEFT JOIN bl_dm.dm_shippings dsh
           ON dsh.shipping_src_id = t.shipping_id
          AND dsh.source_system   = 'BL_3NF'
          AND dsh.source_entity   = 'CE_SHIPPINGS'
    LEFT JOIN bl_dm.dm_junk_transactions dj
           ON dj.junk_payment_method  IS NOT DISTINCT FROM t.transaction_payment_method
          AND dj.junk_currency_paid   IS NOT DISTINCT FROM t.transaction_currency_paid
          AND dj.junk_sales_channel   IS NOT DISTINCT FROM t.transaction_sales_channel
          AND dj.junk_shipment_status IS NOT DISTINCT FROM t.transaction_shipment_status
    LEFT JOIN bl_dm.dim_dates dd_event ON dd_event.full_date = t.transaction_dt
    LEFT JOIN bl_dm.dim_dates dd_ship  ON dd_ship.full_date  = t.transaction_shipped_dt
    LEFT JOIN bl_dm.dim_dates dd_deliv ON dd_deliv.full_date = t.transaction_delivery_dt
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_dm.fct_transactions_dd f
        WHERE f.transaction_src_id = t.transaction_id
    );

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    CALL bl_cn.log_success(v_log_id, v_inserted);
    RAISE NOTICE '[load_fct_transactions_dd] Inserted: % rows', v_inserted;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;