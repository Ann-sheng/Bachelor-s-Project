

CREATE OR REPLACE PROCEDURE bl_3nf.load_ce_transactions()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_3nf.load_ce_transactions', 'BL_3NF');

    INSERT INTO bl_3nf.ce_transactions (
        transaction_id,
        transaction_src_id,
        product_id,
        shipping_id,
        store_branch_id,
        customer_id,
        employee_id,
        transaction_dt,
        transaction_payment_method,
        transaction_quantity_sold,
        transaction_discount_pct,
        transaction_sale_amount,
        transaction_currency_paid,
        transaction_sales_channel,
        transaction_shipment_status,
        transaction_shipped_dt,
        transaction_delivery_dt,
        ta_insert_dt,
        ta_update_dt,
        source_system,
        source_entity
    )
    SELECT
        nextval('bl_3nf.seq_ce_transactions'),
        t.transaction_id,
        COALESCE(p.product_id,      -1),
        COALESCE(sh.shipping_id,    -1),
        COALESCE(sb.store_branch_id,-1),
        COALESCE(c.customer_id,     -1),
        COALESCE(e.employee_id,     -1),
        t.transaction_dt,
        t.transaction_payment_method,
        t.transaction_quantity_sold,
        t.transaction_discount_pct,
        t.transaction_sale_amount,
        t.transaction_currency_paid,
        t.transaction_sales_channel,
        t.transaction_shipment_status,
        t.transaction_shipped_dt,
        t.transaction_delivery_dt,
        NOW(), NOW(),
        t.source_system,
        t.source_entity
    FROM (
        SELECT
            transaction_id,
            transaction_dt,
            transaction_payment_method,
            transaction_quantity_sold,
            transaction_discount_pct,
            transaction_sale_amount,
            transaction_currency_paid,
            transaction_sales_channel,
            product_id          AS product_src_id,
            customer_id         AS customer_src_id,
            employee_id         AS employee_src_id,
            store_branch_id     AS store_branch_src_id,
            NULL::VARCHAR(15)   AS shipping_src_id,
            NULL::VARCHAR(50)   AS transaction_shipment_status,
            NULL::DATE          AS transaction_shipped_dt,
            NULL::DATE          AS transaction_delivery_dt,
            'OFFLINE'           AS source_system,
            'SALES_OFFLINE'     AS source_entity
        FROM stg_cln.sales_offline

        UNION ALL


        SELECT
            transaction_id,
            transaction_dt,
            transaction_payment_method,
            transaction_quantity_sold,
            transaction_discount_pct,
            transaction_sale_amount,
            transaction_currency_paid,
            transaction_sales_channel,
            product_id          AS product_src_id,
            customer_id         AS customer_src_id,
            employee_id         AS employee_src_id,
            NULL::VARCHAR(15)   AS store_branch_src_id,
            shipping_id         AS shipping_src_id,
            shipment_status     AS transaction_shipment_status,
            shipped_dt          AS transaction_shipped_dt,
            delivery_dt         AS transaction_delivery_dt,
            'ONLINE'            AS source_system,
            'SALES_ONLINE'      AS source_entity
        FROM stg_cln.sales_online
    ) t


    LEFT JOIN bl_3nf.ce_products p ON p.product_src_id  = t.product_src_id
          AND p.source_system   = t.source_system
          AND p.source_entity   = t.source_entity

    LEFT JOIN bl_3nf.ce_store_branches sb ON sb.store_branch_src_id = t.store_branch_src_id
          AND sb.source_system       = t.source_system
          AND sb.source_entity       = t.source_entity

    LEFT JOIN bl_3nf.ce_shippings sh ON sh.shipping_src_id = t.shipping_src_id
          AND sh.source_system   = t.source_system
          AND sh.source_entity   = t.source_entity

    LEFT JOIN bl_3nf.ce_customers_scd c ON c.customer_src_id = t.customer_src_id
          AND c.source_system   = t.source_system
          AND c.source_entity   = t.source_entity
          AND t.transaction_dt BETWEEN c.start_dt AND c.end_dt

    LEFT JOIN bl_3nf.ce_employees_scd e ON e.employee_src_id = t.employee_src_id
          AND e.source_system   = t.source_system
          AND e.source_entity   = t.source_entity
          AND t.transaction_dt BETWEEN e.start_dt AND e.end_dt

    WHERE NOT EXISTS (
        SELECT 1
        FROM bl_3nf.ce_transactions x
        WHERE x.transaction_src_id = t.transaction_id
          AND x.source_system      = t.source_system
          AND x.source_entity      = t.source_entity
    );

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

  CALL bl_cn.log_success(v_log_id, v_affected);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;