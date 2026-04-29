-- PURPOSE : Clean src.sales_online → stg_cln.sales_online

CREATE OR REPLACE PROCEDURE stg_cln.load_stg_online()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_rejected INT := 0;
    v_total    INT := 0;
    v_err_msg  TEXT;
BEGIN
    SELECT COUNT(*) INTO v_total FROM src.sales_online;

    TRUNCATE TABLE stg_cln.sales_online;

    DELETE FROM stg_cln.reject_sales WHERE source_system = 'ONLINE';

    INSERT INTO stg_cln.sales_online (
        customer_id, customer_firstname, customer_lastname,
        customer_country, customer_city, customer_phone_number, customer_email,
        product_id, product_category, product_name,
        product_unit_cost, product_unit_price, product_warranty_period,
        transaction_id, transaction_dt,
        transaction_quantity_sold, transaction_discount_pct,
        transaction_sale_amount, transaction_payment_method,
        transaction_currency_paid, transaction_sales_channel,
        employee_id, employee_firstname, employee_lastname, employee_title,
        employee_email, employee_phone_number, employee_salary,
        supplier_id, supplier_name, supplier_email,
        supplier_number, supplier_primary_contact, supplier_location,
        shipping_id, shipping_method, shipping_carrier,
        shipped_dt, delivery_dt, shipment_status
    )
    SELECT
        TRIM(customer_id),
        INITCAP(TRIM(customer_firstname)),
        INITCAP(TRIM(customer_lastname)),
        INITCAP(NULLIF(TRIM(customer_country), '')),
        INITCAP(NULLIF(TRIM(customer_city), '')),
        NULLIF(REGEXP_REPLACE(customer_phone_number,'[^0-9+]','','g'), ''),
        LOWER(NULLIF(TRIM(customer_email), '')),
        TRIM(product_id),
        INITCAP(TRIM(product_category)),
        INITCAP(TRIM(product_name)),
        COALESCE(NULLIF(REGEXP_REPLACE(unit_cost,'[^0-9.]','','g'),'')::NUMERIC, 0),
        COALESCE(NULLIF(REGEXP_REPLACE(unit_price,'[^0-9.]','','g'),'')::NUMERIC, 0),
        NULLIF(REGEXP_REPLACE(warranty_period_months,'[^0-9]','','g'),'')::INTEGER,
        TRIM(transaction_id),
        transaction_date::DATE,
        NULLIF(REGEXP_REPLACE(quantity_sold,'[^0-9]','','g'),'')::INTEGER,
        COALESCE(NULLIF(REGEXP_REPLACE(discount_applied,'[^0-9.]','','g'),'')::NUMERIC, 0),
        NULLIF(REGEXP_REPLACE(sales_amount,'[^0-9.]','','g'),'')::NUMERIC,
        INITCAP(TRIM(payment_method)),
        UPPER(TRIM(currency_paid)),
        INITCAP(TRIM(sales_channel)),
        TRIM(employee_id),
        INITCAP(TRIM(employee_firstname)),
        INITCAP(TRIM(employee_lastname)),
        INITCAP(TRIM(employee_title)),
        LOWER(NULLIF(TRIM(employee_email), '')),
        NULLIF(REGEXP_REPLACE(employee_phone_number,'[^0-9+]','','g'),''),
        COALESCE(NULLIF(REGEXP_REPLACE(employee_salary,'[^0-9.]','','g'),'')::NUMERIC, 0),
        TRIM(supplier_id),
        INITCAP(TRIM(supplier_name)),
        LOWER(NULLIF(TRIM(supplier_email), '')),
        NULLIF(REGEXP_REPLACE(supplier_number,'[^0-9+]','','g'),''),
        INITCAP(NULLIF(TRIM(supplier_primary_contact), '')),
        INITCAP(NULLIF(TRIM(supplier_location), '')),
        TRIM(shipping_id),
        INITCAP(NULLIF(TRIM(shipping_method), '')),
        INITCAP(NULLIF(TRIM(shipping_carrier), '')),
        NULLIF(shipped_date, '')::DATE,
        NULLIF(delivery_date, '')::DATE,
        INITCAP(NULLIF(TRIM(shipment_status), ''))
    FROM src.sales_online
    WHERE
        customer_id    IS NOT NULL AND TRIM(customer_id)    <> ''
        AND product_id IS NOT NULL AND TRIM(product_id)     <> ''
        AND transaction_id IS NOT NULL AND TRIM(transaction_id) <> ''
        AND transaction_date ~ '^\d{4}-\d{2}-\d{2}$'
        AND NULLIF(REGEXP_REPLACE(quantity_sold,'[^0-9]','','g'),'')::INTEGER > 0
        AND NULLIF(REGEXP_REPLACE(sales_amount,'[^0-9.]','','g'),'')::NUMERIC > 0
        AND COALESCE(NULLIF(REGEXP_REPLACE(discount_applied,'[^0-9.]','','g'),'')::NUMERIC, 0)
            BETWEEN 0 AND 100;

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    INSERT INTO stg_cln.reject_sales (source_system, source_table, reject_reason, raw_data)
    SELECT
        'ONLINE', 'src.sales_online',
        CONCAT_WS(' | ',
            CASE WHEN customer_id    IS NULL OR TRIM(customer_id)    = '' THEN 'NULL/EMPTY customer_id'    END,
            CASE WHEN product_id     IS NULL OR TRIM(product_id)     = '' THEN 'NULL/EMPTY product_id'     END,
            CASE WHEN transaction_id IS NULL OR TRIM(transaction_id) = '' THEN 'NULL/EMPTY transaction_id' END,
            CASE WHEN transaction_date IS NULL OR transaction_date NOT LIKE '____-__-__'
                 THEN 'INVALID transaction_date' END,
            CASE WHEN NULLIF(REGEXP_REPLACE(quantity_sold,'[^0-9]','','g'),'')::INTEGER <= 0
                 THEN 'quantity_sold <= 0' END,
            CASE WHEN NULLIF(REGEXP_REPLACE(sales_amount,'[^0-9.]','','g'),'')::NUMERIC <= 0
                 THEN 'sales_amount <= 0' END,
            CASE WHEN COALESCE(NULLIF(REGEXP_REPLACE(discount_applied,'[^0-9.]','','g'),'')::NUMERIC, 0)
                      NOT BETWEEN 0 AND 100
                 THEN 'discount out of range' END
        ),
        TO_JSONB(s)
    FROM src.sales_online s
    WHERE
        customer_id    IS NULL OR TRIM(customer_id)    = ''
        OR product_id  IS NULL OR TRIM(product_id)     = ''
        OR transaction_id IS NULL OR TRIM(transaction_id) = ''
        OR transaction_date IS NULL OR transaction_date NOT LIKE '____-__-__'
        OR NULLIF(REGEXP_REPLACE(quantity_sold,'[^0-9]','','g'),'')::INTEGER <= 0
        OR NULLIF(REGEXP_REPLACE(sales_amount,'[^0-9.]','','g'),'')::NUMERIC  <= 0
        OR COALESCE(NULLIF(REGEXP_REPLACE(discount_applied,'[^0-9.]','','g'),'')::NUMERIC, 0)
           NOT BETWEEN 0 AND 100;

    GET DIAGNOSTICS v_rejected = ROW_COUNT;

    v_log_id := bl_cn.log_start('stg_cln.load_stg_online', 'STG_CLN');
    CALL bl_cn.log_success(v_log_id, v_inserted);

    RAISE NOTICE '[load_stg_online] Total: % | Valid: % | Rejected: %',
        v_total, v_inserted, v_rejected;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    IF v_log_id IS NOT NULL THEN
        CALL bl_cn.log_failure(v_log_id, v_err_msg);
    END IF;
    RAISE;
END;
$$;