-- PURPOSE : Clean src.sales_offline → stg_cln.sales_offline

CREATE OR REPLACE PROCEDURE stg_cln.load_stg_offline()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_rejected INT := 0;
    v_total    INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('stg_cln.load_stg_offline', 'STG_CLN');
    SELECT COUNT(*) INTO v_total FROM src.sales_offline;

    TRUNCATE TABLE stg_cln.sales_offline;

    DELETE FROM stg_cln.reject_sales WHERE source_system = 'OFFLINE';

    INSERT INTO stg_cln.sales_offline (
        customer_id, customer_firstname, customer_lastname, customer_email,
        product_id, product_category, product_name,
        product_unit_cost, product_unit_price, product_warranty_period,
        transaction_id, transaction_dt,
        transaction_quantity_sold, transaction_discount_pct,
        transaction_sale_amount, transaction_payment_method,
        transaction_currency_paid, transaction_sales_channel,
        employee_id, employee_firstname, employee_lastname, employee_title,
        employee_email, employee_phone_number, employee_salary,
        store_branch_id, store_branch_state, store_branch_city,
        store_branch_phone_number, store_branch_operating_days,
        store_branch_operating_hours,
        supplier_id, supplier_name, supplier_email,
        supplier_number, supplier_primary_contact, supplier_location
    )
    SELECT
        TRIM(customer_id),
        INITCAP(TRIM(customer_firstname)),
        INITCAP(TRIM(customer_lastname)),
        LOWER(NULLIF(TRIM(customer_email), '')),
        TRIM(product_id),
        INITCAP(TRIM(product_category)),
        INITCAP(TRIM(product_name)),
        COALESCE(NULLIF(REGEXP_REPLACE(unit_cost,  '[^0-9.]','','g'), '')::NUMERIC, 0),
        COALESCE(NULLIF(REGEXP_REPLACE(unit_price, '[^0-9.]','','g'), '')::NUMERIC, 0),
        NULLIF(REGEXP_REPLACE(warranty_period_months,'[^0-9]','','g'), '')::INTEGER,
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
        NULLIF(REGEXP_REPLACE(employee_phone_number,'[^0-9+]','','g'), ''),
        COALESCE(NULLIF(REGEXP_REPLACE(employee_salary,'[^0-9.]','','g'),'')::NUMERIC, 0),
        NULLIF(TRIM(storebranch_id), ''),
        INITCAP(NULLIF(TRIM(storebranch_state), '')),
        INITCAP(NULLIF(TRIM(storebranch_city), '')),
        NULLIF(REGEXP_REPLACE(storebranch_phone_number,'[^0-9+]','','g'), ''),
        NULLIF(TRIM(storebranch_operating_days), ''),
        NULLIF(TRIM(storebranch_operating_hours), ''),
        TRIM(supplier_id),
        INITCAP(TRIM(supplier_name)),
        LOWER(NULLIF(TRIM(supplier_email), '')),
        NULLIF(REGEXP_REPLACE(supplier_number,'[^0-9+]','','g'), ''),
        INITCAP(NULLIF(TRIM(supplier_primary_contact), '')),
        INITCAP(NULLIF(TRIM(supplier_location), ''))
    FROM src.sales_offline
    WHERE
        customer_id    IS NOT NULL AND TRIM(customer_id)    <> ''
        AND product_id IS NOT NULL AND TRIM(product_id)     <> ''
        AND transaction_id IS NOT NULL AND TRIM(transaction_id) <> ''
        AND transaction_date ~ '^\d{4}-\d{2}-\d{2}$'
        AND NULLIF(REGEXP_REPLACE(quantity_sold,'[^0-9]','','g'),'')::INTEGER > 0
        AND NULLIF(REGEXP_REPLACE(sales_amount,'[^0-9.]','','g'),'')::NUMERIC > 0
        AND COALESCE(
                NULLIF(REGEXP_REPLACE(discount_applied,'[^0-9.]','','g'),'')::NUMERIC, 0
            ) BETWEEN 0 AND 100;

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    INSERT INTO stg_cln.reject_sales (source_system, source_table, reject_reason, raw_data)
    SELECT
        'OFFLINE', 'src.sales_offline',
        CONCAT_WS(' | ',
            CASE WHEN customer_id    IS NULL OR TRIM(customer_id)    = '' THEN 'NULL/EMPTY customer_id'    END,
            CASE WHEN product_id     IS NULL OR TRIM(product_id)     = '' THEN 'NULL/EMPTY product_id'     END,
            CASE WHEN transaction_id IS NULL OR TRIM(transaction_id) = '' THEN 'NULL/EMPTY transaction_id' END,
            CASE WHEN transaction_date IS NULL OR transaction_date NOT LIKE '____-__-__'
                 THEN 'INVALID transaction_date: ' || COALESCE(transaction_date, 'NULL') END,
            CASE WHEN NULLIF(REGEXP_REPLACE(quantity_sold,'[^0-9]','','g'),'')::INTEGER <= 0
                 THEN 'quantity_sold must be > 0' END,
            CASE WHEN NULLIF(REGEXP_REPLACE(sales_amount,'[^0-9.]','','g'),'')::NUMERIC <= 0
                 THEN 'sales_amount must be > 0' END,
            CASE WHEN COALESCE(NULLIF(REGEXP_REPLACE(discount_applied,'[^0-9.]','','g'),'')::NUMERIC, 0)
                      NOT BETWEEN 0 AND 100
                 THEN 'discount_applied out of range (0-100)' END
        ),
        TO_JSONB(s)
    FROM src.sales_offline s
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


    CALL bl_cn.log_success(v_log_id, v_inserted);

    RAISE NOTICE '[load_stg_offline] Total: % | Valid: % | Rejected: %',
        v_total, v_inserted, v_rejected;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    IF v_log_id IS NOT NULL THEN
        CALL bl_cn.log_failure(v_log_id, v_err_msg);
    END IF;
    RAISE;
END;
$$;