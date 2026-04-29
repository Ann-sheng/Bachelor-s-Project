

--  Central fact table - one row per transaction, DD = Degenerate Dimension (transaction_src_id stored)
--           Gross profit and margin are DERIVED measures

CREATE TABLE IF NOT EXISTS bl_dm.fct_transactions_dd (
    transaction_src_id          BIGINT          PRIMARY KEY,  
    product_surr_id             BIGINT          NOT NULL
                                REFERENCES bl_dm.dm_products(product_surr_id),
    customer_surr_id            BIGINT          NOT NULL
                                REFERENCES bl_dm.dm_customers_scd(customer_surr_id),
    employee_surr_id            BIGINT          NOT NULL
                                REFERENCES bl_dm.dm_employees_scd(employee_surr_id),
    store_branch_surr_id        BIGINT          NOT NULL
                                REFERENCES bl_dm.dm_store_branches(store_branch_surr_id),
    shipping_surr_id            BIGINT          NOT NULL
                                REFERENCES bl_dm.dm_shippings(shipping_surr_id),
    supplier_surr_id            BIGINT          NOT NULL
                                REFERENCES bl_dm.dm_suppliers(supplier_surr_id),                              
    junk_surr_id                BIGINT          NOT NULL
                                REFERENCES bl_dm.dm_junk_transactions(junk_surr_id),
    event_date_key              INTEGER         NOT NULL
                                REFERENCES bl_dm.dim_dates(date_key),
    shipped_date_key            INTEGER
                                REFERENCES bl_dm.dim_dates(date_key),
    delivery_date_key           INTEGER
                                REFERENCES bl_dm.dim_dates(date_key),
    transaction_quantity_sold   INTEGER         NOT NULL DEFAULT 0,
    transaction_discount_pct    NUMERIC(5,2)    NOT NULL DEFAULT 0,
    transaction_sale_amount     NUMERIC(12,2)   NOT NULL DEFAULT 0,
    transaction_gross_profit    NUMERIC(12,2),
    transaction_profit_margin   NUMERIC(8,4), 
    ta_insert_dt                TIMESTAMP       NOT NULL DEFAULT NOW(),
    ta_update_dt                TIMESTAMP       NOT NULL DEFAULT NOW()
);

