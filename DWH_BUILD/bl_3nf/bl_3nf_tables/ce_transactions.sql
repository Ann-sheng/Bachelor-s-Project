
-- Core transaction fact entity in 3NF

CREATE TABLE IF NOT EXISTS bl_3nf.ce_transactions (
    transaction_id              BIGINT          PRIMARY KEY
                                DEFAULT nextval('bl_3nf.seq_ce_transactions'),
    transaction_src_id          VARCHAR(15)     NOT NULL,
    product_id                  BIGINT          NOT NULL
                                REFERENCES bl_3nf.ce_products(product_id),
    shipping_id                 BIGINT          NOT NULL
                                REFERENCES bl_3nf.ce_shippings(shipping_id),
    store_branch_id             BIGINT          NOT NULL
                                REFERENCES bl_3nf.ce_store_branches(store_branch_id),
    customer_id                 BIGINT          NOT NULL
                                REFERENCES bl_3nf.ce_customers_scd(customer_id),
    employee_id                 BIGINT          NOT NULL
                                REFERENCES bl_3nf.ce_employees_scd(employee_id),
    transaction_dt              DATE            NOT NULL,
    transaction_payment_method  VARCHAR(50)     NOT NULL,
    transaction_quantity_sold   INTEGER         NOT NULL,
    transaction_discount_pct    NUMERIC(5,2)    NOT NULL DEFAULT 0,
    transaction_sale_amount     NUMERIC(12,2)   NOT NULL,
    transaction_currency_paid   VARCHAR(10)     NOT NULL,
    transaction_sales_channel   VARCHAR(20)     NOT NULL,
    transaction_shipment_status VARCHAR(50),
    transaction_shipped_dt      DATE,
    transaction_delivery_dt     DATE,
    ta_insert_dt                TIMESTAMP       NOT NULL DEFAULT NOW(),
    ta_update_dt                TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_system               VARCHAR(20)     NOT NULL,
    source_entity               VARCHAR(50)     NOT NULL,

    CONSTRAINT uq_ce_transactions  UNIQUE (transaction_src_id, source_system, source_entity)
);

