

-- PURPOSE : Junk dimension - low-cardinality transaction flags Combines: payment_method, currency, sales_channel, shipment_status into single degenerate dimension

CREATE TABLE IF NOT EXISTS bl_dm.dm_junk_transactions (
    junk_surr_id                 BIGINT       PRIMARY KEY
                                 DEFAULT nextval('bl_dm.sq_junk_surr_id'),
    junk_payment_method          VARCHAR(50),
    junk_currency_paid           VARCHAR(10),
    junk_sales_channel           VARCHAR(20),
    junk_shipment_status         VARCHAR(50),
    ta_insert_dt                 TIMESTAMP    NOT NULL DEFAULT NOW(),
    ta_update_dt                 TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dm_junk UNIQUE (junk_payment_method, junk_currency_paid, junk_sales_channel, junk_shipment_status)
);

