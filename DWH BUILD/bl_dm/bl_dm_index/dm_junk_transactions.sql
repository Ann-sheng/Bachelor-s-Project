

CREATE INDEX IF NOT EXISTS idx_dm_junk_lookup
    ON bl_dm.dm_junk_transactions (
        junk_payment_method,
        junk_currency_paid,
        junk_sales_channel,
        junk_shipment_status
    );



