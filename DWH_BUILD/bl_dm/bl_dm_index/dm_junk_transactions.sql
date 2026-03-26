

CREATE UNIQUE INDEX IF NOT EXISTS uq_dm_junk
    ON bl_dm.dm_junk_transactions (
        COALESCE(junk_payment_method,  'n. a.'),
        COALESCE(junk_currency_paid,   'n. a.'),
        COALESCE(junk_sales_channel,   'n. a.'),
        COALESCE(junk_shipment_status, 'n. a.')
    );


