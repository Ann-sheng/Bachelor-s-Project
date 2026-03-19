
-- Load unique attribute combinations into junk dimension, INSERT-only: combinations are immutable

CREATE OR REPLACE PROCEDURE bl_dm.load_dm_junk_transactions()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id   BIGINT;
    v_inserted INT := 0;
    v_err_msg  TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_dm_junk_transactions', 'BL_DM');

    INSERT INTO bl_dm.dm_junk_transactions (
        junk_surr_id,
        junk_payment_method,
        junk_currency_paid,
        junk_sales_channel,
        junk_shipment_status,
        ta_insert_dt,
        ta_update_dt
    )
    SELECT
        nextval('bl_dm.sq_junk_surr_id'),
        t.transaction_payment_method,
        t.transaction_currency_paid,
        t.transaction_sales_channel,
        t.transaction_shipment_status,
        NOW(),
        NOW()
    FROM (
        SELECT DISTINCT
            transaction_payment_method,
            transaction_currency_paid,
            transaction_sales_channel,
            transaction_shipment_status
        FROM bl_3nf.ce_transactions
    ) t
    WHERE NOT EXISTS (
        SELECT 1 FROM bl_dm.dm_junk_transactions j
        WHERE j.junk_payment_method  IS NOT DISTINCT FROM t.transaction_payment_method
          AND j.junk_currency_paid   IS NOT DISTINCT FROM t.transaction_currency_paid
          AND j.junk_sales_channel   IS NOT DISTINCT FROM t.transaction_sales_channel
          AND j.junk_shipment_status IS NOT DISTINCT FROM t.transaction_shipment_status
    );
  
    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    UPDATE bl_cn.etl_log
    SET status = 'SUCCESS', rows_inserted = v_inserted, finished_at = NOW()
    WHERE log_id = v_log_id;

    RAISE NOTICE '[load_dm_junk_transactions] Inserted: % new combinations', v_inserted;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    UPDATE bl_cn.etl_log
    SET status = 'FAILED', error_message = v_err_msg, finished_at = NOW()
    WHERE log_id = v_log_id;
    RAISE EXCEPTION '[load_dm_junk_transactions] FAILED: %', v_err_msg;
END;
$$;

