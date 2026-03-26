
-- Populate DIM_DATES from transaction date range in BL_3NF


CREATE OR REPLACE PROCEDURE bl_dm.load_dim_dates()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id        BIGINT;
    v_total         INT := 0;
    v_rows          INT := 0;
    v_start_date    DATE;
    v_end_date      DATE;
    v_current_dt    DATE;
    v_fiscal_year   INTEGER;
    v_fiscal_qtr    INTEGER;
    v_err_msg       TEXT;
BEGIN
    v_log_id := bl_cn.log_start('bl_dm.load_dim_dates', 'BL_DM');
    SELECT
        LEAST(
            MIN(transaction_dt),
            MIN(transaction_shipped_dt),
            MIN(transaction_delivery_dt)
        ),
        GREATEST(
            MAX(transaction_dt),
            MAX(transaction_shipped_dt),
            MAX(transaction_delivery_dt)
        )
    INTO v_start_date, v_end_date
    FROM bl_3nf.ce_transactions
    WHERE transaction_dt IS NOT NULL;

    IF v_start_date IS NULL THEN
        RAISE NOTICE '[load_dim_dates] No transactions found, skipping.';
        CALL bl_cn.log_success(v_log_id, 0);
        RETURN;
    END IF;

    RAISE NOTICE '[load_dim_dates] Populating date range: % to %',
        v_start_date, v_end_date;

    v_current_dt := v_start_date;

    WHILE v_current_dt <= v_end_date LOOP
        fiscal_year_calc: BEGIN
            IF EXTRACT(MONTH FROM v_current_dt) >= 2 THEN
                v_fiscal_year := EXTRACT(YEAR FROM v_current_dt)::INTEGER;
            ELSE
                v_fiscal_year := (EXTRACT(YEAR FROM v_current_dt) - 1)::INTEGER;
            END IF;
        END fiscal_year_calc;

        CASE
            WHEN EXTRACT(MONTH FROM v_current_dt) IN (2,3,4)   THEN v_fiscal_qtr := 1;
            WHEN EXTRACT(MONTH FROM v_current_dt) IN (5,6,7)   THEN v_fiscal_qtr := 2;
            WHEN EXTRACT(MONTH FROM v_current_dt) IN (8,9,10)  THEN v_fiscal_qtr := 3;
            ELSE  v_fiscal_qtr := 4;  
        END CASE;

        INSERT INTO bl_dm.dim_dates (
            date_key,
            full_date,
            day_of_month,
            month_num,
            month_name,
            quarter,
            year,
            day_of_week_num,
            day_of_week_name,
            is_weekend,
            fiscal_year,
            fiscal_quarter
        )
        VALUES (
            TO_CHAR(v_current_dt, 'YYYYMMDD')::INTEGER,
            v_current_dt,
            EXTRACT(DAY  FROM v_current_dt)::INTEGER,
            EXTRACT(MONTH FROM v_current_dt)::INTEGER,
            TRIM(TO_CHAR(v_current_dt, 'Month')),
            EXTRACT(QUARTER FROM v_current_dt)::INTEGER,
            EXTRACT(YEAR  FROM v_current_dt)::INTEGER,
            EXTRACT(ISODOW FROM v_current_dt)::INTEGER,
            TRIM(TO_CHAR(v_current_dt, 'Day')),
            EXTRACT(ISODOW FROM v_current_dt) IN (6, 7),  
            v_fiscal_year,
            v_fiscal_qtr
        )
        ON CONFLICT (date_key) DO NOTHING;

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total := v_total + v_rows;

        v_current_dt := v_current_dt + INTERVAL '1 day';
    END LOOP;


    CALL bl_cn.log_success(v_log_id, v_total);

    RAISE NOTICE '[load_dim_dates] Inserted: % new date rows', v_total;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;

