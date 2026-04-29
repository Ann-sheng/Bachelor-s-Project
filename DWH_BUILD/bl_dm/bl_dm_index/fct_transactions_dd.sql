-- indexes to speed up selects
CREATE INDEX IF NOT EXISTS idx_fct_event_dt ON bl_dm.fct_transactions_dd (event_date_key);

CREATE INDEX IF NOT EXISTS idx_fct_product ON bl_dm.fct_transactions_dd (product_surr_id);

CREATE INDEX IF NOT EXISTS idx_fct_customer  ON bl_dm.fct_transactions_dd (customer_surr_id);

CREATE INDEX IF NOT EXISTS idx_fct_employee  ON bl_dm.fct_transactions_dd (employee_surr_id);

CREATE INDEX IF NOT EXISTS idx_fct_store ON bl_dm.fct_transactions_dd (store_branch_surr_id);






