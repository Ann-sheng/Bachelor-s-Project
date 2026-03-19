
CREATE INDEX IF NOT EXISTS idx_stg_off_transaction_id ON stg_cln.sales_offline (transaction_id);

CREATE INDEX IF NOT EXISTS idx_stg_off_customer_id ON stg_cln.sales_offline (customer_id);

CREATE INDEX IF NOT EXISTS idx_stg_off_product_id ON stg_cln.sales_offline (product_id);

CREATE INDEX IF NOT EXISTS idx_stg_off_employee_id ON stg_cln.sales_offline (employee_id);

CREATE INDEX IF NOT EXISTS idx_stg_off_transaction_dt ON stg_cln.sales_offline (transaction_dt);
