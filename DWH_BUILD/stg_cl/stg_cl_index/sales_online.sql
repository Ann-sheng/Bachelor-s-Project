

CREATE INDEX IF NOT EXISTS idx_stg_onl_transaction_id ON stg_cln.sales_online (transaction_id);

CREATE INDEX IF NOT EXISTS idx_stg_onl_customer_id ON stg_cln.sales_online (customer_id);

CREATE INDEX IF NOT EXISTS idx_stg_onl_product_id ON stg_cln.sales_online (product_id);

CREATE INDEX IF NOT EXISTS idx_stg_onl_employee_id ON stg_cln.sales_online (employee_id);

CREATE INDEX IF NOT EXISTS idx_stg_onl_transaction_dt ON stg_cln.sales_online (transaction_dt);


