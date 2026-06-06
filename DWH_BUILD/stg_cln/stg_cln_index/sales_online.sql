
-- Supports joins and lookups using transaction_id.
CREATE INDEX IF NOT EXISTS idx_stg_onl_transaction_id ON stg_cln.sales_online (transaction_id);

-- Supports customer-based searches, aggregations,
CREATE INDEX IF NOT EXISTS idx_stg_onl_customer_id ON stg_cln.sales_online (customer_id);

-- Supports up product-level analysis, reporting,
CREATE INDEX IF NOT EXISTS idx_stg_onl_product_id ON stg_cln.sales_online (product_id);

-- Supports employee-related reporting,
CREATE INDEX IF NOT EXISTS idx_stg_onl_employee_id ON stg_cln.sales_online (employee_id);

-- Supports filtering, sorting, and range queries
CREATE INDEX IF NOT EXISTS idx_stg_onl_transaction_dt ON stg_cln.sales_online (transaction_dt);


