-- Supports joins and lookups using transaction_id.
CREATE INDEX IF NOT EXISTS idx_stg_off_transaction_id ON stg_cln.sales_offline (transaction_id);

-- Supports customer-based searches, aggregations
CREATE INDEX IF NOT EXISTS idx_stg_off_customer_id ON stg_cln.sales_offline (customer_id);

-- Supports up product-level analysis, reporting
CREATE INDEX IF NOT EXISTS idx_stg_off_product_id ON stg_cln.sales_offline (product_id);

-- Supports employee-related reporting
CREATE INDEX IF NOT EXISTS idx_stg_off_employee_id ON stg_cln.sales_offline (employee_id);

-- Supports filtering, sorting, and range queries
CREATE INDEX IF NOT EXISTS idx_stg_off_transaction_dt ON stg_cln.sales_offline (transaction_dt);

