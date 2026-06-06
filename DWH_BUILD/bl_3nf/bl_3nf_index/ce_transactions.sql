
-- Fast lookup  by transaction ID
CREATE INDEX IF NOT EXISTS idx_ce_tx_dt  ON bl_3nf.ce_transactions (transaction_dt);

-- Fast lookup  by customer ID
CREATE INDEX IF NOT EXISTS idx_ce_tx_customer  ON bl_3nf.ce_transactions (customer_id);

-- Fast lookup  by product ID
CREATE INDEX IF NOT EXISTS idx_ce_tx_product ON bl_3nf.ce_transactions (product_id);

-- Fast lookup  by employee ID
CREATE INDEX IF NOT EXISTS idx_ce_tx_employee ON bl_3nf.ce_transactions (employee_id);

-- Fast lookup  by store ID
CREATE INDEX IF NOT EXISTS idx_ce_tx_store ON bl_3nf.ce_transactions (store_branch_id);