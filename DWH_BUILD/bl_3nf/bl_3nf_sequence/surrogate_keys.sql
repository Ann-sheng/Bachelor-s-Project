-- Sequences for generating surrogate keys in the 3NF layer
-- Each sequence is used for a specific dimension or entity


CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_suppliers START 1 INCREMENT 1;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_shippings START 1 INCREMENT 1;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_store_branches START 1 INCREMENT 1;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_products START 1 INCREMENT 1;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_customers_scd START 1 INCREMENT 1;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_employees_scd START 1 INCREMENT 1;

CREATE SEQUENCE IF NOT EXISTS bl_3nf.seq_ce_transactions START 1 INCREMENT 1;

