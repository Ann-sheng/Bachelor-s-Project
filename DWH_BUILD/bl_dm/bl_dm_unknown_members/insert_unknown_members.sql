
--  Insert -1 unknown/default members into all entities

BEGIN;

--  DIM_DATES
INSERT INTO bl_dm.dim_dates (
    date_key, full_date,
    day_of_month, month_num, month_name,
    quarter, year,
    day_of_week_num, day_of_week_name,
    is_weekend, fiscal_year, fiscal_quarter
)
SELECT -1, DATE '1900-01-01',
       1, 1, 'n. a.',
       1, 1900,
       1, 'n. a.',
       FALSE, 1900, 1
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dim_dates WHERE date_key = -1
);

-- DM_SUPPLIERS
INSERT INTO bl_dm.dm_suppliers (
    supplier_surr_id, supplier_src_id,
    supplier_name, supplier_email,
    supplier_number, supplier_primary_contact, supplier_location,
    ta_insert_dt, ta_update_dt, source_system, source_entity
)
SELECT -1, -1,
       'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.',
       TIMESTAMP '1900-01-01', TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dm_suppliers WHERE supplier_surr_id = -1
);

-- DM_SHIPPINGS
INSERT INTO bl_dm.dm_shippings (
    shipping_surr_id, shipping_src_id,
    shipping_method, shipping_carrier,
    ta_insert_dt, ta_update_dt, source_system, source_entity
)
SELECT -1, -1,
       'n. a.', 'n. a.',
       TIMESTAMP '1900-01-01', TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dm_shippings WHERE shipping_surr_id = -1
);

-- DM_STORE_BRANCHES
INSERT INTO bl_dm.dm_store_branches (
    store_branch_surr_id, store_branch_src_id,
    store_branch_state, store_branch_city,
    store_branch_phone_number,
    store_branch_operating_days, store_branch_operating_hours,
    ta_insert_dt, ta_update_dt, source_system, source_entity
)
SELECT -1, -1,
       'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.',
       TIMESTAMP '1900-01-01', TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dm_store_branches WHERE store_branch_surr_id = -1
);

-- DM_PRODUCTS: needs supplier_surr_id=-1 first (FK)
INSERT INTO bl_dm.dm_products (
    product_surr_id, product_src_id, supplier_surr_id,
    product_category, product_name,
    product_unit_cost, product_unit_price, product_warranty_period,
    ta_insert_dt, ta_update_dt, source_system, source_entity
)
SELECT -1, -1, -1,
       'n. a.', 'n. a.',
       0, 0, NULL,
       TIMESTAMP '1900-01-01', TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dm_products WHERE product_surr_id = -1
);

-- DM_JUNK_TRANSACTIONS
INSERT INTO bl_dm.dm_junk_transactions (
    junk_surr_id,
    junk_payment_method, junk_currency_paid,
    junk_sales_channel, junk_shipment_status,
    ta_insert_dt, ta_update_dt
)
SELECT -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.',
       TIMESTAMP '1900-01-01', TIMESTAMP '1900-01-01'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dm_junk_transactions WHERE junk_surr_id = -1
);

-- DM_CUSTOMERS_SCD
INSERT INTO bl_dm.dm_customers_scd (
    customer_surr_id, customer_src_id,
    customer_firstname, customer_lastname,
    customer_country, customer_city,
    customer_phone_number, customer_email,
    start_dt, end_dt, is_active,
    ta_insert_dt, source_system, source_entity
)
SELECT -1, -1,
       'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.',
       DATE '1900-01-01', DATE '9999-12-31', TRUE,
       TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dm_customers_scd WHERE customer_surr_id = -1
);

-- DM_EMPLOYEES_SCD
INSERT INTO bl_dm.dm_employees_scd (
    employee_surr_id, employee_src_id,
    employee_firstname, employee_lastname,
    employee_title, employee_email,
    employee_phone_number, employee_salary,
    start_dt, end_dt, is_active,
    ta_insert_dt, source_system, source_entity
)
SELECT -1, -1,
       'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', NULL,
       DATE '1900-01-01', DATE '9999-12-31', TRUE,
       TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_dm.dm_employees_scd WHERE employee_surr_id = -1
);

COMMIT;

