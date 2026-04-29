
--  Insert -1 unknown/default members into all entities

BEGIN;

-- CE_SUPPLIERS unknown member
INSERT INTO bl_3nf.ce_suppliers (
    supplier_id, supplier_src_id,
    supplier_name, supplier_email,
    supplier_number, supplier_primary_contact, supplier_location,
    ta_insert_dt, ta_update_dt,
    source_system, source_entity
)
SELECT -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.',
       TIMESTAMP '1900-01-01', TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_suppliers WHERE supplier_id = -1
);

-- CE_SHIPPINGS unknown member 
INSERT INTO bl_3nf.ce_shippings (
    shipping_id, shipping_src_id,
    shipping_method, shipping_carrier,
    ta_insert_dt, ta_update_dt,
    source_system, source_entity
)
SELECT -1, 'n. a.', 'n. a.', 'n. a.',
       TIMESTAMP '1900-01-01', TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_shippings WHERE shipping_id = -1
);

-- CE_STORE_BRANCHES unknown member
INSERT INTO bl_3nf.ce_store_branches (
    store_branch_id, store_branch_src_id,
    store_branch_state, store_branch_city,
    store_branch_phone_number,
    store_branch_operating_days, store_branch_operating_hours,
    ta_insert_dt, ta_update_dt,
    source_system, source_entity
)
SELECT -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.',
       TIMESTAMP '1900-01-01', TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_store_branches WHERE store_branch_id = -1
);

--  CE_PRODUCTS unknown member
INSERT INTO bl_3nf.ce_products (
    product_id, product_src_id, supplier_id,
    product_category, product_name,
    product_unit_cost, product_unit_price, product_warranty_period,
    ta_insert_dt, ta_update_dt,
    source_system, source_entity
)
SELECT -1, 'n. a.', -1,
       'n. a.', 'n. a.',
       0, 0, NULL,
       TIMESTAMP '1900-01-01', TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_products WHERE product_id = -1
);

-- CE_CUSTOMERS_SCD unknown member
INSERT INTO bl_3nf.ce_customers_scd (
    customer_id, customer_src_id,
    customer_firstname, customer_lastname,
    customer_country, customer_city,
    customer_phone_number, customer_email,
    start_dt, end_dt, is_active,
    customer_row_hash, ta_insert_dt,
    source_system, source_entity
)
SELECT -1, 'n. a.',
       'n. a.', 'n. a.',
       'n. a.', 'n. a.',
       'n. a.', 'n. a.',
       DATE '1900-01-01', DATE '9999-12-31', TRUE,
       'n. a.', TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_customers_scd WHERE customer_id = -1
);

-- CE_EMPLOYEES_SCD unknown member
INSERT INTO bl_3nf.ce_employees_scd (
    employee_id, employee_src_id, store_branch_id,
    employee_firstname, employee_lastname,
    employee_title, employee_email,
    employee_phone_number, employee_salary,
    start_dt, end_dt, is_active,
    employee_row_hash, ta_insert_dt,
    source_system, source_entity
)
SELECT -1, 'n. a.', -1,
       'n. a.', 'n. a.',
       'n. a.', 'n. a.',
       'n. a.', NULL,
       DATE '1900-01-01', DATE '9999-12-31', TRUE,
       'n. a.', TIMESTAMP '1900-01-01',
       'MANUAL', 'MANUAL'
WHERE NOT EXISTS (
    SELECT 1 FROM bl_3nf.ce_employees_scd WHERE employee_id = -1
);

COMMIT;