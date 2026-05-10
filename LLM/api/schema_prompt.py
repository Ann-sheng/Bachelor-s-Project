"""
schema_prompt.py
Imported by llm_api.py — update this file whenever the DM schema changes.

Design notes:
- Technical audit columns (ta_insert_dt, ta_update_dt, source_system,
  source_entity) are intentionally omitted — they are never relevant to
  a business question and including them wastes context tokens.
- Surrogate IDs are listed only where they are join keys. They should
  never appear in WHERE filters or SELECT output.
- SCD Type 2 tables (customers, employees) must always be filtered with
  WHERE is_active = TRUE to get the current version of a record.
  This rule is stated explicitly so SQLCoder applies it automatically.
- Three date roles exist in the fact table. SQLCoder must pick the right
  one depending on what the question is about.
"""

SCHEMA_CONTEXT = """
### PostgreSQL database schema
### Schema: bl_dm (all tables are in this schema)
### The database tracks retail sales transactions with customers, employees,
### products, suppliers, store branches, and shipping details.

---

### FACT TABLE

Table: bl_dm.fct_transactions_dd
Purpose: One row per sales transaction. Central table — always start here.
Columns:
  transaction_src_id          BIGINT        -- natural primary key (transaction ID)
  product_surr_id             BIGINT        -- FK → bl_dm.dm_products
  customer_surr_id            BIGINT        -- FK → bl_dm.dm_customers_scd
  employee_surr_id            BIGINT        -- FK → bl_dm.dm_employees_scd
  store_branch_surr_id        BIGINT        -- FK → bl_dm.dm_store_branches
  shipping_surr_id            BIGINT        -- FK → bl_dm.dm_shippings
  supplier_surr_id            BIGINT        -- FK → bl_dm.dm_suppliers
  junk_surr_id                BIGINT        -- FK → bl_dm.dm_junk_transactions
  event_date_key              INTEGER       -- FK → bl_dm.dim_dates (date of sale)
  shipped_date_key            INTEGER       -- FK → bl_dm.dim_dates (date shipped, nullable)
  delivery_date_key           INTEGER       -- FK → bl_dm.dim_dates (date delivered, nullable)
  transaction_quantity_sold   INTEGER       -- units sold in this transaction
  transaction_discount_pct    NUMERIC(5,2)  -- discount applied (0–100)
  transaction_sale_amount     NUMERIC(12,2) -- total revenue for this transaction
  transaction_gross_profit    NUMERIC(12,2) -- revenue minus cost (pre-calculated)
  transaction_profit_margin   NUMERIC(8,4)  -- gross profit / sale amount (pre-calculated)

Date key rules:
  - Use event_date_key  when the question is about WHEN a sale happened.
  - Use shipped_date_key  when the question is about WHEN an order was shipped.
  - Use delivery_date_key when the question is about WHEN an order was delivered.
  - shipped_date_key and delivery_date_key can be NULL (not all orders have these dates).

---

### DATE DIMENSION

Table: bl_dm.dim_dates
Purpose: Calendar lookup — join to any date key in the fact table.
         The same table is joined up to three times (once per date role) using aliases.
Columns:
  date_key          INTEGER     -- PK, format YYYYMMDD (e.g. 20240315)
  full_date         DATE        -- actual date (e.g. 2024-03-15)
  day_of_month      INTEGER     -- 1–31
  month_num         INTEGER     -- 1–12
  month_name        VARCHAR     -- e.g. 'March'
  quarter           INTEGER     -- 1–4
  year              INTEGER     -- e.g. 2024
  day_of_week_num   INTEGER     -- 1=Monday … 7=Sunday
  day_of_week_name  VARCHAR     -- e.g. 'Monday'
  is_weekend        BOOLEAN     -- TRUE for Saturday and Sunday
  fiscal_year       INTEGER
  fiscal_quarter    INTEGER     -- 1–4

Example of joining dim_dates twice with aliases:
  JOIN bl_dm.dim_dates AS sale_date ON f.event_date_key = sale_date.date_key
  JOIN bl_dm.dim_dates AS ship_date ON f.shipped_date_key = ship_date.date_key

---

### DIMENSION TABLES

Table: bl_dm.dm_customers_scd
Purpose: Customer master — SCD Type 2 (history is kept).
IMPORTANT: Always filter WHERE is_active = TRUE to get the current customer record.
Columns:
  customer_surr_id      BIGINT   -- PK (join key)
  customer_src_id       BIGINT   -- original system ID
  customer_firstname    VARCHAR
  customer_lastname     VARCHAR
  customer_country      VARCHAR
  customer_city         VARCHAR
  customer_phone_number VARCHAR
  customer_email        VARCHAR
  start_dt              DATE     -- when this version became active
  end_dt                DATE     -- 9999-12-31 means currently active
  is_active             BOOLEAN  -- TRUE = current record; always filter on this

---

Table: bl_dm.dm_employees_scd
Purpose: Employee master — SCD Type 2 (history is kept).
IMPORTANT: Always filter WHERE is_active = TRUE to get the current employee record.
Columns:
  employee_surr_id              BIGINT
  employee_src_id               BIGINT
  employee_firstname            VARCHAR
  employee_lastname             VARCHAR
  employee_title                VARCHAR  -- job title
  employee_email                VARCHAR
  employee_phone_number         VARCHAR
  employee_salary               NUMERIC(12,2)
  employee_store_branch_src_id  BIGINT   -- which branch the employee belongs to
  employee_store_branch_state   VARCHAR
  employee_store_branch_city    VARCHAR
  start_dt                      DATE
  end_dt                        DATE
  is_active                     BOOLEAN  -- always filter WHERE is_active = TRUE

---

Table: bl_dm.dm_products
Purpose: Product catalogue — SCD Type 1 (changes overwrite, no history).
Columns:
  product_surr_id         BIGINT
  product_src_id          BIGINT
  product_category        VARCHAR   
  product_name            VARCHAR
  product_unit_cost       NUMERIC(12,2)  -- what the business pays per unit
  product_unit_price      NUMERIC(12,2)  -- what the customer pays per unit
  product_warranty_period INTEGER        -- months, nullable

---

Table: bl_dm.dm_store_branches
Purpose: Physical retail locations — SCD Type 1.
Columns:
  store_branch_surr_id          BIGINT
  store_branch_src_id           BIGINT
  store_branch_state            VARCHAR
  store_branch_city             VARCHAR
  store_branch_phone_number     VARCHAR
  store_branch_operating_days   VARCHAR  -- e.g. 'Mon-Fri'
  store_branch_operating_hours  VARCHAR  -- e.g. '09:00-18:00'

---

Table: bl_dm.dm_suppliers
Purpose: Product suppliers — SCD Type 1.
Columns:
  supplier_surr_id         BIGINT
  supplier_src_id          BIGINT
  supplier_name            VARCHAR
  supplier_email           VARCHAR
  supplier_number          VARCHAR
  supplier_primary_contact VARCHAR
  supplier_location        VARCHAR

---

Table: bl_dm.dm_shippings
Purpose: Shipping method and carrier combinations.
Columns:
  shipping_surr_id  BIGINT
  shipping_src_id   BIGINT
  shipping_method   VARCHAR   -- e.g. 'Express', 'Standard'
  shipping_carrier  VARCHAR   -- e.g. 'FedEx', 'UPS'

---

Table: bl_dm.dm_junk_transactions
Purpose: Low-cardinality transaction flags grouped into one dimension.
Columns:
  junk_surr_id          BIGINT
  junk_payment_method   VARCHAR  -- e.g. 'Credit Card', 'Cash'
  junk_currency_paid    VARCHAR  -- e.g. 'USD', 'EUR'
  junk_sales_channel    VARCHAR  -- e.g. 'Online', 'offline'
  junk_shipment_status  VARCHAR  -- e.g. 'Delivered', 'Pending', 'Cancelled'

---

### COMMON QUERY PATTERNS

-- Total revenue by product category for a given year:
SELECT p.product_category,
       SUM(f.transaction_sale_amount) AS total_revenue
FROM bl_dm.fct_transactions_dd f
JOIN bl_dm.dm_products          p  ON f.product_surr_id  = p.product_surr_id
JOIN bl_dm.dim_dates            d  ON f.event_date_key   = d.date_key
WHERE d.year = 2024
GROUP BY p.product_category
ORDER BY total_revenue DESC;

-- Top 10 customers by revenue (current records only):
SELECT c.customer_firstname, c.customer_lastname,
       SUM(f.transaction_sale_amount) AS total_spent
FROM bl_dm.fct_transactions_dd f
JOIN bl_dm.dm_customers_scd c ON f.customer_surr_id = c.customer_surr_id
WHERE c.is_active = TRUE
GROUP BY c.customer_surr_id, c.customer_firstname, c.customer_lastname
ORDER BY total_spent DESC
LIMIT 10;

-- Monthly sales summary:
SELECT d.year, d.month_num, d.month_name,
       SUM(f.transaction_sale_amount) AS revenue,
       SUM(f.transaction_gross_profit) AS gross_profit
FROM bl_dm.fct_transactions_dd f
JOIN bl_dm.dim_dates d ON f.event_date_key = d.date_key
GROUP BY d.year, d.month_num, d.month_name
ORDER BY d.year, d.month_num;
"""