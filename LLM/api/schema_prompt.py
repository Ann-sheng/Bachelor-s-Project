SCHEMA_CONTEXT = """
### PostgreSQL schema: bl_dm
### Retail sales warehouse. Always start from the fact table.

### FACT TABLE
Table: bl_dm.fct_transactions_dd
Columns:
  transaction_src_id        BIGINT
  product_surr_id           BIGINT   -- FK → dm_products
  customer_surr_id          BIGINT   -- FK → dm_customers_scd
  employee_surr_id          BIGINT   -- FK → v_employees_public
  store_branch_surr_id      BIGINT   -- FK → dm_store_branches
  shipping_surr_id          BIGINT   -- FK → dm_shippings
  supplier_surr_id          BIGINT   -- FK → dm_suppliers
  junk_surr_id              BIGINT   -- FK → dm_junk_transactions
  event_date_key            INTEGER  -- FK → dim_dates (YYYYMMDD integer, NOT a date)
  shipped_date_key          INTEGER  -- FK → dim_dates (YYYYMMDD integer, nullable)
  delivery_date_key         INTEGER  -- FK → dim_dates (YYYYMMDD integer, nullable)
  transaction_quantity_sold INTEGER
  transaction_discount_pct  NUMERIC(5,2)
  transaction_sale_amount   NUMERIC(12,2)
  transaction_gross_profit  NUMERIC(12,2)
  transaction_profit_margin NUMERIC(8,4)  -- pre-computed margin ratio. Use this directly for any "profit margin" question.

---
### DATE DIMENSION
Table: bl_dm.dim_dates
-- To filter or group by any date part, JOIN dim_dates on date_key and use its columns.
Columns:
  date_key         INTEGER  -- PK, YYYYMMDD
  full_date        DATE
  day_of_month     INTEGER
  month_num        INTEGER  -- 1-12
  month_name       VARCHAR
  quarter          INTEGER  -- 1-4
  year             INTEGER
  day_of_week_num  INTEGER  -- 1=Monday, 7=Sunday
  day_of_week_name VARCHAR
  is_weekend       BOOLEAN  -- TRUE = Saturday or Sunday
  fiscal_year      INTEGER
  fiscal_quarter   INTEGER

-- To filter by year:    JOIN bl_dm.dim_dates d ON f.event_date_key = d.date_key, then WHERE d.year = 2023
-- To group by month:    GROUP BY d.year, d.month_num, d.month_name
-- To group by quarter:  GROUP BY d.year, d.quarter
-- To filter weekends:   WHERE d.is_weekend = TRUE

---
### DIMENSIONS

Table: bl_dm.dm_customers_scd
-- SCD2 table. Always filter: AND c.is_active = TRUE
-- For "unique customers" counts, use customer_src_id (a real customer can have multiple surr_id rows across SCD2 history).
Columns:
  customer_surr_id      BIGINT   -- unique per SCD2 version
  customer_src_id       BIGINT   -- unique per real customer; use this for distinct customer counts
  customer_firstname    VARCHAR
  customer_lastname     VARCHAR
  customer_country      VARCHAR
  customer_city         VARCHAR
  customer_phone_number VARCHAR
  customer_email        VARCHAR
  is_active             BOOLEAN

---
Table: bl_dm.v_employees_public
-- View on SCD2 employee table. Already filters current rows; still safe to add AND e.is_active = TRUE.
-- Employee location is on this table (employee_store_branch_state/city). Do not join dm_store_branches for it.
-- For "unique employees" counts, use employee_src_id.
-- employee_salary is always NULL (masked for privacy).
Columns:
  employee_surr_id             BIGINT   -- unique per SCD2 version
  employee_src_id              BIGINT   -- unique per real employee
  employee_firstname           VARCHAR
  employee_lastname            VARCHAR
  employee_title               VARCHAR  -- job role, e.g. 'Store Manager', 'Cashier'. Group by this for title-level questions.
  employee_email               VARCHAR
  employee_phone_number        VARCHAR
  employee_salary              NUMERIC  -- always NULL (masked)
  employee_store_branch_src_id BIGINT
  employee_store_branch_state  VARCHAR  -- e.g. 'Ny', 'Ca'
  employee_store_branch_city   VARCHAR
  is_active                    BOOLEAN

---
Table: bl_dm.dm_products
Columns:
  product_surr_id         BIGINT
  product_src_id          BIGINT
  product_category        VARCHAR
  product_name            VARCHAR
  product_unit_cost       NUMERIC(12,2)
  product_unit_price      NUMERIC(12,2)
  product_warranty_period INTEGER

---
Table: bl_dm.dm_store_branches
-- State values are abbreviations only: 'Ca', 'Ny', 'Tx', 'Fl'
-- No store_branch_name column — identify branches by store_branch_city.
Columns:
  store_branch_surr_id         BIGINT
  store_branch_src_id          BIGINT
  store_branch_state           VARCHAR
  store_branch_city            VARCHAR
  store_branch_phone_number    VARCHAR
  store_branch_operating_days  VARCHAR
  store_branch_operating_hours VARCHAR

---
Table: bl_dm.dm_suppliers
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
Columns:
  shipping_surr_id BIGINT
  shipping_src_id  BIGINT
  shipping_method  VARCHAR  -- 'Express Shipping', 'Standard Shipping'
  shipping_carrier VARCHAR

---
Table: bl_dm.dm_junk_transactions
-- Payment method, currency, sales channel, and shipment status live here only.
Columns:
  junk_surr_id         BIGINT
  junk_payment_method  VARCHAR  -- 'Credit Card', 'Cash', 'PayPal', 'Debit Card'
  junk_currency_paid   VARCHAR  -- 'USD', 'EUR'
  junk_sales_channel   VARCHAR  -- 'In-Store', 'Website', 'Mobile App'
  junk_shipment_status VARCHAR  -- 'Delivered', 'Pending', 'Cancelled', 'Returned'

---
### EXAMPLES

-- Monthly revenue and profit:
SELECT d.year, d.month_num, d.month_name,
       SUM(f.transaction_sale_amount)  AS revenue,
       SUM(f.transaction_gross_profit) AS gross_profit
FROM bl_dm.fct_transactions_dd f
JOIN bl_dm.dim_dates d ON f.event_date_key = d.date_key
GROUP BY d.year, d.month_num, d.month_name
ORDER BY d.year, d.month_num

-- Revenue by payment method:
SELECT j.junk_payment_method,
       SUM(f.transaction_sale_amount) AS total_revenue
FROM bl_dm.fct_transactions_dd f
JOIN bl_dm.dm_junk_transactions j ON f.junk_surr_id = j.junk_surr_id
GROUP BY j.junk_payment_method
ORDER BY total_revenue DESC

-- Top customers by total spending (note SCD2 filter):
SELECT c.customer_firstname, c.customer_lastname,
       SUM(f.transaction_sale_amount) AS total_spending
FROM bl_dm.fct_transactions_dd f
JOIN bl_dm.dm_customers_scd c ON f.customer_surr_id = c.customer_surr_id
                              AND c.is_active = TRUE
GROUP BY c.customer_src_id, c.customer_firstname, c.customer_lastname
ORDER BY total_spending DESC
LIMIT 5

-- Revenue by year and product category (multi-join with date):
SELECT d.year, p.product_category,
       SUM(f.transaction_sale_amount) AS total_revenue
FROM bl_dm.fct_transactions_dd f
JOIN bl_dm.dim_dates d   ON f.event_date_key = d.date_key
JOIN bl_dm.dm_products p ON f.product_surr_id = p.product_surr_id
GROUP BY d.year, p.product_category
ORDER BY d.year, total_revenue DESC
"""