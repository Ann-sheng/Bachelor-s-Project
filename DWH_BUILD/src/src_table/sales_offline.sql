-- PURPOSE : Raw source table - offline sales.

CREATE TABLE IF NOT EXISTS src.sales_offline (
    -- Row-level audit
    src_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id        VARCHAR(50)     NOT NULL,
    load_type       VARCHAR(20)     NOT NULL,
    batch_dt        TIMESTAMP,
    staged_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_file     VARCHAR(500)    NOT NULL,

    -- Customer
    customer_id                  VARCHAR(255),
    customer_firstname           VARCHAR(255),
    customer_lastname            VARCHAR(255),
    customer_email               VARCHAR(255),

    -- Product
    product_id                   VARCHAR(255),
    product_category             VARCHAR(255),
    product_name                 VARCHAR(255),
    unit_cost                    VARCHAR(255),
    unit_price                   VARCHAR(255),
    warranty_period_months       VARCHAR(255),

    -- Transaction
    transaction_id               VARCHAR(255),
    transaction_date             VARCHAR(255),
    quantity_sold                VARCHAR(255),
    discount_applied             VARCHAR(255),
    sales_amount                 VARCHAR(255),
    payment_method               VARCHAR(255),
    currency_paid                VARCHAR(255),
    sales_channel                VARCHAR(255),

    -- Employee
    employee_id                  VARCHAR(255),
    employee_firstname           VARCHAR(255),
    employee_lastname            VARCHAR(255),
    employee_title               VARCHAR(255),
    employee_email               VARCHAR(255),
    employee_phone_number        VARCHAR(255),
    employee_salary              VARCHAR(255),

    -- Store Branch
    storebranch_id               VARCHAR(255),
    storebranch_state            VARCHAR(255),
    storebranch_city             VARCHAR(255),
    storebranch_phone_number     VARCHAR(255),
    storebranch_operating_days   VARCHAR(255),
    storebranch_operating_hours  VARCHAR(255),

    -- Supplier
    supplier_id                  VARCHAR(255),
    supplier_name                VARCHAR(255),
    supplier_email               VARCHAR(255),
    supplier_number              VARCHAR(255),
    supplier_primary_contact     VARCHAR(255),
    supplier_location            VARCHAR(255),

    -- Metadata
    src_insert_dt                TIMESTAMP DEFAULT NOW(),
    src_filename                 VARCHAR(255) DEFAULT 'Offline_Sales_Dataset.csv'

);