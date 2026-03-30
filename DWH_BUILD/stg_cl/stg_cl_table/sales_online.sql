--  Cleaned, validated online sales data

CREATE TABLE IF NOT EXISTS stg_cln.sales_online (

    -- Customer 
    customer_id                 VARCHAR(15)     NOT NULL,
    customer_firstname          VARCHAR(50)     NOT NULL,
    customer_lastname           VARCHAR(50)     NOT NULL,
    customer_country            VARCHAR(50),
    customer_city               VARCHAR(50),
    customer_phone_number       VARCHAR(20),
    customer_email              VARCHAR(100),

    -- Product
    product_id                  VARCHAR(15)     NOT NULL,
    product_category            VARCHAR(100)    NOT NULL,
    product_name                VARCHAR(100)    NOT NULL,
    product_unit_cost           NUMERIC(12,2)   NOT NULL,
    product_unit_price          NUMERIC(12,2)   NOT NULL,
    product_warranty_period     INTEGER,

    -- Transaction
    transaction_id              VARCHAR(15)     NOT NULL,
    transaction_dt              DATE            NOT NULL,
    transaction_quantity_sold   INTEGER         NOT NULL,
    transaction_discount_pct    NUMERIC(5,2)    NOT NULL,
    transaction_sale_amount     NUMERIC(12,2)   NOT NULL,
    transaction_payment_method  VARCHAR(50)     NOT NULL,
    transaction_currency_paid   VARCHAR(10)     NOT NULL,
    transaction_sales_channel   VARCHAR(20)     NOT NULL,

    -- Employee
    employee_id                 VARCHAR(15)     NOT NULL,
    employee_firstname          VARCHAR(50)     NOT NULL,
    employee_lastname           VARCHAR(50)     NOT NULL,
    employee_title              VARCHAR(50)     NOT NULL,
    employee_email              VARCHAR(100),
    employee_phone_number       VARCHAR(20),
    employee_salary             NUMERIC(12,2)   NOT NULL,

    -- Supplier
    supplier_id                 VARCHAR(15)     NOT NULL,
    supplier_name               VARCHAR(100)    NOT NULL,
    supplier_email              VARCHAR(100),
    supplier_number             VARCHAR(20),
    supplier_primary_contact    VARCHAR(100),
    supplier_location           VARCHAR(100),

    -- Shipping 
    shipping_id                 VARCHAR(15),
    shipping_method             VARCHAR(100),
    shipping_carrier            VARCHAR(100),
    shipped_dt                  DATE,         
    delivery_dt                 DATE,          
    shipment_status             VARCHAR(50), 

    -- Metadata
    stg_insert_dt               TIMESTAMP       DEFAULT NOW(),
    src_system                  VARCHAR(10)     DEFAULT 'ONLINE',
    customer_row_hash VARCHAR(32)
        GENERATED ALWAYS AS (
            MD5(
                COALESCE(customer_firstname, '') ||
                COALESCE(customer_lastname, '') ||
                COALESCE(customer_email, '') ||
                COALESCE(customer_country, '') ||
                COALESCE(customer_city, '') ||
                COALESCE(customer_phone_number, '')
            )
        ) STORED,
    employee_row_hash VARCHAR(32)
        GENERATED ALWAYS AS (
            MD5(
                COALESCE(employee_firstname, '') ||
                COALESCE(employee_lastname, '') ||
                COALESCE(employee_title, '') ||
                COALESCE(employee_email, '') ||
                COALESCE(employee_salary::TEXT, '')
            )
        ) STORED
);