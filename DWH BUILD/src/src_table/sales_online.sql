-- Raw source table - online sales

CREATE TABLE IF NOT EXISTS src.sales_online (

    -- Customer 
    Customer_ID                  VARCHAR(255),
    Customer_FirstName           VARCHAR(255),
    Customer_LastName            VARCHAR(255),
    Customer_Country             VARCHAR(255),
    Customer_City                VARCHAR(255),
    Customer_Phone_Number        VARCHAR(255),  
    Customer_Email               VARCHAR(255),

    -- Product
    Product_ID                   VARCHAR(255),
    Product_Category             VARCHAR(255),
    Product_Name                 VARCHAR(255),
    Unit_Cost                    VARCHAR(255),
    Unit_Price                   VARCHAR(255),
    Warranty_Period_months       VARCHAR(255),


    -- Transaction
    Transaction_ID               VARCHAR(255),
    Transaction_Date             VARCHAR(255),
    Quantity_Sold                VARCHAR(255),
    Discount_Applied             VARCHAR(255),
    Sales_Amount                 VARCHAR(255),
    Payment_Method               VARCHAR(255),
    Currency_Paid                VARCHAR(255),
    Sales_Channel                VARCHAR(255),

    -- Employee
    Employee_ID                  VARCHAR(255),
    Employee_FirstName           VARCHAR(255),
    Employee_LastName            VARCHAR(255),
    Employee_Title               VARCHAR(255),
    Employee_Email               VARCHAR(255),
    Employee_Phone_Number        VARCHAR(255),
    Employee_Salary              VARCHAR(255),


    -- Supplier
    Supplier_ID                  VARCHAR(255),
    Supplier_Name                VARCHAR(255),
    Supplier_Email               VARCHAR(255),
    Supplier_Number              VARCHAR(255),
    Supplier_Primary_Contact     VARCHAR(255),
    Supplier_Location            VARCHAR(255),


    -- Shipping  
    Shipping_ID                  VARCHAR(255),
    Shipping_Method              VARCHAR(255),
    Shipping_Carrier             VARCHAR(255),
    Shipped_Date                 VARCHAR(255),
    Delivery_Date                VARCHAR(255),
    Shipment_Status              VARCHAR(255),

    -- Metadata
    src_insert_dt                TIMESTAMP DEFAULT NOW(),
    src_filename                 VARCHAR(255) DEFAULT 'Online_Sales_Dataset.csv'
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
        ) STORED;
);