-- Foreign table pointing to online sales CSV file

DROP FOREIGN TABLE IF EXISTS ext_source.ext_sales_online;

CREATE FOREIGN TABLE ext_source.ext_sales_online (

    -- Customer attributes
    Customer_ID                  VARCHAR(255),
    Customer_FirstName           VARCHAR(255),
    Customer_LastName            VARCHAR(255),
    Customer_Country             VARCHAR(255),
    Customer_City                VARCHAR(255),
    Customer_Phone_Number        VARCHAR(255),    
    Customer_Email               VARCHAR(255),

    -- Product attributes
    Product_ID                   VARCHAR(255),
    Product_Category             VARCHAR(255),
    Product_Name                 VARCHAR(255),
    Unit_Cost                    VARCHAR(255),
    Unit_Price                   VARCHAR(255),
    Warranty_Period_months       VARCHAR(255),

    -- Transaction attributes
    Transaction_ID               VARCHAR(255),
    Transaction_Date             VARCHAR(255),
    Quantity_Sold                VARCHAR(255),
    Discount_Applied             VARCHAR(255),
    Sales_Amount                 VARCHAR(255),
    Payment_Method               VARCHAR(255),
    Currency_Paid                VARCHAR(255),
    Sales_Channel                VARCHAR(255),

    -- Employee attributes
    Employee_ID                  VARCHAR(255),
    Employee_FirstName           VARCHAR(255),
    Employee_LastName            VARCHAR(255),
    Employee_Title               VARCHAR(255),
    Employee_Email               VARCHAR(255),
    Employee_Phone_Number        VARCHAR(255),
    Employee_Salary              VARCHAR(255),

    -- Supplier attributes
    Supplier_ID                  VARCHAR(255),
    Supplier_Name                VARCHAR(255),
    Supplier_Email               VARCHAR(255),
    Supplier_Number              VARCHAR(255),
    Supplier_Primary_Contact     VARCHAR(255),
    Supplier_Location            VARCHAR(255),

    -- Shipping attributes  (online only)
    Shipping_ID                  VARCHAR(255),
    Shipping_Method              VARCHAR(255),
    Shipping_Carrier             VARCHAR(255),
    Shipped_Date                 VARCHAR(255),
    Delivery_Date                VARCHAR(255),
    Shipment_Status              VARCHAR(255)

)
SERVER file_server
OPTIONS (
    filename  'C:\Program Files\PostgreSQL\17\data\Online_Sales_Dataset.csv',
    format    'csv',
    header    'true',
    delimiter ',',
    null      ''
);