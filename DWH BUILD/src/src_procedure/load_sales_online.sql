-- Load procedure - ext_source → src_online

CREATE OR REPLACE PROCEDURE src.load_sales_online()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id      BIGINT;
    v_inserted    INT := 0;
    v_err_msg     TEXT;
BEGIN
       v_log_id := bl_cn.log_start('src.load_sales_online', 'SRC');

    TRUNCATE TABLE src.sales_online;

    INSERT INTO src.sales_online (
        Customer_ID,
        Customer_FirstName,
        Customer_LastName,
        Customer_Country,
        Customer_City,
        Customer_Phone_Number,
        Customer_Email,
        Product_ID,
        Product_Category,
        Product_Name,
        Unit_Cost,
        Unit_Price,
        Warranty_Period_months,
        Transaction_ID,
        Transaction_Date,
        Quantity_Sold,
        Discount_Applied,
        Sales_Amount,
        Payment_Method,
        Currency_Paid,
        Sales_Channel,
        Employee_ID,
        Employee_FirstName,
        Employee_LastName,
        Employee_Title,
        Employee_Email,
        Employee_Phone_Number,
        Employee_Salary,
        Supplier_ID,
        Supplier_Name,
        Supplier_Email,
        Supplier_Number,
        Supplier_Primary_Contact,
        Supplier_Location,
        Shipping_ID,
        Shipping_Method,
        Shipping_Carrier,
        Shipped_Date,
        Delivery_Date,
        Shipment_Status
    )
    SELECT
        Customer_ID,
        Customer_FirstName,
        Customer_LastName,
        Customer_Country,
        Customer_City,
        Customer_Phone_Number,
        Customer_Email,
        Product_ID,
        Product_Category,
        Product_Name,
        Unit_Cost,
        Unit_Price,
        Warranty_Period_months,
        Transaction_ID,
        Transaction_Date,
        Quantity_Sold,
        Discount_Applied,
        Sales_Amount,
        Payment_Method,
        Currency_Paid,
        Sales_Channel,
        Employee_ID,
        Employee_FirstName,
        Employee_LastName,
        Employee_Title,
        Employee_Email,
        Employee_Phone_Number,
        Employee_Salary,
        Supplier_ID,
        Supplier_Name,
        Supplier_Email,
        Supplier_Number,
        Supplier_Primary_Contact,
        Supplier_Location,
        Shipping_ID,
        Shipping_Method,
        Shipping_Carrier,
        Shipped_Date,
        Delivery_Date,
        Shipment_Status
    FROM ext_source.ext_sales_online;

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    CALL bl_cn.log_success(v_log_id, v_affected);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;



