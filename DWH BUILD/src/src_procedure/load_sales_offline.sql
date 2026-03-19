
-- Load procedure - ext_source → src_offline

CREATE OR REPLACE PROCEDURE src.load_sales_offline()
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id      BIGINT;
    v_inserted    INT := 0;
    v_err_msg     TEXT;
BEGIN
       v_log_id := bl_cn.log_start('src.load_sales_offline', 'SRC');

    TRUNCATE TABLE src.sales_offline;

    INSERT INTO src.sales_offline (
        Customer_ID,
        Customer_FirstName,
        Customer_LastName,
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
        StoreBranch_ID,
        StoreBranch_State,
        StoreBranch_City,
        StoreBranch_Phone_Number,
        StoreBranch_Operating_Days,
        StoreBranch_Operating_Hours,
        Supplier_ID,
        Supplier_Name,
        Supplier_Email,
        Supplier_Number,
        Supplier_Primary_Contact,
        Supplier_Location
    )
    SELECT
        Customer_ID,
        Customer_FirstName,
        Customer_LastName,
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
        StoreBranch_ID,
        StoreBranch_State,
        StoreBranch_City,
        StoreBranch_Phone_Number,
        StoreBranch_Operating_Days,
        StoreBranch_Operating_Hours,
        Supplier_ID,
        Supplier_Name,
        Supplier_Email,
        Supplier_Number,
        Supplier_Primary_Contact,
        Supplier_Location
    FROM ext_source.ext_sales_offline;

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    CALL bl_cn.log_success(v_log_id, v_affected);

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT;
    CALL bl_cn.log_failure(v_log_id, v_err_msg);
    RAISE;
END;
$$;