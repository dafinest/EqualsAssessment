--Total Transaction Amount by Customer
SELECT 
    c.Customer_Name,
    SUM(f.Transaction_Amount) AS Total_Transaction_Amount
FROM 
    Financial_Transactions_Fact f
JOIN 
    Customer_Dim c ON f.Customer_ID = c.Customer_ID
GROUP BY 
    c.Customer_Name
ORDER BY 
    Total_Transaction_Amount DESC;

--Average Transaction Amount by Branch
SELECT 
    b.Branch_Name,
    AVG(f.Transaction_Amount) AS Average_Transaction_Amount
FROM 
    Financial_Transactions_Fact f
JOIN 
    Branch_Dim b ON f.Branch_ID = b.Branch_ID
GROUP BY 
    b.Branch_Name
ORDER BY 
    Average_Transaction_Amount DESC;

--Monthly Transactions by Account Type
SELECT 
    a.Account_Type,
    DATE_TRUNC('month', f.Transaction_Date) AS Transaction_Month,
    COUNT(f.Transaction_ID) AS Transaction_Count
FROM 
    Financial_Transactions_Fact f
JOIN 
    Account_Dim a ON f.Account_ID = a.Account_ID
GROUP BY 
    a.Account_Type, DATE_TRUNC('month', f.Transaction_Date)
ORDER BY 
    Transaction_Month, a.Account_Type;


--Total Transaction Fees Collected by Currency Type
SELECT 
    cu.Currency_Type,
    SUM(f.Transaction_Fee) AS Total_Fees_Collected
FROM 
    Financial_Transactions_Fact f
JOIN 
    Currency_Dim cu ON f.Currency_Type = cu.Currency_Type
GROUP BY 
    cu.Currency_Type
ORDER BY 
    Total_Fees_Collected DESC;

--Customer Lifetime Value by Customer
SELECT 
    c.Customer_Name,
    SUM(f.Transaction_Amount) AS Lifetime_Value
FROM 
    Financial_Transactions_Fact f
JOIN 
    Customer_Dim c ON f.Customer_ID = c.Customer_ID
GROUP BY 
    c.Customer_Name
ORDER BY 
    Lifetime_Value DESC;


