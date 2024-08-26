import psycopg2
from psycopg2 import sql

# Database connection setup
conn = psycopg2.connect(
    dbname="EqualsDataWarehouse",   
    user="postgres",        # put own username 
    password="password",       # put own password
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# Query to insert valid rows from staging table into the fact table
insert_fact_query = """
    INSERT INTO Financial_Transactions_Fact (
        transaction_date, branch_id, account_id, currency_type, transaction_amount,transaction_fee, customer_id, transaction_id 
    )
    SELECT
        t."Transaction_Date"::DATE,
        t."Branch_ID",
        t."Account_ID",
        t."Currency_Type",
        t."Transaction_Amount",
        t."Transaction_Fee",
        t."Customer_ID",
        t."Transaction_ID"
    FROM financial_transactions_staging t
    INNER JOIN Branch_Dim b
    ON t."Branch_ID" = b."branch_id"
    INNER JOIN Account_Dim a
    ON t."Account_ID" = a."account_id"
    INNER JOIN currency_dim c
    on t."Currency_Type" = c."currency_type";
"""

try:
    # Execute the insert query
    cur.execute(insert_fact_query)

    conn.commit()
    print("Data successfully inserted into Financial_Transactions_Fact.")
except psycopg2.errors.ForeignKeyViolation as e:
    print(f"Foreign key violation error: {e}")

finally:
    # Close the cursor and connection
    cur.close()
    conn.close()
