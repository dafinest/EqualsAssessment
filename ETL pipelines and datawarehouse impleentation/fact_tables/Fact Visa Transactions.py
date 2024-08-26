import psycopg2

# Database connection parameters
db_params = {
     'dbname': 'EqualsDataWarehouse',
    'user': 'postgres',     #Put your username
    'password': 'password', #Put your password
    'host': 'localhost',  # Change if your DB is on another host
    'port': '5432'        # Default PostgreSQL port
}

create_table_query = """
CREATE TABLE IF NOT EXISTS visa_transactions_fact (
    TransactionID VARCHAR(255) PRIMARY KEY,
    TransactionAmount FLOAT,
    transaction_date Date,
    TransactionType INT,
    AccountNumber VARCHAR(255),
    FOREIGN KEY (transaction_date) REFERENCES time_dim(transaction_date)
);
"""

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Query to insert data into the fact table
insert_query = """
    INSERT INTO visa_transactions_fact (TransactionID, TransactionAmount, transaction_date, TransactionType, AccountNumber)
    SELECT 
        s.TransactionID,
        CAST(s.TransactionAmount AS FLOAT),
        d.transaction_date,
        CAST(s.TransactionType AS INT),
        s.AccountNumber
    FROM 
        visa_transactions_staging s
    JOIN 
        time_dim d ON s.TransactionDate = d.transaction_date;
"""

try:
    # Execute the insert query
    cur.execute(insert_query)
    
    # Commit the changes
    conn.commit()
    print("Data inserted successfully!")

except Exception as e:
    # Rollback in case of error
    conn.rollback()
    print(f"An error occurred: {e}")

finally:
    # Close the cursor and connection
    cur.close()
    conn.close()