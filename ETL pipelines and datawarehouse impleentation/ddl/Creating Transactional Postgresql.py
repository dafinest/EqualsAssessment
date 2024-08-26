import psycopg2
from psycopg2 import sql

# Define the connection parameters to PostgreSQL
db_params = {
    'dbname': 'EqualsDataWarehouse',
    'user': 'postgres',     #Put your username
    'password': 'password', #Put your password
    'host': 'localhost',  # Change if your DB is on another host
    'port': '5432'        # Default PostgreSQL port
}

# Create the connection and cursor
try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    print("Connected to PostgreSQL database!")

    # Create a table for staging financial transaction data
    create_table_query = sql.SQL('''
        CREATE TABLE IF NOT EXISTS financial_transactions_staging (
            transaction_id SERIAL PRIMARY KEY,
            account_id INT NOT NULL,
            customer_id INT NOT NULL,
            transaction_date TIMESTAMP NOT NULL,
            branch_id INT NOT NULL,
            transaction_amount FLOAT NOT NULL,
            transaction_type VARCHAR(50) NOT NULL,
            transaction_fee FLOAT DEFAULT 0.0,
            currency_type VARCHAR(3) NOT NULL
        );
    ''')

    # Execute the query to create the table
    cursor.execute(create_table_query)
    conn.commit()
    print("Table 'financial_transactions_staging' created successfully!")

except Exception as error:
    print(f"Error while connecting to PostgreSQL: {error}")
finally:
    # Close the connection
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection closed.")



