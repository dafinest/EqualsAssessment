import pyodbc
import random
from faker import Faker

# Define your SQL Server connection details and remove the placeholders
server = 'servername'
database = 'Data_warehouse'
username = 'username'
password = 'password'

conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Create a Faker instance
fake = Faker()

# List of predefined transaction types
transaction_types = ['withdrawal', 'purchase', 'deposit']

# Generate 100 records
records = []
for _ in range(100):
    record = (
        fake.uuid4(),  # TransactionID
        f"{fake.random_number(digits=4)}.{fake.random_number(digits=2)}",  # TransactionAmount
        fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),  # TransactionDate
        random.choice(transaction_types),  # TransactionType (randomized)
        fake.iban()  # AccountNumber
    )
    records.append(record)

# Connect to the SQL Server
with pyodbc.connect(conn_str) as conn:
    cursor = conn.cursor()
    
    # Create the table if it does not exist
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='VisaTransactions' AND xtype='U')
    BEGIN
        CREATE TABLE VisaTransactions (
            TransactionID VARCHAR(255) PRIMARY KEY,
            TransactionAmount VARCHAR(255),
            TransactionDate VARCHAR(255),
            TransactionType VARCHAR(255),
            AccountNumber VARCHAR(255)
        );
    END
    """
    
    # Execute the SQL command to create the table
    cursor.execute(create_table_sql)
    conn.commit()
    
    # Insert data into the table
    insert_sql = """
    INSERT INTO VisaTransactions (TransactionID, TransactionAmount, TransactionDate, TransactionType, AccountNumber)
    VALUES (?, ?, ?, ?, ?);
    """
    
    # Execute the SQL command
    cursor.executemany(insert_sql, records)
    conn.commit()  # Commit the transaction

    print("100 records inserted into 'VisaTransactions'.")