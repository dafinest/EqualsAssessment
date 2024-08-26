import pyodbc

# Define your SQL Server connection details
server = '192.168.1.126'    #put your server name
database = 'Data_warehouse' 
username = 'username'     #put your username
password = 'password$i'     #put your password

# Create a connection string
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Connect to the SQL Server
with pyodbc.connect(conn_str) as conn:
    cursor = conn.cursor()
    
    # Define the SQL command to create the table
    create_table_sql = """
    CREATE TABLE VisaTransactionsDispose (
        TransactionID VARCHAR(255) PRIMARY KEY,
        TransactionAmount VARCHAR(255),
        TransactionDate VARCHAR(255),
        TransactionType VARCHAR(255),
        AccountNumber VARCHAR(255)
    );
    """
    
    # Execute the SQL command
    cursor.execute(create_table_sql)
    conn.commit()  # Commit the transaction

    print("Table 'VisaTransactions' created successfully.")
