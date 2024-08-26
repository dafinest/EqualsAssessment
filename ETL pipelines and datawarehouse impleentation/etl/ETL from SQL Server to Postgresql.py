import pyodbc
import psycopg2
import pandas as pd
import numpy as np

# SQL Server connection details replace server, database, password and username with your own
sql_server_conn_str = (
    'DRIVER={SQL Server};SERVER=server;DATABASE=database;UID=username;PWD=password'
)

# PostgreSQL connection details
postgres_conn_str = {
    'dbname': 'EqualsDataWarehouse',
    'user': 'postgres',
    'password': 'Lyoid1995',
    'host': 'localhost',  # Change if your DB is on another host
    'port': '5432'  
}

# Connect to SQL Server and fetch data
sql_server_conn = pyodbc.connect(sql_server_conn_str)
sql_server_query = 'SELECT * FROM VisaTransactions'
df = pd.read_sql(sql_server_query, sql_server_conn)

# Close the SQL Server connection
sql_server_conn.close()

# Data Transformation
def transform_data(df):
    # Handle null values
    df = df.fillna({
        'TransactionID': 'UNKNOWN',
        'TransactionAmount': '0.00',
        'TransactionDate': '1900-01-01',
        'TransactionType': '0',
        'AccountNumber': 'UNKNOWN'
    })

    # Convert TransactionType to float (assuming a mapping; otherwise use a default)
    # Example: mapping ['withdrawal': 1, 'purchase': 2, 'deposit': 3]
    type_mapping = {'withdrawal': 1.0, 'purchase': 2.0, 'deposit': 3.0}
    df['TransactionType'] = df['TransactionType'].map(type_mapping).fillna(0.0).astype(float)

    # Convert TransactionDate from VARCHAR to DATE
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'], format='%Y-%m-%d %H:%M:%S', errors='coerce').dt.date

    return df

# Transform the data
df_transformed = transform_data(df)

# Connect to PostgreSQL
pg_conn = psycopg2.connect(**postgres_conn_str)
pg_cursor = pg_conn.cursor()

# Create staging table if it does not exist
create_staging_table_sql = """
CREATE TABLE IF NOT EXISTS visa_transactions_staging (
    TransactionID VARCHAR(255),
    TransactionAmount VARCHAR(255),
    TransactionDate DATE,
    TransactionType FLOAT,
    AccountNumber VARCHAR(255)
);
"""

pg_cursor.execute(create_staging_table_sql)
pg_conn.commit()

# Insert data into the PostgreSQL staging table
insert_sql = """
INSERT INTO visa_transactions_staging (TransactionID, TransactionAmount, TransactionDate, TransactionType, AccountNumber)
VALUES (%s, %s, %s, %s, %s);
"""

# Convert DataFrame to list of tuples for insertion
data_tuples = [tuple(x) for x in df_transformed.to_records(index=False)]

pg_cursor.executemany(insert_sql, data_tuples)
pg_conn.commit()

# Close PostgreSQL connection
pg_cursor.close()
pg_conn.close()

print("Data successfully loaded into PostgreSQL staging table.")
