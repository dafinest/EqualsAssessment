import psycopg2

# PostgreSQL connection details
conn_params = {
    'dbname': 'EqualsDataWarehouse',
    'user': 'username',   #put your username
    'password': 'password',    #put your password
    'host': 'localhost',  
    'port': '5432'  
}

# Connect to PostgreSQL
pg_conn = psycopg2.connect(**postgres_conn_str)
pg_cursor = pg_conn.cursor()

# Create the dimension table if it does not exist
create_dim_table_sql = """
CREATE TABLE IF NOT EXISTS transaction_type_dim (
    TransactionTypeID SERIAL PRIMARY KEY,
    TransactionTypeName VARCHAR(255) UNIQUE
);
"""
pg_cursor.execute(create_dim_table_sql)
pg_conn.commit()

# Close PostgreSQL connection
pg_cursor.close()
pg_conn.close()

print("Table 'transaction_type_dim' successfully created.")
