import psycopg2

# Database connection parameters
conn_params = {
    'dbname': 'EqualsDataWarehouse',
    'user': 'username',   #put your username
    'password': 'password',    #put your password
    'host': 'localhost',  
    'port': '5432'  
}

# Connect to the PostgreSQL database
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

# SQL query to insert values
insert_query = """
INSERT INTO transaction_type_dim (transaction_type, transaction_description)
VALUES (%s, %s);
"""

# Data to insert
data = [
    (1, 'withdrawal'),
    (2, 'deposit'),
    (3, 'purchase')
]

# Execute the insert query for each row of data
cur.executemany(insert_query, data)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Data inserted successfully.")
