import psycopg2
from datetime import datetime, timedelta

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

# Function to determine the quarter of the year
def get_quarter(month):
    return (month - 1) // 3 + 1

# Function to get the day of the week
def get_day_of_week(date):
    return date.strftime('%A')

# Generate date range
start_date = datetime(2020, 1, 1)  # Replace with your desired start date
end_date = datetime(2024, 12, 31)  # Replace with your desired end date

# Prepare the insert query
insert_query = """
INSERT INTO Time_Dim (Transaction_Date, Year, Month, Quarter, Day_Of_Week)
VALUES (%s, %s, %s, %s, %s);
"""

# Generate data and execute the insert query
current_date = start_date
while current_date <= end_date:
    year = current_date.year
    month = current_date.month
    quarter = get_quarter(month)
    day_of_week = get_day_of_week(current_date)
    
    data = (current_date.date(), year, month, quarter, day_of_week)
    cur.execute(insert_query, data)
    

    current_date += timedelta(days=1)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Date dimension data inserted successfully.")
