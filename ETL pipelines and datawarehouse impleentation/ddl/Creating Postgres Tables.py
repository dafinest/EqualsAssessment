import psycopg2
from psycopg2 import sql

# Database connection configuration
db_config = {
    'dbname': 'EqualsDataWarehouse',
    'user': 'username',     #Put your username
    'password': 'password', #Put your password
    'host': 'localhost',  # Change if your DB is on another host
    'port': '5432'        # Default PostgreSQL port
}

# Function to create the star schema tables
def create_star_schema():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # List of SQL commands to create the star schema tables
        create_table_queries = [
            
            # Dimension Table: Account_Dim
            """
            CREATE TABLE IF NOT EXISTS Account_Dim (
                Account_ID SERIAL PRIMARY KEY,
                Account_Type VARCHAR(50) NOT NULL,
                Account_Status VARCHAR(20),
                Open_Date DATE NOT NULL,
                Close_Date DATE
            );
            """,
            # Dimension Table: Customer_Dim
            """
            CREATE TABLE IF NOT EXISTS Customer_Dim (
                Customer_ID SERIAL PRIMARY KEY,
                Customer_Name VARCHAR(100) NOT NULL,
                Gender VARCHAR(10),
                Date_Of_Birth DATE,
                Address TEXT,
                Phone_Number VARCHAR(20),
                Email VARCHAR(100),
                Occupation VARCHAR(50)
            );
            """,
            # Dimension Table: Time_Dim
            """
            CREATE TABLE IF NOT EXISTS Time_Dim (
                Transaction_Date DATE PRIMARY KEY,
                Year INT NOT NULL,
                Month INT NOT NULL,
                Quarter INT NOT NULL,
                Day_Of_Week VARCHAR(10) NOT NULL
            );
            """,
            # Dimension Table: Branch_Dim
            """
            CREATE TABLE IF NOT EXISTS Branch_Dim (
                Branch_ID SERIAL PRIMARY KEY,
                Branch_Name VARCHAR(100),
                Branch_Location VARCHAR(100),
                Manager_Name VARCHAR(100)
            );
            """,
            # Dimension Table: Currency_Dim
            """
            CREATE TABLE IF NOT EXISTS Currency_Dim (
                Currency_Type VARCHAR(3) PRIMARY KEY,
                Currency_Symbol VARCHAR(10),
                Exchange_Rate_To_Base DECIMAL(15, 6)
            );
            """,
            # Dimension Table: Transaction_Type_Dim
            """
            CREATE TABLE IF NOT EXISTS Transaction_Type_Dim (
                Transaction_Type VARCHAR(50) PRIMARY KEY,
                Transaction_Description VARCHAR(100)
            );
            """,
            # Fact Table: Financial_Transactions_Fact
            """
            CREATE TABLE IF NOT EXISTS Financial_Transactions_Fact (
                Transaction_ID SERIAL PRIMARY KEY,
                Account_ID INT NOT NULL,
                Customer_ID INT NOT NULL,
                Transaction_Date DATE NOT NULL,
                Branch_ID INT NOT NULL,
                Transaction_Amount DECIMAL(15, 2) NOT NULL,
                Transaction_Type VARCHAR(50) NOT NULL,
                Transaction_Fee DECIMAL(10, 2),
                Currency_Type VARCHAR(3) NOT NULL,
                FOREIGN KEY (Account_ID) REFERENCES Account_Dim(Account_ID),
                FOREIGN KEY (Customer_ID) REFERENCES Customer_Dim(Customer_ID),
                FOREIGN KEY (Branch_ID) REFERENCES Branch_Dim(Branch_ID),
                FOREIGN KEY (Currency_Type) REFERENCES Currency_Dim(Currency_Type)
            );
            """
        ]
        
        # Execute each SQL command
        for query in create_table_queries:
            cursor.execute(query)
        
        # Commit the transactions
        conn.commit()
        
        print("Star Schema tables created successfully!")
        
    except Exception as error:
        print(f"Error creating star schema: {error}")
        
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Main execution
if __name__ == '__main__':
    create_star_schema()
