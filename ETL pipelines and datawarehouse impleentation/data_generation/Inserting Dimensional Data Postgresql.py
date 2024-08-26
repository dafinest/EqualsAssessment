import psycopg2
from psycopg2 import sql
from faker import Faker
import random
from datetime import datetime, timedelta

# Database connection configuration
db_config = {
    'dbname': 'EqualsDataWarehouse',
    'user': 'postgres',     #Put your username
    'password': 'password', #Put your password
    'host': 'localhost',  # Change if your DB is on another host
    'port': '5432'        # Default PostgreSQL port      # Default PostgreSQL port
}

# Initialize Faker
fake = Faker()

# Function to generate fake customer data
def generate_customer_data(n_customers):
    customers = []
    for _ in range(n_customers):
        customers.append((
            fake.name(),
            random.choice(['Male', 'Female']),
            fake.date_of_birth(minimum_age=18, maximum_age=80),
            fake.address(),
            fake.phone_number(),
            fake.email(),
            fake.job()
        ))
    return customers

# Function to generate fake account data
def generate_account_data(n_accounts):
    accounts = []
    for _ in range(n_accounts):
        open_date = fake.date_between(start_date='-10y', end_date='-1y')
        close_date = fake.date_between(start_date=open_date, end_date='today') if random.random() < 0.1 else None
        accounts.append((
            random.choice(['Savings', 'Checking', 'Credit']),
            random.choice(['Active', 'Dormant', 'Closed']),
            open_date,
            close_date
        ))
    return accounts

# Function to generate fake financial transaction data
def generate_transaction_data(n_transactions, customer_ids, account_ids, branch_ids):
    transactions = []
    for _ in range(n_transactions):
        transaction_date = fake.date_time_between(start_date='-5y', end_date='now')
        transactions.append((
            random.choice(account_ids),
            random.choice(customer_ids),
            transaction_date.date(),
            random.choice(branch_ids),
            round(random.uniform(10.0, 5000.0), 2),
            random.choice(['Deposit', 'Withdrawal', 'Transfer', 'Payment']),
            round(random.uniform(0.0, 10.0), 2),
            random.choice(['USD', 'EUR', 'GBP', 'JPY'])
        ))
    return transactions

# Function to insert generated data into PostgreSQL
def insert_data():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Generate and insert data for Customer_Dim
        customers = generate_customer_data(100)
        cursor.executemany("""
            INSERT INTO Customer_Dim (Customer_Name, Gender, Date_Of_Birth, Address, Phone_Number, Email, Occupation)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, customers)
        
        # Generate and insert data for Account_Dim
        accounts = generate_account_data(100)
        cursor.executemany("""
            INSERT INTO Account_Dim (Account_Type, Account_Status, Open_Date, Close_Date)
            VALUES (%s, %s, %s, %s)
        """, accounts)
        
        # Generate and insert data for Branch_Dim
        branches = [(fake.company(), fake.city(), fake.name()) for _ in range(10)]
        cursor.executemany("""
            INSERT INTO Branch_Dim (Branch_Name, Branch_Location, Manager_Name)
            VALUES (%s, %s, %s)
        """, branches)
        
        # Generate and insert data for Currency_Dim
        currencies = [
            ('USD', '$', 1.00),
            ('EUR', '€', 0.85),
            ('GBP', '£', 0.75),
            ('JPY', '¥', 110.0)
        ]
        cursor.executemany("""
            INSERT INTO Currency_Dim (Currency_Type, Currency_Symbol, Exchange_Rate_To_Base)
            VALUES (%s, %s, %s)
        """, currencies)
        
        # Generate and insert data for Transaction_Type_Dim
        transaction_types = [
            ('Deposit', 'Money deposit to an account'),
            ('Withdrawal', 'Money withdrawal from an account'),
            ('Transfer', 'Money transfer between accounts'),
            ('Payment', 'Payment of bills or services')
        ]
        cursor.executemany("""
            INSERT INTO Transaction_Type_Dim (Transaction_Type, Transaction_Description)
            VALUES (%s, %s)
        """, transaction_types)
        
        # Fetch generated customer, account, and branch IDs
        cursor.execute("SELECT Customer_ID FROM Customer_Dim")
        customer_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT Account_ID FROM Account_Dim")
        account_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT Branch_ID FROM Branch_Dim")
        branch_ids = [row[0] for row in cursor.fetchall()]

        # Generate and insert data for Financial_Transactions_Fact
        transactions = generate_transaction_data(200, customer_ids, account_ids, branch_ids)
        cursor.executemany("""
            INSERT INTO Financial_Transactions_Fact (Account_ID, Customer_ID, Transaction_Date, Branch_ID, Transaction_Amount, Transaction_Type, Transaction_Fee, Currency_Type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, transactions)
        
        # Commit the transactions
        conn.commit()
        print("Data inserted successfully!")
        
    except Exception as error:
        print(f"Error inserting data: {error}")
        
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Main execution
if __name__ == '__main__':
    insert_data()
