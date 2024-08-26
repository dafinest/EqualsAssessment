import pymongo
from pymongo import MongoClient
from faker import Faker
import random
from datetime import datetime, timedelta

# Step 1: Establish a connection to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Replace with your MongoDB URI if needed

# Step 2: Connect to the EqualsData database and TransactionalData collection
db = client['EqualsData']
collection = db['TransactionalData']

# Step 3: Initialize Faker to generate random data
faker = Faker()

# Step 4: Function to generate random transactional data
def generate_random_transaction():
    return {
        'Transaction_ID': random.randint(1, 1000000),  # Random unique transaction ID
        'Account_ID': random.randint(1000, 9999),  # Random Account ID
        'Customer_ID': random.randint(1, 1000),  # Random Customer ID
        'Transaction_Date': faker.date_time_between(start_date="-1y", end_date="now"),  # Random date within the last year
        'Branch_ID': random.randint(1, 100),  # Random Branch ID
        'Transaction_Amount': str(faker.pydecimal(left_digits=5, right_digits=2, positive=True)),  # Random transaction amount as string
        'Transaction_Type': random.choice(['Deposit', 'Withdrawal', 'Transfer', 'Payment']),  # Random transaction type
        'Transaction_Fee': str(faker.pydecimal(left_digits=2, right_digits=2, positive=True)),  # Random transaction fee as string
        'Currency_Type': random.choice(['USD', 'EUR', 'GBP', 'JPY'])  # Random currency type
    }

# Step 5: Insert randomly generated data into the collection
def insert_random_transactions(n):
    transactions = [generate_random_transaction() for _ in range(n)]
    collection.insert_many(transactions)
    print(f"{n} random transactions inserted into '{collection.name}' collection.")

# Step 6: Generate and insert 100 random transactions
insert_random_transactions(1000)
