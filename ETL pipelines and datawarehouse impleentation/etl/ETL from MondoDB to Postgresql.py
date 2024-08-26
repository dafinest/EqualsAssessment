from pymongo import MongoClient
import pandas as pd
from decimal import Decimal
from sqlalchemy import create_engine

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['EqualsData']
collection = db['TransactionalData']

# Extract data from MongoDB
data = list(collection.find())

# Convert to pandas DataFrame
df = pd.DataFrame(data)

# Convert MongoDB ObjectId to string
if '_id' in df.columns:
    df['_id'] = df['_id'].astype(str)

# Data transformations with null handling and data type conversion
def convert_to_float(value):
    try:
        if pd.notnull(value):
   
            if isinstance(value, str):
                return float(value.replace(',', '')) 
            elif isinstance(value, Decimal):
                return float(value.to_decimal())
            else:
                return float(value)
    except (ValueError, TypeError):
        return None
    return None

# Convert Transaction_Amount and Transaction_Fee to float
df['Transaction_Amount'] = df['Transaction_Amount'].apply(convert_to_float)
df['Transaction_Fee'] = df['Transaction_Fee'].apply(convert_to_float)

# Convert Transaction_Date to datetime, handling nulls and invalid formats
df['Transaction_Date'] = pd.to_datetime(df['Transaction_Date'], errors='coerce')

# Ensure Account_ID, Customer_ID, Branch_ID are integers and handle nulls or invalid values
def convert_to_int(value):
    try:
        if pd.notnull(value):
            return int(value)
    except (ValueError, TypeError):
        return None
    return None

df['Account_ID'] = df['Account_ID'].apply(convert_to_int)
df['Customer_ID'] = df['Customer_ID'].apply(convert_to_int)
df['Branch_ID'] = df['Branch_ID'].apply(convert_to_int)

# Ensure Transaction_Type and Currency_Type are strings and handle nulls or invalid values
df['Transaction_Type'] = df['Transaction_Type'].apply(lambda x: str(x) if pd.notnull(x) else None)
df['Currency_Type'] = df['Currency_Type'].apply(lambda x: str(x) if pd.notnull(x) else None)

# Handle null values - Replace nulls with default values
df.fillna({
    'Transaction_Amount': 0.0,
    'Transaction_Fee': 0.0,
    'Transaction_Type': 'Unknown',   # Replace missing transaction types with 'Unknown'
    'Currency_Type': 'Unknown',      # Replace missing currency types with 'Unknown'
    'Account_ID': -1,                # Use -1 or a default placeholder for missing IDs
    'Customer_ID': -1,
    'Branch_ID': -1,
    'Transaction_Date': pd.Timestamp('1970-01-01')  # Use a default date for missing/invalid dates
}, inplace=True)

# drop rows that still have nulls 
df.dropna(inplace=True)

# Create a connection to PostgreSQL, replace postgres with username and Lyoid1993 with password
engine = create_engine('postgresql://postgres:Lyoid1993@localhost:5432/EqualsDataWarehouse')

# Step 8: Load the data into a staging table
df.to_sql('financial_transactions_staging', engine, if_exists='replace', index=False)

# Check for invalid transaction amounts
invalid_amounts = df[df['Transaction_Amount'] < 0]
if not invalid_amounts.empty:
    print(f"Found invalid transaction amounts: {invalid_amounts}")
