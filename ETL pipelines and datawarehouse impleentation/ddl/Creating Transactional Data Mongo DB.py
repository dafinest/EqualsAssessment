import pymongo
from pymongo import MongoClient

# Establish a connection to MongoDB
client = MongoClient('mongodb://localhost:27017/')  

# connect to the existing database
db = client['EqualsData']  

# Define the collection name
collection_name = 'TransactionalData'

# Check if the collection exists
if collection_name not in db.list_collection_names():
    # Create the collection with schema validation only if it doesn't exist
    db.create_collection(collection_name, validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': [
                'Account_ID', 
                'Customer_ID', 
                'Transaction_Date', 
                'Branch_ID', 
                'Transaction_Amount', 
                'Transaction_Type', 
                'Currency_Type'
            ],
            'properties': {
                'Transaction_ID': {
                    'bsonType': 'int',
                    'description': 'must be an integer and is the primary key'
                },
                'Account_ID': {
                    'bsonType': 'int',
                    'description': 'must be an integer and references the Account_Dim'
                },
                'Customer_ID': {
                    'bsonType': 'int',
                    'description': 'must be an integer and references the Customer_Dim'
                },
                'Transaction_Date': {
                    'bsonType': 'date',
                    'description': 'must be a date and is required'
                },
                'Branch_ID': {
                    'bsonType': 'int',
                    'description': 'must be an integer and references the Branch_Dim'
                },
                'Transaction_Amount': {
                    'bsonType': 'string',
                    'description': 'must be a decimal value and is required'
                },
                'Transaction_Type': {
                    'bsonType': 'string',
                    'description': 'must be a string and is required'
                },
                'Transaction_Fee': {
                    'bsonType': ['string', 'null'],
                    'description': 'must be a decimal value or null'
                },
                'Currency_Type': {
                    'bsonType': 'string',
                    'description': 'must be a string and references the Currency_Dim'
                }
            }
        }
    })
    print(f"Collection '{collection_name}' created with schema validation!")
else:
    print(f"Collection '{collection_name}' already exists!")

