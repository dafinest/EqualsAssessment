from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['EqualsData']
collection = db['CustomerData']

# Extract data from MongoDB
data = list(collection.find({}))

# Convert to DataFrame for easier manipulation
df = pd.json_normalize(data)

print(df.columns)
