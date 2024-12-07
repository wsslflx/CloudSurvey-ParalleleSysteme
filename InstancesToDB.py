import pandas as pd
from pymongo import MongoClient
import os

# Load CSV file into a DataFrame
csv_file = "azure_vm_sizes.csv"
data = pd.read_csv(csv_file)

CONNECTION_STRING = os.getenv('MONGODB_URI')

# Connect to MongoDB Atlas
client = MongoClient(CONNECTION_STRING)
db = client["AzureInstancesDB"]
collection = db["AzureInstancesCollection"]

# Convert DataFrame to dictionary and insert into MongoDB
data_dict = data.to_dict("records")
collection.insert_many(data_dict)

print("Data imported successfully!")