import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# Load CSV file into a DataFrame
csv_file = "vm_sizes2.csv"
data = pd.read_csv(csv_file)

CONNECTION_STRING = os.getenv('MONGODB_URI')
print("Using CONNECTION_STRING:", CONNECTION_STRING)

# Connect to MongoDB Atlas
client = MongoClient(CONNECTION_STRING)
db = client["AzureInstancesDB"]
collection = db["AzureInstancesCollection"]

# Convert DataFrame to dictionary
data_dict = data.to_dict("records")

# Set the '_id' field to the instance name for each record
for record in data_dict:
    # Assuming the column name in your CSV is exactly "Name der Größe"
    record['_id'] = record.get("Name der Größe")

# Insert into MongoDB
collection.insert_many(data_dict)

print("Data imported successfully!")
