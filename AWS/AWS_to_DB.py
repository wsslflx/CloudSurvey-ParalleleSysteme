import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# Load CSV file into a DataFrame
csv_file = "all_instance_types.csv"
data = pd.read_csv(csv_file)

CONNECTION_STRING = os.getenv('MONGODB_URI')
print("Using CONNECTION_STRING:", CONNECTION_STRING)

# Connect to MongoDB Atlas
client = MongoClient(CONNECTION_STRING)
db = client["AWSInstancesDB"]
collection = db["AWSInstancesCollection"]

# Convert DataFrame to dictionary
data_dict = data.to_dict("records")

# Insert into MongoDB
collection.insert_many(data_dict)

print("Data imported successfully!")
