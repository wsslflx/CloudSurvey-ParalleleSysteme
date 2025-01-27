from CloudSurvey_Package.math_operations import calculate_konfidenzintervall

def get_database(db_name, client):
    return client[db_name]

def fetch_all_hours_prices(db, collection_name, instance_type, region):
    collection = db[collection_name]
    query = {
        "instance_type": instance_type,
        "region": region
    }
    cursor = collection.find(query).sort("timestamp", 1)

    return list(cursor)

def fetch_instance_prices(db_name, collection_name,
                          instance_type, hour, region, client):
    db = client[db_name]
    collection = db[collection_name]

    query = {
        "instance_type": instance_type,
        "hour": hour,
        "region": region
    }

    cursor = collection.find(query).sort("timestamp", 1)

    results = list(cursor)

    return results

def get_all_instancePriceperHour(provider, instance, region, konfidenzgrad, client):
    if provider == "Azure":
        db = get_database("AzureSpotPricesDB", client)
        collection_name = "SpotPrices"
    elif provider == "AWS":
        db = get_database("aws_spot_prices_db", client)
        collection_name = "aws_spot_prices"

    docs = fetch_all_hours_prices(db, collection_name, instance, region)
    prices_by_hour = {h: [] for h in range(24)}  # dict of hour -> list of prices
    for doc in docs:
        hour = doc['hour']
        spot_price = doc['spot_price']
        prices_by_hour[hour].append(spot_price)

    costs = []
    for h in range(24):
        price_list = prices_by_hour[h]
        if price_list:
            konfidenz_prices = calculate_konfidenzintervall(price_list, konfidenzgrad)
        else:
            konfidenz_prices = [0, 0, 0]  # or handle empty data gracefully
        costs.append(konfidenz_prices)

    return costs

def fetch_storage_prices(provider, skuName, client):
    if provider == "Azure":
        db = get_database("azure_storage_pricing_db", client)
        collection_name = "StoragePrices"
        query = {
            "skuName": {"$regex": f"^{skuName}$", "$options": "i"},  # Case-insensitive match
            "unitOfMeasure": "1/Month",
        }
        projection = {"_id": 0, "region": 1, "skuName": 1, "price": 1}
        try:
            # Fetch results with query and projection
            cursor = db[collection_name].find(query, projection).sort("timestamp", 1)

            # Convert to a list of dictionaries and extract required fields
            results = list(cursor)
            output = [{"region": doc["region"], "skuName": doc["skuName"], "price": doc["price"]} for doc in results]

            if not results:
                print("No documents found.")
            return output
        except Exception as e:
            print(f"Error querying the database: {e}")
            return []

    if provider == "AWS":
        db = get_database("aws_storage_pricing_db", client)
        collection_name = "aws_ebs_prices"
        query = {
            "description": {"$regex": skuName, "$options": "i"},  # Case-insensitive match -> Sku Name here gp3 or gp2
        }
        projection = {"_id": 0, "region": 1, "description" : 1, "price": 1}
    try:
        # Fetch results with query and projection
        cursor = db[collection_name].find(query, projection).sort("timestamp", 1)
        # Convert to a list of dictionaries and extract required fields
        results = list(cursor)
        output = [{"region": doc["region"], "description": doc["description"], "price": doc["price"]} for doc in results]

        if not results:
            print("No documents found.")
        return output
    except Exception as e:
        print(f"Error querying the database: {e}")
        return []

    return []

def fetch_transfer_prices(provider, fromRegion, toRegion, client):
    if provider == "AWS":
        db = get_database("aws_storage_pricing_db", client)
        collection_name = "aws_ebs_prices"
        query = {
            "fromRegion": fromRegion,
            "toRegion": toRegion
        }
        projection = {"_id": 0, "price": 1}

        try:
            # Fetch results with query and projection
            cursor = db[collection_name].find(query, projection).sort("timestamp", 1)

            # Convert to a list of dictionaries and extract required fields
            results = list(cursor)
            output = [{"price": doc["price"]} for doc in results]

            if not results:
                print("No documents found.")
            return output
        except Exception as e:
            print(f"Error querying the database: {e}")
            return []
"""

from pymongo import MongoClient
import os
from dotenv import load_dotenv 

load_dotenv()
connection_string = os.getenv('MONGODB_URI')
connection_string2 = os.getenv('MONGODB_URI2')

client = MongoClient(connection_string2)
print(fetch_storage_prices("AWS", "gp3", client))
"""