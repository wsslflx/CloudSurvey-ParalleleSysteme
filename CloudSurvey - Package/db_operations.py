from pymongo import MongoClient
import os
from dotenv import load_dotenv
from .math_operations import calculate_konfidenzintervall

load_dotenv()
connection_string = os.getenv('MONGODB_URI')
connection_string2 = os.getenv('MONGODB_URI2')

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

def fetch_storage_prices(provider, type, client):
    if provider == "Azure":
        db = get_database("azure_storage_pricing_db", client)
        collection_name = "storage_pricing"