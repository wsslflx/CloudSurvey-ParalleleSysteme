from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

connection_string = os.getenv('MONGODB_URI')
def fetch_instance_prices(db_name, collection_name,
                          instance_type, hour, region):
    client = MongoClient(connection_string)
    db = client[db_name]
    collection = db[collection_name]

    query = {
        "instance_type": instance_type,
        "hour": hour,
        "region": region
    }

    cursor = collection.find(query).sort("timestamp", 1)

    results = list(cursor)
    client.close()

    return results

def cost_one_job(duration, instancePrice):
    return duration * instancePrice

#fetch Instance Price from Database for specific time and return median
def get_instancePrice(provider, instance, hour, region):
    list = []
    if provider == "Azure":
        list = fetch_instance_prices("AzureSpotPricesDB", "SpotPrices", instance, hour, region)
    if provider == "AWS":
        list = fetch_instance_prices("aws_spot_prices_db", "aws_spot_prices", instance, hour, region)
    prices = [doc['spot_price'] for doc in list]
    prices.sort()

    return prices[int(len(prices) / 2)]

# return minimal price and start hour for given duration per Instance
def cost_startHour(instance, duration):
    hour = 0
    cost = 0
    return cost, hour

return_list = (fetch_instance_prices("AzureSpotPricesDB", "SpotPrices", "E2s v5 Spot", 16, "northeurope"))
spot_prices = [doc['spot_price'] for doc in return_list]
print(spot_prices)

# import [[instance, time]]
def main(list):


