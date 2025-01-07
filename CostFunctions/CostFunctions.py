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

def cost_one_job(priceList, hourCombination):
    cost = 0
    for hour in hourCombination:
        cost += priceList[hour]

    return cost

#fetch Instance Price from Database for specific time and return median
def get_instancePriceperHour(provider, instance, hour, region):
    list = []
    if provider == "Azure":
        list = fetch_instance_prices("AzureSpotPricesDB", "SpotPrices", instance, hour, region)
    if provider == "AWS":
        list = fetch_instance_prices("aws_spot_prices_db", "aws_spot_prices", instance, hour, region)
    prices = [doc['spot_price'] for doc in list]
    prices.sort()

    return prices[int(len(prices) / 2)]

# get prices for all hours of a instance, provider, region and return it in list
def get_all_instancePriceperHour(provider, instance, region):
    numbers = list(range(24))
    costs = []
    for number in numbers:
        price = get_instancePriceperHour(provider, instance, number, region)
        costs.append(price)

    return costs

def get_hour_combinations(duration):
    numbers = list(range(24)) # 0 to 23
    combinations = []

    for i in range(len(numbers)):
        combination = [numbers[i], numbers[(i + 1) % len(numbers)], numbers[(i + 2) % len(numbers)]]
        combinations.append(combination)

    return combinations

def min_cost_instance(provider, instance, duration, region):
    costs_slot = []
    costsPerHour = get_all_instancePriceperHour(provider, instance, region)
    hour_combinations = get_hour_combinations(duration)

    for timeSlot in hour_combinations:
        costs_slot.append([cost_one_job(costsPerHour, timeSlot), timeSlot[0]])
    costs_slot.sort(key=lambda x: x[0])

    return costs_slot[0]


def one_job_complete(list, provider, region):
    costs_slot_time =[]
    for instance in list:
        costs_slot_time.append([min_cost_instance(provider, list[0], list[1], region), list[1]]) # list[1] should be the duration
    return costs_slot_time


# Connect to your MongoDB Atlas cluster
client = MongoClient(connection_string)

# Select your database and collection
db = client["aws_spot_prices_db"]
collection = db["aws_spot_prices"]

# Get all unique values for the field
unique_values = collection.distinct("region")

# Print the unique values
print(unique_values)


