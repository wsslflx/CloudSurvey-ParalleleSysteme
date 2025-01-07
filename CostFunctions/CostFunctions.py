from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv
import requests
import numpy as np
from scipy.stats import t


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

    return prices

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
        m = 0
        combination = []
        while m < duration:
            combination.append(numbers[(i + m) % len(numbers)])
            m += 1
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
        costs_slot_time.append([min_cost_instance(provider, instance[0], instance[1], region), instance[1]]) # list[1] should be the duration
    return costs_slot_time


def request_into_list():
    response = requests.post(
        "http://localhost:8080/simulate/aws?cloudletLength=10000",
        json={"key": "value"} # Request body
    )
    """
    {
        "instance_name": "t2.micro",
        "execution_time": 123.45
    }
    """
    for item in response.json():
        list.append([item[0], item[1]])

    return list


def calculate_konfidenzintervall(list, konfidenzgrad):
    mean = np.mean(list)  # Mittelwert
    std_dev = np.std(list, ddof=1)  # Standardabweichung (ddof=1 für Stichprobe)
    n = len(list)  # Stichprobengröße
    standard_error = std_dev / np.sqrt(n)  # Standardfehler

    list.sort()
    # Kritischer t-Wert für 95% Konfidenzintervall und df = n-1
    alpha = 1 - konfidenzgrad / 100
    t_value = t.ppf(1 - alpha, df=n - 1)

    # Konfidenzintervall
    lower_bound = max(mean - t_value * standard_error, list[0])
    upper_bound = min(mean + t_value * standard_error, list[len(list)- 1])

    return [lower_bound, mean, upper_bound]

def second_to_hour(seconds):
    return seconds / 3600

# prices = get_instancePriceperHour("Azure", "FX48-12mds v2 Spot", 17, "germanynorth")