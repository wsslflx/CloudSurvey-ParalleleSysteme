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

def cost_one_job(priceList, hourCombination, duration):
    min_cost = 0
    mean_cost = 0
    max_cost = 0
    if int(duration) == duration:
        for hour in hourCombination:
            if not priceList:
                return 0, hourCombination[0]

            min_cost += priceList[hour][0]
            mean_cost += priceList[hour][1]
            max_cost += priceList[hour][2]
        return min_cost, mean_cost, max_cost, hourCombination[0]
    else:
        started_hour = duration - int(duration)
        startTime = hourCombination[0] - started_hour
        min_cost_atBeginning = 0
        mean_cost_atBeginning = 0
        max_cost_atBeginning = 0
        min_cost_atEnd = 0
        mean_cost_atEnd = 0
        max_cost_atEnd = 0

        for hour in hourCombination:
            if not priceList[hour][1]: # no slotprice available
                return 0, 0, 0, hourCombination[0]

            if hourCombination.index(hour) == 0:
                min_cost_atBeginning += priceList[hour][0] * started_hour
                mean_cost_atBeginning += priceList[hour][1] * started_hour
                max_cost_atBeginning += priceList[hour][2] * started_hour
                min_cost_atEnd += priceList[hour][0]
                mean_cost_atEnd += priceList[hour][1]
                max_cost_atEnd += priceList[hour][2]
            elif hourCombination.index(hour) == len(hourCombination) - 1:
                min_cost_atBeginning += priceList[hour][0]
                mean_cost_atBeginning += priceList[hour][1]
                max_cost_atBeginning += priceList[hour][2]
                min_cost_atEnd += priceList[hour][0] * started_hour
                mean_cost_atEnd += priceList[hour][1] * started_hour
                max_cost_atEnd += priceList[hour][2] * started_hour
            else:
                min_cost_atBeginning += priceList[hour][0]
                mean_cost_atBeginning += priceList[hour][1]
                max_cost_atBeginning += priceList[hour][2]
                min_cost_atEnd += priceList[hour][0]
                mean_cost_atEnd += priceList[hour][1]
                max_cost_atEnd += priceList[hour][2]

        if mean_cost_atBeginning > mean_cost_atEnd:
            return min_cost_atEnd, mean_cost_atEnd, max_cost_atEnd, startTime
        else:
            return min_cost_atBeginning, mean_cost_atBeginning, max_cost_atBeginning, hourCombination[0]


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
def get_all_instancePriceperHour(provider, instance, region, konfidenzgrad):
    numbers = list(range(24))
    costs = []
    for number in numbers:
        price = get_instancePriceperHour(provider, instance, number, region)
        if price is not None:
            konfidenz_prices = calculate_konfidenzintervall(price, konfidenzgrad)
        costs.append(konfidenz_prices)

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

def min_cost_instance(provider, instance, duration, region, konfidenzgrad):
    costs_slot = []
    costsPerHour = get_all_instancePriceperHour(provider, instance, region, konfidenzgrad)
    hour_combinations = get_hour_combinations(duration)

    if costsPerHour == []:
        return "no slot available"

    for timeSlot in hour_combinations:
        cost_min, cost_mean, cost_max, startTime = cost_one_job(costsPerHour, timeSlot, duration)
        costs_slot.append([cost_min, cost_mean, cost_max, startTime, duration])
    costs_slot.sort(key=lambda x: x[2])

    first_positive = next((row for row in costs_slot if row[1] > 0), None) #if no spot available cost = 0 -> need Filter

    return first_positive


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
    if len(list) > 1:
        if list[1] == []:
            return [0, 0, 0]
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
    else:
        return [0, 0, 0]

    return [lower_bound, mean, upper_bound]

def second_to_hour(seconds):
    return seconds / 3600

# input list: [instance, duration in s]
def one_job_complete(list, provider, region, konfidenzgrad):
    costs_slot_time =[]
    for instance in list:
        duration = second_to_hour(instance[1])
        min_cost = min_cost_instance(provider, instance[0], duration, region, konfidenzgrad)
        costs_slot_time.append([min_cost, instance[0]])
    return costs_slot_time


# prices = get_instancePriceperHour("Azure", "FX48-12mds v2 Spot", 17, "germanynorth")
list_test = [["FX48-12mds v2 Spot", 4002],["E2s v5 Spot", 3500]]
# [[[2.550947769061363, 2.708583270833333, 2.9049772231550968, 3, 1.1116666666666666], 'FX48-12mds v2 Spot'],
# [[0.27033597423115385, 0.31099333333333334, 0.373436772041086, 1, 10.13888888888889], 'E2s v5 Spot']]

print(one_job_complete(list_test, "Azure", "germanynorth", 95))