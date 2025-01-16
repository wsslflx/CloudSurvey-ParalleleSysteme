from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv
import requests
import numpy as np
from scipy.stats import t


load_dotenv()
connection_string = os.getenv('MONGODB_URI')
client = MongoClient(connection_string)

def get_database(db_name):
    return client[db_name]

def fetch_instance_prices(db_name, collection_name,
                          instance_type, hour, region):
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
        print(instance, hour, region)
        list = fetch_instance_prices("AzureSpotPricesDB", "SpotPrices", instance, hour, region)
    if provider == "AWS":
        list = fetch_instance_prices("aws_spot_prices_db", "aws_spot_prices", instance, hour, region)
    prices = [doc['spot_price'] for doc in list]

    return prices

# get prices for all hours of a instance, provider, region and return it in list
def get_all_instancePriceperHour(provider, instance, region, konfidenzgrad):
    """
    numbers = list(range(24))
    costs = []
    for number in numbers:
        price = get_instancePriceperHour(provider, instance, number, region)
        if price is not None:
            konfidenz_prices = calculate_konfidenzintervall(price, konfidenzgrad)
        costs.append(konfidenz_prices)

    return costs
    """
    if provider == "Azure":
        db = get_database("AzureSpotPricesDB")
        collection_name = "SpotPrices"
    elif provider == "AWS":
        db = get_database("aws_spot_prices_db")
        collection_name = "aws_spot_prices"

    docs = fetch_all_hours_prices(db, collection_name, instance, region)

    # Bucket the prices by hour
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

def fetch_all_hours_prices(db, collection_name, instance_type, region):
    collection = db[collection_name]
    query = {
        "instance_type": instance_type,
        "region": region
    }
    cursor = collection.find(query).sort("timestamp", 1)

    return list(cursor)

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
        costs_slot.append([cost_min, cost_mean, cost_max, startTime, duration, region])
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
def one_job_complete(list, provider, regions, konfidenzgrad):
    costs_slot_time =[]
    first_positive = []
    for instance in list:
        duration = second_to_hour(instance[1])
        for region in regions:
            min_cost = min_cost_instance(provider, instance[0], duration, region, konfidenzgrad)
            costs_slot_time.append([min_cost, instance[0]])

        sorted_costs_slot_time = sorted(costs_slot_time, key=lambda x: x[0][1] if x[0] else float('inf'))
        first_positive.append(next((item for item in sorted_costs_slot_time if item[0] and item[0][1] > 0), None))

    return first_positive

def azure_instance_name(input_string):
    if input_string.startswith("Standard"):
        input_string = input_string[len("Standard"):]
        input_string = input_string.replace("_", " ")
        input_string += " spot"
    return input_string

def generate_Output(print_result, konfidenzgrad):
    print("Expected Price: " + str(print_result[0][0][1]) + "€")
    print("Price Range: " + str(print_result[0][0][0]) + "€" + " - " + str(print_result[0][0][2]) + "€" + "in " + str(konfidenzgrad) + "% of cases")
    print("Expected Time needed: " + str(print_result[0][0][4]) + " hours")
    print("Best Starting Time: " + str(print_result[0][0][3]))
    print("Best Instance: " + str(print_result[0][1]) + "in Region: " + str(print_result[0][0][5]))

def dimensions_test(list):
    np_list = np.array(list)
    return np_list.ndim
def multiple_jobs(provider, jobs, regions, konfidenzgrad):
    results_multiple_jobs = []
    if dimensions_test(jobs) == 3:
        min_cost = 0
        mean_cost = 0
        max_cost = 0
        duration = 0
        for job in jobs:
            result_single = []
            if provider == "Azure":
                for element in job:
                    element[0] = azure_instance_name(element[0])
                result_single = (one_job_complete(job, provider, regions, konfidenzgrad))
            elif provider == "AWS":
                result_single = (one_job_complete(list, provider, aws_regions, konfidenzgrad))

            results_single_sorted = sorted(result_single, key=lambda x: x[0][1])
            results_multiple_jobs.append(results_single_sorted[0])

        for element in results_multiple_jobs:
            min_cost += element[0][0]
            mean_cost += element[0][1]
            max_cost += element[0][2]
            duration += element[0][4]
        total_cost = (min_cost, mean_cost, max_cost, duration)
        return [total_cost, results_multiple_jobs]

    elif dimensions_test(jobs) == 2: # just a single job
        result_single = []
        if provider == "Azure":
            for element in jobs:
                element[0] = azure_instance_name(element[0])
            result_single = (one_job_complete(jobs, provider, regions, konfidenzgrad))
        elif provider == "AWS":
            result_single = (one_job_complete(list, provider, aws_regions, konfidenzgrad))

        results_single_sorted = sorted(result_single, key=lambda x: x[0][1])
        results_multiple_jobs = (results_single_sorted[0])
    return results_multiple_jobs


def get_transfer_cost(region, provider, volume):
    if provider == "Azure":
        return volume * 0.0192 #research shows that for inter eu data transfer it always costs 0.0192 per GB



list_test = [["FX48-12mds v2 Spot", 4002]],[["E2s v5 Spot", 3500]]

# AWS Regions:
aws_regions = [
    'eu-central-1', 'eu-west-1', 'eu-west-2',
    'eu-west-3', 'eu-north-1',
]

# Azure Regions:
azure_regions = [
    'westeurope', 'germanywestcentral', 'northeurope',
    'swedencentral', 'uksouth', 'francecentral',
    'italynorth', 'norwayeast', 'polandcentral', 'spaincentral',
    'switzerlandnorth', 'europe', 'francesouth',
    'norwaywest', 'switzerlandwest', 'ukwest',
    'germanynorth'
]
def main():
    provider = "Azure"
    list = list_test
    konfidenzgrad = 95
    results_job = (multiple_jobs(provider, list, azure_regions, konfidenzgrad))
    print(results_job)

main()



client.close()