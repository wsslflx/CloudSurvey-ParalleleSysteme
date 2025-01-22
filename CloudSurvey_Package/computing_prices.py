from CloudSurvey_Package.help_methods import dimensions_test, azure_instance_name, get_hour_combinations
import CloudSurvey_Package.constants as constants
from CloudSurvey_Package.math_operations import second_to_hour
from CloudSurvey_Package.db_operations import fetch_instance_prices, get_all_instancePriceperHour

#fetch Instance Price from Database for specific time and return median
def get_instancePriceperHour(provider, instance, hour, region, client):
    list = []
    if provider == "Azure":
        list = fetch_instance_prices("AzureSpotPricesDB", "SpotPrices", instance, hour, region, client)
    if provider == "AWS":
        list = fetch_instance_prices("aws_spot_prices_db", "aws_spot_prices", instance, hour, region, client)
    prices = [doc['spot_price'] for doc in list]

    return prices

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

def min_cost_instance(provider, instance, duration, region, konfidenzgrad, client):
    costs_slot = []
    costsPerHour = get_all_instancePriceperHour(provider, instance, region, konfidenzgrad, client)
    hour_combinations = get_hour_combinations(duration)

    if costsPerHour == []:
        return "no slot available"

    for timeSlot in hour_combinations:
        cost_min, cost_mean, cost_max, startTime = cost_one_job(costsPerHour, timeSlot, duration)
        costs_slot.append([cost_min, cost_mean, cost_max, startTime, duration, region])
    costs_slot.sort(key=lambda x: x[2])

    first_positive = next((row for row in costs_slot if row[1] > 0), None) #if no spot available cost = 0 -> need Filter

    return first_positive


def one_job_complete(list, provider, regions, konfidenzgrad, client):
    costs_slot_time =[]
    first_positive = []
    for instance in list:
        duration = second_to_hour(instance[1])
        for region in regions:
            min_cost = min_cost_instance(provider, instance[0], duration, region, konfidenzgrad, client)
            costs_slot_time.append([min_cost, instance[0]])

        sorted_costs_slot_time = sorted(costs_slot_time, key=lambda x: x[0][1] if x[0] else float('inf'))
        first_positive.append(next((item for item in sorted_costs_slot_time if item[0] and item[0][1] > 0), None))

    return first_positive

def multiple_jobs(provider, jobs, konfidenzgrad, client):
    results_multiple_jobs = []
    aws_regions = constants.aws_regions
    azure_regions = constants.azure_regions
    if dimensions_test(jobs) == 3:
        min_cost = 0
        mean_cost = 0
        max_cost = 0
        duration = 0
        for job in jobs:
            result_single = []
            if provider == "Azure":
                for element in job:
                    print(element)
                    element[0] = azure_instance_name(element[0])
                result_single = (one_job_complete(job, provider, azure_regions, konfidenzgrad, client))
            elif provider == "AWS":

                result_single = (one_job_complete(list, provider, aws_regions, konfidenzgrad, client))

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
            result_single = (one_job_complete(jobs, provider, azure_regions, konfidenzgrad, client))
        elif provider == "AWS":
            result_single = (one_job_complete(list, provider, aws_regions, konfidenzgrad, client))

        results_single_sorted = sorted(result_single, key=lambda x: x[0][1])
        results_multiple_jobs = (results_single_sorted[0])
    return results_multiple_jobs

#Testing
"""
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
connection_string = os.getenv("MONGODB_URI")
client = MongoClient(connection_string)
list_test = [["FX48-12mds v2 Spot", 4002]],[["E2s v5 Spot", 3500]]
provider = "Azure"
list = list_test
konfidenzgrad = 95
results_job = (multiple_jobs(provider, list, konfidenzgrad, client))
print(results_job)
"""