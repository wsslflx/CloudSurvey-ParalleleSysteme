from CloudSurvey_Package.help_methods import *
import CloudSurvey_Package.constants as constants
from CloudSurvey_Package.math_operations import second_to_hour
from CloudSurvey_Package.db_operations import fetch_instance_prices, get_all_instancePriceperHour
import numpy as np

#fetch Instance Price from Database for specific time and return median
def get_instancePriceperHour(provider, instance, hour, region, client):
    """
        Retrieves the instance prices for a specific provider, instance type, hour, region, and client.

        Fetches data from the corresponding database based on the provider ("Azure" or "AWS") and returns a list
        of spot prices for the given time slot.

        Parameters:
          - provider: The cloud provider name (e.g., "Azure" or "AWS").
          - instance: The type of instance to fetch prices for.
          - hour: The specific hour for which the price is needed.
          - region: The region in which the instance is located.
          - client: The database client connection.

        Returns:
          A list of spot prices extracted from the database records.
        """
    list = []
    if provider == "Azure":
        list = fetch_instance_prices("AzureSpotPricesDB", "SpotPrices", instance, hour, region, client)
    if provider == "AWS":
        list = fetch_instance_prices("aws_spot_prices_db", "aws_spot_prices", instance, hour, region, client)
    prices = [doc['spot_price'] for doc in list]

    return prices

def cost_one_job(priceList, hourCombination, duration):
    """
        Calculates the minimum, mean, and maximum cost for executing one job over a given hour combination and duration.

        The method evaluates two scenarios based on whether the duration is an integer or fractional:
          - For integer durations, sums the costs for each hour in the combination.
          - For fractional durations, splits the cost calculation into beginning and ending partial hours
            and selects the scenario with the lower mean cost.

        Parameters:
          - priceList: A list of price details for each hour, where each element is a list of costs [min, mean, max].
          - hourCombination: A list of hours that form the combination to be evaluated.
          - duration: The duration of the job (can be a fractional hour).

        Returns:
          A tuple containing:
            - The computed minimum cost.
            - The computed mean cost.
            - The computed maximum cost.
            - The start time (or adjusted starting hour) for the job.
        """
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
        if hourCombination[0] == 0:
            startTime = 24 - started_hour
        else:
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
    """
        Determines the instance slot with the minimum cost for a specific job.

        The method fetches cost data per hour and generates all possible hour combinations based on the job duration.
        It then evaluates each combination to find the one with the minimum mean cost that is greater than zero.

        Parameters:
          - provider: The cloud provider name ("Azure" or "AWS").
          - instance: The instance type for which the cost is to be computed.
          - duration: The duration of the job (in hours).
          - region: The region where the instance is located.
          - konfidenzgrad: The confidence level used in filtering cost data.
          - client: The database client connection.

        Returns:
          Either a list representing the best slot with minimum cost [min_cost, mean_cost, max_cost, startTime, duration, region]
          or the string "no slot available" if no valid pricing data is found.
        """
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
    """
        Computes the complete cost details for one job across different instances and regions.

        For each instance in the provided list, the function calculates the job duration, evaluates the cost
        across the specified regions, and returns the optimal cost configuration for each instance.

        Parameters:
          - list: A list of instances with their respective durations (in seconds).
          - provider: The cloud provider name ("Azure" or "AWS").
          - regions: A list of regions to be evaluated.
          - konfidenzgrad: The confidence level used for cost calculations.
          - client: The database client connection.

        Returns:
          A list of tuples, each containing the best cost configuration for an instance, including details such as
          minimum cost, mean cost, maximum cost, and the associated starting hour.
        """
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
    """
        Aggregates cost computations for multiple jobs or a single job based on input dimensions.

        Depending on whether the input jobs list is 2-dimensional (a single job) or 3-dimensional (multiple jobs),
        the function computes the optimal cost configurations and aggregates total costs (min, mean, max) and durations.

        Parameters:
          - provider: The cloud provider name ("Azure" or "AWS").
          - jobs: A nested list representing one or multiple jobs with instance details.
          - konfidenzgrad: The confidence level used for cost filtering.
          - client: The database client connection.

        Returns:
          For multiple jobs:
            A tuple containing the total aggregated cost (min, mean, max, duration) and a list of single job cost details.
          For a single job:
            A tuple with total costs and detailed single job cost breakdown.
        """
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
        single_costs = formatting_compute_cost_multiple(results_multiple_jobs)
        return total_cost, single_costs

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
        single_costs = formatting_compute_cost_single(results_multiple_jobs)
        total_costs = [single_costs[0], single_costs[1], single_costs[2], single_costs[4]]
        return total_costs, single_costs

def compute_cost_for_start_hour(
    start_hour,
    duration_hours,
    extended_costs,
    prefix_min,
    prefix_mean,
    prefix_max
):
    """
        Computes the cost for a job starting at a specific hour based on its effective duration.

        The method evaluates two scenarios:
          - Scenario 1: The fractional part of the duration is applied at the beginning of the period.
          - Scenario 2: The fractional part is applied at the end.
        It calculates the minimum, mean, and maximum cost for each scenario using provided cost arrays and prefix sums,
        and then selects the scenario with the lower mean cost.

        Parameters:
          - start_hour: The hour at which the job is proposed to start.
          - duration_hours: The total duration of the job (can be fractional).
          - extended_costs: A list or array containing cost details [min, mean, max] for each hour.
          - prefix_min: Prefix sum array for minimum costs.
          - prefix_mean: Prefix sum array for mean costs.
          - prefix_max: Prefix sum array for maximum costs.

        Returns:
          A tuple containing:
            - The computed minimum cost.
            - The computed mean cost.
            - The computed maximum cost.
            - The adjusted starting time for the job.
        """
    int_dur = int(duration_hours)               # integer part of duration
    frac_dur = duration_hours - int_dur         # fractional part

    # Helper function to compute sum from prefix arrays
    def sum_cost(prefix_array, i, j):
        return prefix_array[j] - prefix_array[i]

    #  SCENARIO 1: partial at the beginning
    scenario1_min = 0.0
    scenario1_mean = 0.0
    scenario1_max = 0.0
    if frac_dur > 0:
        scenario1_min += extended_costs[start_hour][0] * frac_dur
        scenario1_mean += extended_costs[start_hour][1] * frac_dur
        scenario1_max += extended_costs[start_hour][2] * frac_dur

    start_index_1 = start_hour + 1
    end_index_1 = start_hour + 1 + int_dur
    if int_dur > 0:
        scenario1_min += sum_cost(prefix_min,  start_index_1, end_index_1)
        scenario1_mean += sum_cost(prefix_mean, start_index_1, end_index_1)
        scenario1_max += sum_cost(prefix_max,  start_index_1, end_index_1)

    # The final "reported" startTime if partial is at the beginning
    if start_hour != 0:
        scenario1_start_time = start_hour - frac_dur
    else:
        scenario1_start_time = 24 - frac_dur

    # SCENARIO 2: partial at the end
    scenario2_min = 0.0
    scenario2_mean = 0.0
    scenario2_max = 0.0

    start_index_2 = start_hour
    end_index_2 = start_hour + int_dur
    if int_dur > 0:
        scenario2_min += sum_cost(prefix_min,  start_index_2, end_index_2)
        scenario2_mean += sum_cost(prefix_mean, start_index_2, end_index_2)
        scenario2_max += sum_cost(prefix_max,  start_index_2, end_index_2)

    if frac_dur > 0:
        final_hour = start_hour + int_dur
        scenario2_min += extended_costs[final_hour][0] * frac_dur
        scenario2_mean += extended_costs[final_hour][1] * frac_dur
        scenario2_max += extended_costs[final_hour][2] * frac_dur

    # The final "reported" startTime if partial is at the end
    scenario2_start_time = float(start_hour)

    # Decide which scenario to pick based on mean cost
    if scenario1_mean < scenario2_mean:
        return scenario1_min, scenario1_mean, scenario1_max, scenario1_start_time
    else:
        return scenario2_min, scenario2_mean, scenario2_max, scenario2_start_time


def find_cheapest_slot_vectorized(instance_list, pricing_data, region, parallelization):
    """
    Finds the cheapest cost slot for each instance using vectorized operations based on pricing data and parallelization factors.

    For each instance (represented as [instance_type, duration_in_seconds]), the function:
      - Builds a NumPy array of hourly prices for the given region.
      - For each allowed parallelization factor, calculates the effective duration per instance and computes the cost
        for each possible starting hour (0-23) considering full days, extra full hours, and fractional hours.
      - Determines the starting hour that yields the lowest cost per instance.

    Parameters:
      - instance_list: A list of instances, each represented as [instance_type, duration_in_seconds].
      - pricing_data: A list of pricing records containing instance_type, region, hour, and spot_price.
      - region: The region in which the pricing data should be considered.
      - parallelization: A list of allowed parallelization factors (integers).

    Returns:
      A dictionary mapping each instance type to another dictionary that maps each parallelization factor to a tuple:
        (region, best_start_hour, total_cost, effective_duration)
    """
    results = {}
    # Build mapping: instance_type -> NumPy array of shape (24,) for the given region.
    instance_pricing = {}
    for entry in pricing_data:
        if entry['region'] != region:
            continue
        itype = entry['instance_type']
        hour = entry['hour']
        price = entry['spot_price']
        if itype not in instance_pricing:
            instance_pricing[itype] = np.full(24, np.nan)
        instance_pricing[itype][hour] = price

    for instance_type, duration in instance_list:
        # Skip if pricing data is incomplete
        if instance_type not in instance_pricing or np.isnan(instance_pricing[instance_type]).any():
            continue

        hourly_prices = instance_pricing[instance_type]
        daily_cost = np.sum(hourly_prices)  # cost for 24 hours
        results[instance_type] = {}

        # Process each allowed parallelization factor.
        for p in parallelization:
            # Effective duration per instance when using p instances.
            effective_duration = duration / p  # may be fractional seconds

            # Compute effective full hours and remainder for the effective duration.
            effective_full_hours = int(effective_duration // 3600)
            effective_remainder = effective_duration - effective_full_hours * 3600

            # Compute number of full days and extra full hours for the effective duration.
            full_days, extra_hours = divmod(effective_full_hours, 24)

            # For each starting hour (0 to 23), compute cost for the extra hours beyond full days.
            starts = np.arange(24)
            if extra_hours > 0:
                # For each start, sum the prices over extra_hours (cyclically).
                indices = (starts[:, None] + np.arange(extra_hours)) % 24
                extra_costs = np.sum(hourly_prices[indices], axis=1)
            else:
                extra_costs = np.zeros(24)

            # Fractional hour cost: use the price at the next hour (cyclic)
            fractional_indices = (starts + extra_hours) % 24
            fractional_costs = hourly_prices[fractional_indices] * (effective_remainder / 3600)

            # Total cost for each starting hour (for one instance)
            cost_per_instance = full_days * daily_cost + extra_costs + fractional_costs

            # Find the best starting hour (lowest cost per instance)
            best_index = int(np.argmin(cost_per_instance))
            best_instance_cost = float(cost_per_instance[best_index])

            results[instance_type][p] = (region, best_index, best_instance_cost, effective_duration)

    return results


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