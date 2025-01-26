from CloudSurvey_Package.computing_prices import cost_one_job
from CloudSurvey_Package.help_methods import *
from CloudSurvey_Package.db_operations import get_all_instancePriceperHour
from CloudSurvey_Package.storage_prices import get_storage_cost, get_transfer_cost
from CloudSurvey_Package.math_operations import second_to_hour
import CloudSurvey_Package.constants as constants


def all_cost_instance(provider, instance, duration, region, konfidenzgrad, client, parallelization):
    """
    Returns a list of [min_cost, mean_cost, max_cost, startTime, duration, region]
    for *every* possible time slot combination, without filtering to the cheapest.

    :param provider: "AWS" or "Azure"
    :param instance: instance name/string (e.g. "m5.large")
    :param duration: job duration in hours (can be float)
    :param region: region name/string
    :param konfidenzgrad: confidence level used in your DB queries
    :param client: database client or connection
    :param parallelization: set of possible parallelization factors
    :return: list of lists, one entry per time slot combination
    """
    costs_slot = []

    # Retrieve all the per-hour prices for this instance/region
    costsPerHour = get_all_instancePriceperHour(provider, instance, region, konfidenzgrad, client)
    if not costsPerHour:
        # No price data
        return []

    for factor in parallelization:
        parallelization_duration = duration / factor
        # Generate all possible hour combinations for the given duration and parallelization
        hour_combinations = get_hour_combinations((parallelization_duration))

        # For each possible time slot combination, compute min/mean/max
        for timeSlot in hour_combinations:
            cost_min, cost_mean, cost_max, startTime = cost_one_job(
                priceList=costsPerHour,
                hourCombination=timeSlot,
                duration=parallelization_duration
            )
            if cost_mean > 0:
                cost_min = cost_min * factor
                cost_mean = cost_mean * factor
                cost_max = cost_max * factor
                costs_slot.append([cost_min, cost_mean, cost_max, startTime, parallelization_duration, region, factor])

    return costs_slot


def fill_compute_cost_map_all(provider, instance_list, konfidenzgrad, client, parallelization):
    compute_cost_map = {}

    if provider == "Azure":
        regions = constants.azure_regions
    else:
        regions = constants.aws_regions

    for instance_info in instance_list:
        instance_type = instance_info[0]  # e.g. "m5.large"
        duration_in_seconds = instance_info[1]
        duration_hours = second_to_hour(duration_in_seconds)

        for region in regions:
            # Get all cost possibilities for this (instance, region, duration)
            costs_for_all_slots = all_cost_instance(
                provider=provider,
                instance=instance_type,
                duration=duration_hours,
                region=region,
                konfidenzgrad=konfidenzgrad,
                client=client,
                parallelization=parallelization
            )

            for cost_min, cost_mean, cost_max, start_time, dur, reg, factor in costs_for_all_slots:
                # Build the dictionary key
                dict_key = (reg, instance_type, start_time, factor)

                # Add to the dictionary
                if dict_key not in compute_cost_map:
                    compute_cost_map[dict_key] = []

                # Append a tuple of the cost data
                compute_cost_map[dict_key].append(
                    (cost_min, cost_mean, cost_max, dur)
                )

    return compute_cost_map


def fill_storage_cost_map(provider, volume, premium, lrs, instance_list, client, parallelization):
    """
    Creates a map { region: storage_cost } for *all* regions
    available in the DB for the specified storage SKU, ignoring transfer cost.

    :param provider:        "AWS" or "Azure"
    :param volume:          The storage volume in GB
    :param premium:         Boolean indicating if premium storage
    :param lrs:             Boolean indicating if LRS or ZRS (Azure-specific)
    :param client:          DB/API client
    :param duration_hours:  Duration in hours for which we calculate storage cost
    :return: dict { region_name: cost_for_that_region }
    """

    storage_price_list = get_storage_cost(provider, volume, premium, lrs, client)

    storage_cost_map = {}

    for price_info in storage_price_list:
        for instance in instance_list:
            for factor in parallelization:
                region_name = price_info["region"]
                instance_name = instance[0]
                hour_duration = second_to_hour(int(instance[1]))
                hour_duration_parallelization = hour_duration / factor
                cost = (price_info["price"] / 730) * hour_duration_parallelization
                storage_cost_map[region_name, instance_name, factor] = cost

    return storage_cost_map


def fill_transfer_cost_map(provider, client):
    """
    Creates a map of transfer costs for *all* (regionFrom, regionTo) pairs.
    Key:   (regionFrom, regionTo)
    Value: transfer cost per GB
    """
    transfer_cost_map = {}

    if provider == "Azure":
        regions = constants.azure_regions
    else:
        regions = constants.aws_regions

    # Enumerate all region pairs
    for region_from in regions:
        for region_to in regions:
            # Use your existing function to fetch cost
            cost = get_transfer_cost(region_from, region_to, provider, client)
            transfer_cost_map[(region_from, region_to)] = cost

    return transfer_cost_map




import os
from dotenv import load_dotenv
from pymongo import MongoClient
load_dotenv()
connection_string_compute = os.getenv('MONGODB_URI')
connection_string_storage = os.getenv('MONGODB_URI2')
client_storage = MongoClient(connection_string_storage)

"""
client_compute = MongoClient(connection_string_compute)


print(fill_compute_cost_map_all(
    provider="Azure",
    instance_list= [["FX48-12mds v2 Spot", 4002],["E2s v5 Spot", 3500]],
    regions = constants.azure_regions,
    konfidenzgrad=95,
    client=client_compute
))
"""