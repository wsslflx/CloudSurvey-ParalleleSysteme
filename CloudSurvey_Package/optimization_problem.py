from pulp import LpProblem, LpMinimize, LpVariable, lpSum, LpBinary, PULP_CBC_CMD

import CloudSurvey_Package.constants as constants
from CloudSurvey_Package.computing_prices import get_all_instancePriceperHour, get_hour_combinations, cost_one_job
from CloudSurvey_Package.help_methods import *
from CloudSurvey_Package.db_operations import get_all_instancePriceperHour
from CloudSurvey_Package.storage_prices import get_storage_cost,calculate_storage_price


def all_cost_instance(provider, instance, duration, region, konfidenzgrad, client):
    """
    Returns a list of [min_cost, mean_cost, max_cost, startTime, duration, region]
    for *every* possible time slot combination, without filtering to the cheapest.

    :param provider: "AWS" or "Azure"
    :param instance: instance name/string (e.g. "m5.large")
    :param duration: job duration in hours (can be float)
    :param region: region name/string
    :param konfidenzgrad: confidence level used in your DB queries
    :param client: database client or connection
    :return: list of lists, one entry per time slot combination
    """
    costs_slot = []

    # Retrieve all the per-hour prices for this instance/region
    costsPerHour = get_all_instancePriceperHour(provider, instance, region, konfidenzgrad, client)
    if not costsPerHour:
        # No price data
        return []

    # Generate all possible hour combinations for the given duration
    # (This is presumably your existing function that returns a list of hour arrays)
    hour_combinations = get_hour_combinations(duration)

    # For each possible time slot combination, compute min/mean/max
    for timeSlot in hour_combinations:
        cost_min, cost_mean, cost_max, startTime = cost_one_job(
            priceList=costsPerHour,
            hourCombination=timeSlot,
            duration=duration
        )
        if cost_mean > 0:
            costs_slot.append([cost_min, cost_mean, cost_max, startTime, duration, region])

    return costs_slot


def fill_compute_cost_map_all(provider, instance_list, regions, konfidenzgrad, client):
    compute_cost_map = {}

    for instance_info in instance_list:
        instance_type = instance_info[0]  # e.g. "m5.large"
        duration_in_seconds = instance_info[1]
        duration_hours = second_to_hour(duration_in_seconds)

        for region in regions:
            # Get *all* cost possibilities for this (instance, region, duration)
            costs_for_all_slots = all_cost_instance(
                provider=provider,
                instance=instance_type,
                duration=duration_hours,
                region=region,
                konfidenzgrad=konfidenzgrad,
                client=client
            )

            for cost_min, cost_mean, cost_max, start_time, dur, reg in costs_for_all_slots:
                # Build the dictionary key
                dict_key = (reg, instance_type, start_time)

                # Add or append to the dictionary
                if dict_key not in compute_cost_map:
                    compute_cost_map[dict_key] = []

                # Append a tuple of the cost data
                compute_cost_map[dict_key].append(
                    (cost_min, cost_mean, cost_max, dur)
                )

    return compute_cost_map


import math
from CloudSurvey_Package.math_operations import gb_to_gib
from CloudSurvey_Package.db_operations import (
    fetch_storage_prices,
    fetch_transfer_prices
)
from CloudSurvey_Package.math_operations import second_to_hour

def fill_storage_cost_map(provider, volume, premium, lrs, client, duration_hours):
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

    # 1) Get ALL region-prices for this storage spec from DB
    #    Example of returned structure:
    #    [
    #      {"region": "germanywestcentral", "price": 12.4},
    #      {"region": "polandcentral",      "price": 10.3},
    #      ...
    #    ]
    storage_price_list = get_storage_cost(provider, volume, premium, lrs, client)

    # 2) Build a dictionary: region -> cost_for_that_region
    storage_cost_map = {}

    for price_info in storage_price_list:
        region_name = price_info["region"]
        # Calculate cost ignoring any transfer charge
        region_cost = calculate_storage_price(price_info, duration_hours, provider)
        # Store in the map
        storage_cost_map[region_name] = region_cost

    return storage_cost_map





import os
from dotenv import load_dotenv
from pymongo import MongoClient
load_dotenv()
connection_string_compute = os.getenv('MONGODB_URI')
connection_string_storage = os.getenv('MONGODB_URI2')
client_storage = MongoClient(connection_string_storage)

print(fill_storage_cost_map("Azure", 400, True, False, client_storage, 50))
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