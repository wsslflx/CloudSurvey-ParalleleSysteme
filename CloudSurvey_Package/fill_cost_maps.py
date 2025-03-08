from CloudSurvey_Package.computing_prices import compute_cost_for_start_hour
from CloudSurvey_Package.help_methods import *
from CloudSurvey_Package.db_operations import get_all_instancePriceperHour
from CloudSurvey_Package.storage_prices import get_storage_cost, get_transfer_cost
from CloudSurvey_Package.math_operations import second_to_hour
import CloudSurvey_Package.constants as constants
from CloudSurvey_Package.db_operations import get_mean_spot_price
from CloudSurvey_Package.computing_prices import find_cheapest_slot_vectorized
def all_cost_instance(provider, instance, duration, region, konfidenzgrad, client, parallelization):
    """
        Computes cost metrics (min, mean, max) for every possible start hour (0..23)
        and for each parallelization factor for the given instance and region.

        Performance Improvements:
          - Considers only 24 possible start hours.
          - Uses prefix sums (with a 48-hour extension) to compute min/mean/max in O(1) per slot.
          - Handles partial hours at the beginning or end and selects the cheaper option based on mean cost.

        Linear Model Optimization:
          Builds and solves a linear model picking exactly ONE combination of:
            (r1, r2, i, s, p)

          Where:
            - storage is in region r1 (with instance i, parallel factor p),
            - transfer is (r1->r2),
            - compute is in region r2 (with instance i, parallel factor p), starting time s.

          The total cost = storage_cost_map[r1, i, p]
                         + transfer_cost_map[r1, r2]
                         + compute_cost_map[r2, i, s, p][0][1]
          Exactly one tuple is chosen (x=1), to minimize total cost.
        """
    # 1) Retrieve per-hour prices for this instance/region
    costs_per_hour = get_all_instancePriceperHour(provider, instance, region, konfidenzgrad, client)
    if not costs_per_hour:
        # No price data
        return []

    # Check if mean is all zeros => no real cost
    has_nonzero_mean = any(hour_cost[1] != 0 for hour_cost in costs_per_hour)
    if not has_nonzero_mean:
        return []

    # 2) Build extended array and prefix sums
    extended_costs, prefix_min, prefix_mean, prefix_max = build_prefix_arrays(costs_per_hour)

    results = []
    # 3) For each parallelization factor, compute costs for each of the 24 start hours
    for factor in parallelization:
        par_duration = duration / factor
        for start_hour in range(24):
            cmin, cmean, cmax, final_start_time = compute_cost_for_start_hour(
                start_hour,
                par_duration,
                extended_costs,
                prefix_min,
                prefix_mean,
                prefix_max
            )
            # Scale by factor
            cmin *= factor
            cmean *= factor
            cmax *= factor

            # Only add if mean cost > 0
            if cmean > 0:
                results.append([
                    cmin,
                    cmean,
                    cmax,
                    final_start_time,
                    par_duration,
                    region,
                    factor
                ])
    return results


def fill_compute_cost_map_all(provider, instance_list, konfidenzgrad, client, parallelization):
    """
    Builds a dictionary keyed by (region, instance_type, start_time, factor)
    containing lists of (cost_min, cost_mean, cost_max, duration) tuples
    computed for all valid combinations of start times and parallelization factors
    for every instance in each region.

    Linear Model Optimization:
      Builds and solves a linear model picking exactly ONE combination of:
        (r1, r2, i, s, p)

      Where:
        - storage is in region r1 (with instance i, parallel factor p),
        - transfer is (r1->r2),
        - compute is in region r2 (with instance i, parallel factor p), starting time s.

      The total cost = storage_cost_map[r1, i, p]
                     + transfer_cost_map[r1, r2]
                     + compute_cost_map[r2, i, s, p][0][1]
      Exactly one tuple is chosen (x=1), to minimize total cost.
    """
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
                dict_key = (reg, instance_type, start_time, factor)
                if dict_key not in compute_cost_map:
                    compute_cost_map[dict_key] = []

                compute_cost_map[dict_key].append(
                    (cost_min, cost_mean, cost_max, dur)
                )

    return compute_cost_map


def fill_compute_cost_map_all_performance(provider, instance_list, client, parallelization):
    """
    Builds a compute cost map dictionary where each key is (region, instance_type, start_time, factor)
    and the corresponding value is a tuple (best_cost, effective_duration) derived by selecting
    the cheapest compute slot based on vectorized pricing.

    Linear Model Optimization:
      Builds and solves a linear model picking exactly ONE combination of:
        (r1, r2, i, s, p)

      Where:
        - storage is in region r1 (with instance i, parallel factor p),
        - transfer is (r1->r2),
        - compute is in region r2 (with instance i, parallel factor p), starting time s.

      The total cost = storage_cost_map[r1, i, p]
                     + transfer_cost_map[r1, r2]
                     + compute_cost_map[r2, i, s, p][0][1]
      Exactly one tuple is chosen (x=1), to minimize total cost.
    """
    compute_cost_map = {}

    if provider == "Azure":
        regions = constants.azure_regions
    else:
        regions = constants.aws_regions

    instance_types = [item[0] for item in instance_list]
    pricing_list = get_mean_spot_price(client, instance_types, provider)

    for region in regions:
        best_slots = (find_cheapest_slot_vectorized(instance_list, pricing_list, region, parallelization))

        for instance_type, parallel_results in best_slots.items():
            for factor, (reg, start_time, best_cost, effective_duration) in parallel_results.items():
                dict_key = (reg, instance_type, start_time, factor)
                compute_cost_map[dict_key] = (best_cost, effective_duration)

    return compute_cost_map



def fill_storage_cost_map(provider, volume, premium, lrs, instance_list, client, parallelization):
    """
    Creates a dictionary mapping each region (and instance/parallel factor) to its respective storage cost.
    The cost is calculated based on the storage price, volume, and usage duration (adjusted for parallelization).

    Linear Model Optimization:
      Builds and solves a linear model picking exactly ONE combination of:
        (r1, r2, i, s, p)

      Where:
        - storage is in region r1 (with instance i, parallel factor p),
        - transfer is (r1->r2),
        - compute is in region r2 (with instance i, parallel factor p), starting time s.

      The total cost = storage_cost_map[r1, i, p]
                     + transfer_cost_map[r1, r2]
                     + compute_cost_map[r2, i, s, p][0][1]
      Exactly one tuple is chosen (x=1), to minimize total cost.
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
    Creates a dictionary mapping each (regionFrom, regionTo) pair to its transfer cost per GB.
    This map is used to estimate data transfer costs between regions.

    Linear Model Optimization:
      Builds and solves a linear model picking exactly ONE combination of:
        (r1, r2, i, s, p)

      Where:
        - storage is in region r1 (with instance i, parallel factor p),
        - transfer is (r1->r2),
        - compute is in region r2 (with instance i, parallel factor p), starting time s.

      The total cost = storage_cost_map[r1, i, p]
                     + transfer_cost_map[r1, r2]
                     + compute_cost_map[r2, i, s, p][0][1]
      Exactly one tuple is chosen (x=1), to minimize total cost.
    """
    transfer_cost_map = {}

    if provider == "Azure":
        regions = constants.azure_regions
    else:
        regions = constants.aws_regions

    # Enumerate all region pairs
    for region_from in regions:
        for region_to in regions:
            cost = get_transfer_cost(region_from, region_to, provider, client)
            transfer_cost_map[(region_from, region_to)] = cost
    return transfer_cost_map

"""
#Testing
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
connection_string = os.getenv('MONGODB_URI')
connection_string2 = os.getenv('MONGODB_URI2')

client = MongoClient(connection_string)

list_test = [["FX48-12mds v2 Spot", 90900], ["E2s v5 Spot", 3000]]

(fill_compute_cost_map_all_performance("Azure", list_test, client, [1, 2]))
"""