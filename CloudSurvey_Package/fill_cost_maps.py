from CloudSurvey_Package.computing_prices import compute_cost_for_start_hour
from CloudSurvey_Package.help_methods import *
from CloudSurvey_Package.db_operations import get_all_instancePriceperHour
from CloudSurvey_Package.storage_prices import get_storage_cost, get_transfer_cost
from CloudSurvey_Package.math_operations import second_to_hour
import CloudSurvey_Package.constants as constants

def all_cost_instance(provider, instance, duration, region, konfidenzgrad, client, parallelization):
    """
    Returns a list of [min_cost, mean_cost, max_cost, startTime, duration, region, factor]
    for every possible start hour (0..23) for each parallelization factor,
    without filtering to the cheapest time slot.

    **Performance Improvements**:
    - We only consider 24 possible start hours.
    - We use prefix sums (48-hour extension) to compute min/mean/max in O(1) for each slot.
    - We handle partial hours at the beginning or end and pick the cheaper option by mean cost.
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
            cmin  *= factor
            cmean *= factor
            cmax  *= factor

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
    with all possible (cost_min, cost_mean, cost_max, duration) data.
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
            cost = get_transfer_cost(region_from, region_to, provider, client)
            transfer_cost_map[(region_from, region_to)] = cost
    return transfer_cost_map

