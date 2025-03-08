from CloudSurvey_Package.computing_prices import multiple_jobs
from CloudSurvey_Package.storage_prices import calculate_complete_storage_price
from CloudSurvey_Package.fill_cost_maps import *
from CloudSurvey_Package.optimization_problem import *
import os
from dotenv import load_dotenv
from pymongo import MongoClient

def main_storage(provider, list, konfidenzgrad, volume, premium, lrs, connection_string_compute, connection_string_storage):
    client_compute = MongoClient(connection_string_compute)
    client_storage = MongoClient(connection_string_storage)

    total_cost, single_cost = (multiple_jobs(provider, list, konfidenzgrad, client_compute))
    if dimensions_test(single_cost) == 1:
        storage_cost = calculate_complete_storage_price(provider, volume, premium, lrs, client_storage, total_cost[3], single_cost[5])
        generate_output_storage(total_cost, single_cost, storage_cost, konfidenzgrad, False, provider)
    elif dimensions_test(single_cost) >= 2:
        single_cost_sorted = sorted(single_cost, key=lambda x: x[4])
        storage_cost = calculate_complete_storage_price(provider, volume, premium, lrs, client_storage, total_cost[3], single_cost_sorted[0][3])
        generate_output_storage(total_cost, single_cost, storage_cost, konfidenzgrad, True, provider)


def main_no_storage(provider, list, konfidenzgrad, connection_string_compute):
    client_compute = MongoClient(connection_string_compute)
    total_cost, single_cost = ((multiple_jobs(provider, list, konfidenzgrad, client_compute)))
    if dimensions_test(single_cost) == 1:
        generate_output(total_cost, single_cost, konfidenzgrad, False, provider)
    else:
        generate_output(total_cost, single_cost, konfidenzgrad, True, provider)

def main_optimization(provider, instance_list, konfidenzgrad, volume, premium, lrs, parallelization):
    """
        Builds and solves a linear model picking exactly ONE combination of:
          (r1, r2, i, s, p)

        Where:
          - storage is in region r1 (with instance i, parallel factor p),
          - transfer is (r1->r2),
          - compute is in region r2 (with instance i, parallel factor p),
            starting time s.

        The total cost = storage_cost_map[r1, i, p]
                       + transfer_cost_map[r1, r2]
                       + compute_cost_map[r2, i, s, p][0][1]
        Exactly one tuple is chosen (x=1), to minimize total cost.

        This function performs the following steps:
          - Loads environment variables and retrieves connection strings.
          - Establishes connections to compute and storage databases.
          - Normalizes the provider name to "AWS" or "Azure".
          - Constructs cost maps for compute, storage, and transfer by calling corresponding functions.
          - Builds and solves the optimization model using the optimize function.
          - Prints the model status, objective value, and chosen cost combinations.

        Parameters:
          provider (str): Cloud provider name (e.g., "AWS" or "Azure").
          instance_list (list): List of instances with their associated parameters.
          konfidenzgrad (int): Confidence level used in cost computations.
          volume (float): Volume metric for storage cost calculation.
          premium (bool): Indicator for using premium storage.
          lrs (bool): Flag for local redundant storage.
          parallelization (list): List of parallelization factors to consider.
        """
    load_dotenv()
    connection_string_compute = os.getenv('MONGODB_URI')
    connection_string_storage = os.getenv('MONGODB_URI2')
    client_compute = MongoClient(connection_string_compute)
    client_storage = MongoClient(connection_string_storage)

    if provider.lower() == 'aws':
        provider = "AWS"
    else:
        provider = "Azure"

    # compute_cost_map = fill_compute_cost_map_all(provider, instance_list, konfidenzgrad, client_compute, parallelization)
    compute_cost_map = fill_compute_cost_map_all_performance(provider, instance_list, client_compute, parallelization)
    storage_cost_map = fill_storage_cost_map(provider, volume, premium, lrs, instance_list, client_storage, parallelization)
    transfer_cost_map = fill_transfer_cost_map(provider, client_storage)

    model, x_var = (optimize(compute_cost_map, storage_cost_map, transfer_cost_map))
    print("Status:", pulp.LpStatus[model.status])
    print("Objective:", pulp.value(model.objective))

    response = {
        "status": pulp.LpStatus[model.status],
        "objective": pulp.value(model.objective),
        "chosen_combinations": []
    }

    for key, var_obj in x_var.items():
        if var_obj.varValue > 0.5:  # chosen
            print("Chosen combination:", key, "Cost:", var_obj.varValue)
            response["chosen_combinations"].append({
                "combination": key,
                "cost": var_obj.varValue
            })

"""
#Testing
import os
from dotenv import load_dotenv
load_dotenv()
# Get connection strings from environment variables
connection_string_compute = os.getenv('MONGODB_URI')
connection_string_storage = os.getenv('MONGODB_URI2')
instance_list = [["FX48-12mds v2 Spot", 360000], ["E2s v5 Spot", 300000]]
parallelization = [1, 2, 4]
print(main_optimization("Azure", instance_list, 95, 200, True, False, parallelization))
"""