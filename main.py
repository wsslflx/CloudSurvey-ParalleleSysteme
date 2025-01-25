from CloudSurvey_Package.computing_prices import multiple_jobs
from CloudSurvey_Package.storage_prices import calculate_complete_storage_price
from CloudSurvey_Package.help_methods import dimensions_test, generate_output_storage, generate_output
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from CloudSurvey_Package.fill_cost_maps import *
from CloudSurvey_Package.optimization_problem import *

load_dotenv()
connection_string_compute = os.getenv('MONGODB_URI')
connection_string_storage = os.getenv('MONGODB_URI2')

def main_storage(provider, list, konfidenzgrad, volume, premium, lrs):
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


def main_no_storage(provider, list, konfidenzgrad):
    client_compute = MongoClient(connection_string_compute)
    total_cost, single_cost = ((multiple_jobs(provider, list, konfidenzgrad, client_compute)))
    if dimensions_test(single_cost) == 1:
        generate_output(total_cost, single_cost, konfidenzgrad, False, provider)
    else:
        generate_output(total_cost, single_cost, konfidenzgrad, True, provider)

def main_optimization(provider, instance_list, konfidenzgrad, volume, premium, lrs):
    client_compute = MongoClient(connection_string_compute)
    client_storage = MongoClient(connection_string_storage)
    compute_cost_map = fill_compute_cost_map_all(provider, instance_list, konfidenzgrad, client_compute)
    storage_cost_map = fill_storage_cost_map(provider, volume, premium, lrs, instance_list, client_storage)
    transfer_cost_map = fill_transfer_cost_map(provider, client_storage)

    model, x_var = (optimize_with_triple_compute(compute_cost_map, storage_cost_map, transfer_cost_map))
    print("Status:", pulp.LpStatus[model.status])
    print("Objective:", pulp.value(model.objective))

    for key, var_obj in x_var.items():
        if var_obj.varValue > 0.5:  # chosen
            print("Chosen combination:", key, "Cost:", var_obj.varValue)

list_test = ([["FX48-12mds v2 Spot", 4002],["E2s v5 Spot", 3500]])
list_test_2 = [["FX48-12mds v2 Spot", 4002]]


main_optimization("Azure", list_test, 95, 200, False, False)
