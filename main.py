from CloudSurvey_Package.computing_prices import multiple_jobs
from CloudSurvey_Package.storage_prices import calculate_complete_storage_price
from CloudSurvey_Package.help_methods import dimensions_test, generate_output_storage, generate_output
from pymongo import MongoClient
import os
from dotenv import load_dotenv

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


list_test = [["FX48-12mds v2 Spot", 4002]],[["E2s v5 Spot", 3500]]
list_test_2 = [["FX48-12mds v2 Spot", 4002]]


main_storage("Azure", list_test, 95, 200, False, False)
