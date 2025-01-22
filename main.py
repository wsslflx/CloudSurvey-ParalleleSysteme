from CloudSurvey_Package.computing_prices import multiple_jobs
from CloudSurvey_Package.storage_prices import calculate_complete_storage_price
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
connection_string_compute = os.getenv('MONGODB_URI')
connection_string_storage = os.getenv('MONGODB_URI2')

def main(provider, list, konfidenzgrad, volume):
    client_compute = MongoClient(connection_string_compute)
    client_storage = MongoClient(connection_string_storage)

    total_cost, single_cost = ((multiple_jobs(provider, list, konfidenzgrad, client_compute)))
    print(total_cost)
    print(single_cost)



list_test = [["FX48-12mds v2 Spot", 4002]],[["E2s v5 Spot", 3500]]
list_test_2 = [["FX48-12mds v2 Spot", 4002]]


main("Azure", list_test_2, 95, 200)
