import math
from CloudSurvey_Package.math_operations import gb_to_gib
from CloudSurvey_Package.db_operations import fetch_storage_prices, fetch_transfer_prices
"""
from pymongo import MongoClient
import os
from dotenv import load_dotenv
"""

def get_transfer_cost(fromRegion, toRegion, provider, client):
    if toRegion == fromRegion:
        return 0
    if provider == "Azure":
        return 0.0192 #research shows that for inter eu data transfer it always costs 0.0192 per GB
    if provider == "AWS":
        price = fetch_transfer_prices(provider, fromRegion, toRegion, client)
        if price:
            return price
        return 0.04 #standard transfer cost for aws

def get_storage_skuname(volume, premium, lrs):
    number = 1
    storage_map = {
        4: "1",
        8: "2",
        16: "3",
        32: "4",
        64: "6",
        128: "10",
        256: "15",
        512: "20",
        1024: "30",
        2048: "40",
        4096: "50",
        8192: "60",
        16384: "70",
        32768: "80",
    }
    volume_in_gib = gb_to_gib(volume)

    for key in storage_map:
        if volume_in_gib < key:
            number = storage_map[key]

    if lrs:
        if premium:
            return "P" + str(number) + " LRS"
        else:
            return "E" + str(number) + " LRS"
    else:
        if premium:
            return "P" + str(number) + " ZRS"
        else:
            return "E" + str(number) + " ZRS"


def get_storage_cost(provider, volume, premium, lrs, client):
    storage_prices = []
    if provider == "Azure":
        volume_in_gib = gb_to_gib(volume)
        skuName = get_storage_skuname(volume_in_gib, premium, lrs)
        storage_prices = fetch_storage_prices(provider, skuName, client)
    elif provider == "AWS":
        if premium:
            skuName = "gp2"
        else:
            skuName = "gp3"
        storage_prices = fetch_storage_prices(provider, skuName, client)
    return storage_prices

def calculate_storage_price(price_region, duration, provider):
    if provider == "Azure":
        hour_price = (price_region["price"] / 7300)
        print(hour_price)
        print(math.ceil(duration))
        print((hour_price * math.ceil(duration)))
        return (hour_price * math.ceil(duration))
    elif provider == "AWS":
        hour_price = (price_region["price"] / 7300)
        return (hour_price * (duration))

def calculate_transfer_cost(region_from, region_to, provider, volume, client):
    transfer_cost = get_transfer_cost(region_from, region_to, provider, client)
    return transfer_cost * volume

def calculate_complete_storage_price(provider, volume, premium, lrs, client, duration, regionTo):
    price_region = get_storage_cost(provider, volume, premium, lrs, client)
    lowest_price_region = min(price_region, key=lambda x: x["price"])
    lowest_price = calculate_storage_price(lowest_price_region, duration, provider)
    transfer_cost = calculate_transfer_cost(lowest_price_region["region"], regionTo, provider, volume, client)
    complete_cost_cheapestRegion = lowest_price + transfer_cost
    
    priceToRegion_list = [item for item in price_region if item['region'] == regionTo]
    
    if priceToRegion_list:
        priceToRegion = priceToRegion_list[0]
        storagePriceToRegion = calculate_storage_price(priceToRegion, duration, provider)

        if storagePriceToRegion < complete_cost_cheapestRegion:
            return [storagePriceToRegion, regionTo]

    return [complete_cost_cheapestRegion, lowest_price_region["region"]]

"""
load_dotenv()
connection_string = os.getenv("MONGODB_URI2")

client = MongoClient(connection_string)

print(calculate_complete_storage_price("AWS", 100, False, False, client, 11.5, "eu-west-2"))
"""