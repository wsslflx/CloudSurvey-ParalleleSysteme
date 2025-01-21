import math
from CloudSurvey_Package.math_operations import gb_to_gib
from CloudSurvey_Package.db_operations import fetch_storage_prices

def get_transfer_cost(region_from, region_to, provider, volume):
    if provider == "Azure":
        return volume * 0.0192 #research shows that for inter eu data transfer it always costs 0.0192 per GB

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
    if provider == "Azure":
        volume_in_gib = gb_to_gib(volume)
        skuName = get_storage_skuname(volume_in_gib, premium, lrs)
        storage_prices = fetch_storage_prices(provider, skuName, client)
        lowest_price_region = min(storage_prices, key=lambda x: x["price"])
        return lowest_price_region

def calculate_storage_price(price_region, duration):
    hour_price = (price_region["price"] / 7300)
    return (hour_price * math.ceil(duration))

def calculate_transfer_cost(region_from, region_to, provider, volume):
    transfer_cost = get_transfer_cost(region_from, region_to, provider, volume)
    return transfer_cost * volume

def calculate_complete_storage_price(provider, volume, premium, lrs, client, duration, region_to):
    lowest_price_region = get_storage_cost(provider, volume, premium, lrs, client)
    lowest_price = calculate_storage_price(lowest_price_region, duration)
    transfer_cost = calculate_transfer_cost(lowest_price_region["region"], region_to, provider, volume)
    complete_cost = lowest_price_region + transfer_cost
    return [complete_cost, lowest_price_region["region"]]
