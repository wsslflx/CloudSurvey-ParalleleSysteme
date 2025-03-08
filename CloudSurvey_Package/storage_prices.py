import math
from CloudSurvey_Package.math_operations import gb_to_gib
from CloudSurvey_Package.db_operations import fetch_storage_prices, fetch_transfer_prices


def get_transfer_cost(fromRegion, toRegion, provider, client):
    """
       Calculates the data transfer cost between two regions for a given provider.

       Where:
         - If both regions are the same, cost is 0.
         - For Azure, inter EU data transfers cost a fixed 0.0192 per GB.
         - For AWS, attempts to fetch the transfer price from the database; if not available,
           a default cost of 0.04 per GB is applied.
       """
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
    """
        Determines the SKU name for storage based on volume, premium, and redundancy options.

        Where:
          - Volume in GB is converted to GiB.
          - A mapping (storage_map) is used to pick a numeric suffix based on volume size.
          - Returns a SKU string:
              - If both LRS and premium are true, returns "P{number} LRS".
              - If only LRS is true, returns "E{number} LRS".
              - If only premium is true, returns "P{number} ZRS".
              - Otherwise, returns "E{number} ZRS".
    """
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

    if lrs and premium:
        return f"P{number} LRS"
    if lrs:
        return f"E{number} LRS"
    if premium:
        return f"P{number} ZRS"
    return f"E{number} ZRS"


def get_storage_cost(provider, volume, premium, lrs, client):
    """
        Retrieves storage cost details based on provider, volume, and storage options.

        Where:
          - For Azure, converts volume to GiB and determines the SKU name using get_storage_skuname.
          - For AWS, chooses "gp2" for premium storage or "gp3" otherwise.
          - Fetches and returns a list of storage price details for the given provider and SKU.
    """
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
    """
    Calculates the storage price for a given region and duration.

    Where:
      - For both Azure and AWS, the hourly price is computed by dividing the region's
        base price by 7300 (approximate monthly hours).
      - For Azure, the duration is ceiled to the next hour.
      - For AWS, the duration is taken as is.
      - Returns the total storage cost for the duration.
    """
    if provider == "Azure":
        hour_price = (price_region["price"] / 7300)
        return (hour_price * math.ceil(duration))
    elif provider == "AWS":
        hour_price = (price_region["price"] / 7300)
        return (hour_price * (duration))

def calculate_transfer_cost(region_from, region_to, provider, volume, client):
    """
        Computes the total transfer cost between two regions based on volume.

        Where:
          - Retrieves the per GB transfer cost using get_transfer_cost.
          - Multiplies the transfer cost per GB by the volume to determine the total cost.
    """
    transfer_cost = get_transfer_cost(region_from, region_to, provider, client)
    return transfer_cost * volume

def calculate_complete_storage_price(provider, volume, premium, lrs, client, duration, regionTo):
    """
        Calculates the complete cost for storage including both storage and transfer fees.

        Where:
          - Retrieves storage price details using get_storage_cost.
          - Determines the lowest storage price among regions and calculates its total cost over the duration.
          - Computes the transfer cost from the region with the lowest storage price to the target region.
          - If the storage cost in the target region is lower than the combined cost (storage + transfer)
            from the cheapest region, it returns that cost and target region.
          - Otherwise, returns the combined cost and the region of the lowest storage price.
    """
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
