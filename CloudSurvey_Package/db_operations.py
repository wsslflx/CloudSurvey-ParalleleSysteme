from CloudSurvey_Package.math_operations import calculate_konfidenzintervall
import math

def get_database(db_name, client):
    return client[db_name]

def fetch_all_hours_prices(db, collection_name, instance_type, region):
    collection = db[collection_name]
    query = {
        "instance_type": instance_type,
        "region": region
    }
    cursor = collection.find(query).sort("timestamp", 1)

    return list(cursor)

def fetch_instance_prices(db_name, collection_name,
                          instance_type, hour, region, client):
    db = client[db_name]
    collection = db[collection_name]

    query = {
        "instance_type": instance_type,
        "hour": hour,
        "region": region
    }

    cursor = collection.find(query).sort("timestamp", 1)

    results = list(cursor)

    return results

def get_all_instancePriceperHour(provider, instance, region, konfidenzgrad, client):
    if provider == "Azure":
        db = get_database("AzureSpotPricesDB", client)
        collection_name = "SpotPrices"
        docs = fetch_all_hours_prices(db, collection_name, instance, region)
        prices_by_hour = {h: [] for h in range(24)}  # dict of hour -> list of prices
        for doc in docs:
            hour = doc['hour']
            spot_price = doc['spot_price']
            prices_by_hour[hour].append(spot_price)

    elif provider == "AWS":
        db = get_database("aws_spot_prices_db", client)
        collection_name = "aws_spot_prices"
        docs = fetch_all_hours_prices(db, collection_name, instance, region)
        prices_by_hour = {h: [] for h in range(24)}  # dict of hour -> list of prices
        for doc in docs:
            hour = doc['hour']
            spot_price = doc['spot_price_eur']
            prices_by_hour[hour].append(spot_price)


    costs = []
    for h in range(24):
        price_list = prices_by_hour[h]
        if price_list:
            konfidenz_prices = calculate_konfidenzintervall(price_list, konfidenzgrad)
        else:
            konfidenz_prices = [0, 0, 0]  # or handle empty data gracefully
        costs.append(konfidenz_prices)

    return costs

def fetch_storage_prices(provider, skuName, client):
    if provider == "Azure":
        db = get_database("azure_storage_pricing_db", client)
        collection_name = "StoragePrices"
        query = {
            "skuName": {"$regex": f"^{skuName}$", "$options": "i"},  # Case-insensitive match
            "unitOfMeasure": "1/Month",
        }
        projection = {"_id": 0, "region": 1, "skuName": 1, "price": 1}
        try:
            # Fetch results with query and projection
            cursor = db[collection_name].find(query, projection).sort("timestamp", 1)

            # Convert to a list of dictionaries and extract required fields
            results = list(cursor)
            output = [{"region": doc["region"], "skuName": doc["skuName"], "price": doc["price"]} for doc in results]

            if not results:
                print("No documents found.")
            return output
        except Exception as e:
            print(f"Error querying the database: {e}")
            return []

    if provider == "AWS":
        db = get_database("aws_storage_pricing_db", client)
        collection_name = "aws_ebs_prices"
        query = {
            "description": {"$regex": skuName, "$options": "i"},  # Case-insensitive match -> Sku Name here gp3 or gp2
        }
        projection = {"_id": 0, "region": 1, "description" : 1, "price": 1}
    try:
        # Fetch results with query and projection
        cursor = db[collection_name].find(query, projection).sort("timestamp", 1)
        # Convert to a list of dictionaries and extract required fields
        results = list(cursor)
        output = [{"region": doc["region"], "description": doc["description"], "price": doc["price"]} for doc in results]

        if not results:
            print("No documents found.")
        return output
    except Exception as e:
        print(f"Error querying the database: {e}")
        return []

    return []

def fetch_transfer_prices(provider, fromRegion, toRegion, client):
    if provider == "AWS":
        db = get_database("aws_storage_pricing_db", client)
        collection_name = "aws_ebs_prices"
        query = {
            "fromRegion": fromRegion,
            "toRegion": toRegion
        }
        projection = {"_id": 0, "price": 1}

        try:
            # Fetch results with query and projection
            cursor = db[collection_name].find(query, projection).sort("timestamp", 1)

            # Convert to a list of dictionaries and extract required fields
            results = list(cursor)
            output = [{"price": doc["price"]} for doc in results]

            if not results:
                print("No documents found.")
            return output
        except Exception as e:
            print(f"Error querying the database: {e}")
            return []

def get_mean_spot_price(client, instance_types, provider):
    if provider == "AWS":
        db = get_database("aws_spot_prices_db", client)
        collection_name = "aws_spot_prices"
        prices_per_hour = get_mean_spot_prices_aws(db, collection_name, instance_types)
    else:
        db = get_database("AzureSpotPricesDB", client)
        collection_name = "SpotPrices"
        prices_per_hour = get_mean_spot_prices_azure(db, collection_name, instance_types)
    return prices_per_hour


def get_mean_spot_prices_aws(db, collection_name, instance_types):

    collection = db[collection_name]

    pipeline = [
        # Filter documents by instance types
        {
            "$match": {
                "instance_type": {"$in": instance_types}
            }
        },
        # Group by instance_type, hour, and region, calculating the average spot_price_eur
        {
            "$group": {
                "_id": {
                    "instance_type": "$instance_type",
                    "hour": "$hour",
                    "region": "$region"
                },
                "averageSpotPriceEur": {"$avg": "$spot_price_eur"}
            }
        },
        # Reshape the results
        {
            "$project": {
                "_id": 0,
                "instance_type": "$_id.instance_type",
                "hour": "$_id.hour",
                "region": "$_id.region",
                "spot_price_eur": "$averageSpotPriceEur"
            }
        }
    ]

    return list(collection.aggregate(pipeline))

def get_mean_spot_prices_azure(db, collection_name, instance_types):

    collection = db[collection_name]

    pipeline = [
        # Filter documents by instance types
        {
            "$match": {
                "instance_type": {"$in": instance_types}
            }
        },
        # Group by instance_type, hour, and region, calculating the average spot_price_eur
        {
            "$group": {
                "_id": {
                    "instance_type": "$instance_type",
                    "hour": "$hour",
                    "region": "$region"
                },
                "averageSpotPrice": {"$avg": "$spot_price"}
            }
        },
        # Reshape the results
        {
            "$project": {
                "_id": 0,
                "instance_type": "$_id.instance_type",
                "hour": "$_id.hour",
                "region": "$_id.region",
                "spot_price": {"$round": ["$averageSpotPrice", 4]}
            }
        }
    ]

    return list(collection.aggregate(pipeline))




def get_spot_price_interval(provider, instance, region, starttime, duration, konfidenzgrad, client):
    """
    Fetches the spot price data for a given instance type, region, and time range,
    then calculates the confidence interval and mean price.

    Parameters:
    - provider: str, "AWS" or "Azure"
    - instance: str, instance type
    - region: str, region name
    - starttime: float, start hour in decimal (e.g., 6.5)
    - duration: float, duration in hours (e.g., 4.5)
    - konfidenzgrad: float, confidence level for the interval
    - client: MongoDB client instance

    Returns:
    - dict containing mean price and confidence interval
    """

    # Define database and collection based on provider
    if provider == "Azure":
        db = get_database("AzureSpotPricesDB", client)
        collection_name = "SpotPrices"
        price_field = "spot_price"
    elif provider == "AWS":
        db = get_database("aws_spot_prices_db", client)
        collection_name = "aws_spot_prices"
        price_field = "spot_price_eur"
    else:
        raise ValueError("Invalid provider. Choose 'AWS' or 'Azure'.")

    collection = db[collection_name]

    # Define start and end hours
    start_hour = math.floor(starttime)
    end_hour = starttime + duration

    # Store results
    hourly_costs = []

    for hour in range(start_hour, math.ceil(end_hour)):  # Loop over full hour values
        query = {
            "instance_type": instance,
            "region": region,
            "hour": hour
        }

        cursor = collection.find(query, {price_field: 1, "_id": 0})

        # Extract prices
        prices = [doc[price_field] for doc in cursor]

        if not prices:
            konfidenz_prices = [0, 0, 0]  # Handle missing data gracefully
            mean_price = 0
        else:
            konfidenz_prices = calculate_konfidenzintervall(prices, konfidenzgrad)
            mean_price = sum(prices) / len(prices)

        # Handle fractional hour cases
        if hour == start_hour and starttime % 1 != 0:  # If start time is fractional
            fraction = 1 - (starttime % 1)
        elif hour == math.floor(end_hour) and end_hour % 1 != 0:  # If end time is fractional
            fraction = end_hour % 1
        else:
            fraction = 1  # Full hour

        # Apply fraction to mean price and confidence interval
        mean_price *= fraction
        konfidenz_prices = [p * fraction for p in konfidenz_prices]

        # Store results
        hourly_costs.append((mean_price, konfidenz_prices))

    # Sum up the hourly results
    total_mean_price = sum(hourly_cost[0] for hourly_cost in hourly_costs)
    total_confidence_interval = [sum(x) for x in zip(*[hourly_cost[1] for hourly_cost in hourly_costs])]

    return {
        "mean_price": total_mean_price,
        "confidence_interval": total_confidence_interval
    }

"""
from pymongo import MongoClient
import os
from dotenv import load_dotenv 

load_dotenv()
connection_string = os.getenv('MONGODB_URI')
connection_string2 = os.getenv('MONGODB_URI2')

client = MongoClient(connection_string)

list_test = ["FX48-12mds v2 Spot", "E2s v5 Spot"]
interval = (get_spot_price_interval("Azure", "E2s v5 Spot", "polandcentral", 0, 4, 95, client))
print(interval["confidence_interval"])
"""