from CloudSurvey_Package.math_operations import calculate_konfidenzintervall

def get_database(db_name, client):
    return client[db_name]

def fetch_all_hours_prices(db, collection_name, instance_type, region):
    """
        Retrieves all hourly price documents for a specific instance type and region from the given database collection.

        Where:
          - db: The database containing the collection.
          - collection_name: The name of the collection where spot prices are stored.
          - instance_type: The instance type for which prices are retrieved.
          - region: The region from which prices should be fetched.

        Returns:
          A list of all documents, sorted by "timestamp".
    """
    collection = db[collection_name]
    query = {
        "instance_type": instance_type,
        "region": region
    }
    cursor = collection.find(query).sort("timestamp", 1)

    return list(cursor)

def fetch_instance_prices(db_name, collection_name,
                          instance_type, hour, region, client):
    """
        Retrieves spot price documents for a specific instance type, hour, and region from the given database.

        Where:
          - db_name: The name of the database containing the collection.
          - collection_name: The name of the collection where spot prices are stored.
          - instance_type: The instance type for which prices are retrieved.
          - hour: The hour for which prices should be fetched.
          - region: The region from which prices should be fetched.
          - client: The MongoDB client used for the connection.

        Returns:
          A list of documents sorted by "timestamp" containing the spot prices.
    """
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
    """
        Retrieves spot prices for each hour of the day for a given provider, instance type, and region.
        Computes the confidence interval for the prices per hour using the provided confidence level.

        Where:
          - provider: The provider, e.g., "Azure" or "AWS".
          - instance: The instance type for which prices are retrieved.
          - region: The region from which prices should be fetched.
          - konfidenzgrad: The confidence level for calculating the confidence interval.
          - client: The MongoDB client used for the connection.

        Returns:
          A list of confidence intervals (as lists) for each hour (0 to 23).
          If data is missing, [0, 0, 0] is returned.
    """
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
    """
        Retrieves storage prices for a given provider and SKU name from the corresponding database.
        For Azure, matches by skuName exactly (case-insensitive) and filters by "1/Month" unit.
        For AWS, performs a case-insensitive match on the description.

        Where:
          - provider: The provider, e.g., "Azure" or "AWS".
          - skuName: The SKU name for which prices should be fetched.
          - client: The MongoDB client used for the connection.

        Returns:
          A list of dictionaries containing region, SKU/description, and price.
          Returns an empty list in case of errors or missing results.
    """
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
    """
        Retrieves transfer prices for AWS from the corresponding database for a specific region-to-region transfer.

        Where:
          - provider: The provider (only "AWS" is supported).
          - fromRegion: The source region of the transfer.
          - toRegion: The target region of the transfer.
          - client: The MongoDB client used for the connection.

        Returns:
          A list of dictionaries containing the transfer price.
          Returns an empty list in case of errors or missing results.
    """
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
    """
        Retrieves the mean spot price per hour for a list of instance types for a given provider.
        Delegates the calculation to provider-specific functions based on the provider value.

        Where:
          - client: The MongoDB client used for the connection.
          - instance_types: A list of instance types for which average prices should be calculated.
          - provider: The provider, e.g., "AWS" or "Azure".

        Returns:
          A list of documents containing the average spot price per hour.
    """
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
    """
        Calculates the average AWS spot prices grouped by instance type, hour, and region.
        Uses an aggregation pipeline to compute the average spot price in Euro.

        Where:
          - db: The database containing AWS spot prices.
          - collection_name: The name of the collection storing AWS spot prices.
          - instance_types: A list of instance types to be included in the aggregation.

        Returns:
          A list of documents containing instance type, hour, region, and the computed average price.
    """
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
    """
        Calculates the average Azure spot prices grouped by instance type, hour, and region.
        Uses an aggregation pipeline with rounding applied to the computed average price.

        Where:
          - db: The database containing Azure spot prices.
          - collection_name: The name of the collection where Azure spot prices are stored.
          - instance_types: A list of instance types to be included in the aggregation.

        Returns:
          A list of documents containing the instance type, hour, region, and the rounded average price.
    """
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



"""
from pymongo import MongoClient
import os
from dotenv import load_dotenv 

load_dotenv()
connection_string = os.getenv('MONGODB_URI')
connection_string2 = os.getenv('MONGODB_URI2')

client = MongoClient(connection_string)

list_test = ["FX48-12mds v2 Spot", "E2s v5 Spot"]
results = (get_mean_spot_price(client, list_test, "Azure"))
for result in results:
    print(result)
"""