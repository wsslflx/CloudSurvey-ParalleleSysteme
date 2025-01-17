import requests
import time
import pymongo
from pymongo import MongoClient
from datetime import datetime, timezone
import logging
import os
from dotenv import load_dotenv

load_dotenv()
CONNECTION_STRING = os.getenv('MONGODB_URI')
RETAIL_PRICES_API_ENDPOINT = "https://prices.azure.com/api/retail/prices"
DATABASE_NAME = 'azure_spot_prices.db'

logging.basicConfig(level=logging.INFO)

# Connect to MongoDB
def connect_to_mongodb(connection_string):
    try:
        client = MongoClient(connection_string)
        logging.info("Successfully connected to MongoDB Atlas.")
        return client
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        return None

def fetch_retail_prices(params):
    prices = []
    url = RETAIL_PRICES_API_ENDPOINT
    skip = 0
    max_records = 1000  # Maximum number of records per request
    while True:
        params['$skip'] = skip
        logging.info(f"Fetching data with params: {params}")
        logging.info(f"Fetching data from URL: {url}")
        try:
            response = requests.get(url, params=params)
        except Exception as e:
            logging.error(f"Exception during request: {e}")
            break

        if response.status_code != 200:
            logging.error(f"Failed to fetch data: {response.status_code} - {response.text}")
            # Break the loop if a 400 Bad Request error occurs
            if response.status_code == 400:
                break
            else:
                continue  # Optionally retry or handle other status codes

        data = response.json()
        items = data.get('Items', [])
        if not items:
            logging.info("No more items to fetch.")
            break  # Exit the loop if no items are returned

        prices.extend(items)
        logging.info(f"Fetched {len(items)} items.")

        # Increment skip for the next iteration
        skip += max_records
        time.sleep(1)  # Respect API rate limits

    return prices

def insert_spot_price(client, database_name, collection_name, spot_price_data):
    try:
        db = client[database_name]
        collection = db[collection_name]
        result = collection.insert_one(spot_price_data)
        logging.info(f"Inserted document with ID: {result.inserted_id}")
    except pymongo.errors.DuplicateKeyError:
        logging.warning(f"Duplicate document: {spot_price_data}")
    except Exception as e:
        logging.error(f"Error inserting document: {e}")

def insert_spot_prices_bulk(client, database_name, collection_name, spot_prices):
    if not spot_prices:
        logging.info("No data to insert.")
        return

    try:
        db = client[database_name]
        collection = db[collection_name]
        result = collection.insert_many(spot_prices, ordered=False)
        logging.info(f"Inserted {len(result.inserted_ids)} documents.")
    except pymongo.errors.BulkWriteError as bwe:
        logging.warning(f"Bulk write error: {bwe.details}")
    except Exception as e:
        logging.error(f"Error during bulk insert: {e}")


def main():
    client = connect_to_mongodb(CONNECTION_STRING)
    if not client:
        return

    # Define initial parameters for the API request
    params = {
        '$top': 1000,
        'currencyCode': 'EUR',
        '$filter': (
            "serviceFamily eq 'Compute' and "
            "(armRegionName eq 'westeurope' or "
            "armRegionName eq 'germanywestcentral' or "
            "armRegionName eq 'northeurope' or "
            "armRegionName eq 'swedencentral' or "
            "armRegionName eq 'uksouth' or "
            "armRegionName eq 'francecentral' or "
            "armRegionName eq 'italynorth' or "
            "armRegionName eq 'norwayeast' or "
            "armRegionName eq 'polandcentral' or "
            "armRegionName eq 'spaincentral' or "
            "armRegionName eq 'switzerlandnorth' or "
            "armRegionName eq 'europe' or "
            "armRegionName eq 'francesouth' or "
            "armRegionName eq 'norwaywest' or "
            "armRegionName eq 'switzerlandwest' or "
            "armRegionName eq 'ukwest' or "
            "armRegionName eq 'germanynorth') and "
            "contains(meterName, 'Spot')"
        )
    }

    # Fetch retail prices from Azure
    all_prices = fetch_retail_prices(params)
    logging.info(f"Total prices fetched: {len(all_prices)}")

    batch_size = 1000  # Adjust batch size based on your needs and MongoDB configuration
    batch = []
    for item in all_prices:
        current_ts = datetime.now(timezone.utc)
        transformed_data = {
            'region': item.get('armRegionName', ''),
            'instance_type': item.get('meterName', ''),
            'spot_price': item.get('retailPrice', 0),
            'timestamp': current_ts,
            'hour': current_ts.hour
        }
        batch.append(transformed_data)

        # Insert batch into MongoDB
        if len(batch) >= batch_size:
            insert_spot_prices_bulk(client, "AzureSpotPricesDB", "SpotPrices", batch)
            batch.clear()  # Clear the batch after insertion

    # Insert any remaining data
    if batch:
        insert_spot_prices_bulk(client, "AzureSpotPricesDB", "SpotPrices", batch)

    client.close()
    logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    main()
