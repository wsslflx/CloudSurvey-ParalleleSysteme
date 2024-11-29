import requests
import time
import pymongo
from pymongo import MongoClient
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

load_dotenv()
CONNECTION_STRING = os.getenv('MONGODB_URI')
RETAIL_PRICES_API_ENDPOINT = "https://prices.azure.com/api/retail/prices"
DATABASE_NAME = 'azure_spot_prices.db'

base_url = "://prices.azure.com/api/retail/prices?currencyCode='EUR'&"
logging.basicConfig(level=logging.INFO)

#connect to MongoDb
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
    while True:
        logging.info(f"Fetching data with params: {params}")
        response = requests.get(RETAIL_PRICES_API_ENDPOINT, params=params)
        if response.status_code != 200:
            logging.error(f"Failed to fetch data: {response.status_code} - {response.text}")
            break

        data = response.json()
        prices.extend(data.get('Items', []))
        logging.info(f"Fetched {len(data.get('Items', []))} items.")

        if 'NextPageLink' in data and data['NextPageLink']:
            next_page = data['NextPageLink']
            # Extract query parameters from the next page link
            params = {}
            if '?' in next_page:
                query_string = next_page.split('?')[1]
                for param in query_string.split('&'):
                    key, value = param.split('=')
                    params[key] = value
            else:
                break
            # To respect API rate limits
            time.sleep(1)
        else:
            break
    return prices


def store_prices(cursor, conn, prices):
    retrieved_at = datetime.utcnow().isoformat()
    for item in prices:
        unit_price = item.get('unitPrice', 0.0)
        currency_code = item.get('currencyCode', '')
        region = item.get('armRegionName', '')
        service_family = item.get('serviceFamily', '')
        service_id = item.get('serviceId', '')
        service_name = item.get('serviceName', '')
        product_id = item.get('productId', '')
        product_name = item.get('productName', '')
        sku_id = item.get('skuId', '')
        sku_name = item.get('skuName', '')
        effective_start_date = item.get('effectiveStartDate', '')

        cursor.execute('''
            INSERT INTO azure_spot_prices (
                unit_price, currency_code, region, service_family, service_id,
                service_name, product_id, product_name, sku_id, sku_name,
                effective_start_date, retrieved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            unit_price, currency_code, region, service_family, service_id,
            service_name, product_id, product_name, sku_id, sku_name,
            effective_start_date, retrieved_at
        ))
    conn.commit()
    logging.info(f"Stored {len(prices)} spot price entries in the database.")


def insert_spot_price(client, database_name, collection_name, spot_price_data):
    try:
        db = client[database_name]
        collection = db[collection_name]
        # Add a timestamp
        spot_price_data['retrieved_at'] = datetime.utcnow()
        result = collection.insert_one(spot_price_data)
        logging.info(f"Inserted document with ID: {result.inserted_id}")
    except pymongo.errors.DuplicateKeyError:
        logging.warning(f"Duplicate document: {spot_price_data.get('_id')}")
    except Exception as e:
        logging.error(f"Error inserting document: {e}")


def main():

    client = connect_to_mongodb(CONNECTION_STRING)
    if not client:
        return

    # Define initial parameters for the API request
    params = {
        '$top': 100,  # Number of items per page
        # Add filters as needed. Example:
        # '$filter': "serviceFamily eq 'Compute' and armRegionName eq 'eastus'"
    }

    # Fetch retail prices
    all_prices = fetch_retail_prices(params)
    logging.info(f"Total prices fetched: {len(all_prices)}")

    for item in all_prices:
        insert_spot_price(client, "AzureSpotPricesDB", "SpotPrices", item)

    client.close()
    logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    main()
