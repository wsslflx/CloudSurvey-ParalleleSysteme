import requests
import json
import sqlite3
import time
from datetime import datetime
import logging

RETAIL_PRICES_API_ENDPOINT = "https://prices.azure.com/api/retail/prices"
DATABASE_NAME = 'azure_spot_prices.db'

base_url = "://prices.azure.com/api/retail/prices?currencyCode='EUR'&"

# Initialize SQLite database

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

def main():
    # Initialize database
    conn, cursor = init_db(DATABASE_NAME)
    logging.info("Initialized SQLite database.")

    # Define initial parameters for the API request
    params = {
        '$top': 100,  # Number of items per page
        # Add filters as needed. Example:
        # '$filter': "serviceFamily eq 'Compute' and armRegionName eq 'eastus'"
    }

    # Fetch retail prices
    all_prices = fetch_retail_prices(params)
    logging.info(f"Total prices fetched: {len(all_prices)}")

    # Filter spot prices
    spot_prices = filter_spot_prices(all_prices)
    logging.info(f"Total spot prices after filtering: {len(spot_prices)}")

    if not spot_prices:
        logging.warning("No spot prices found with the current filtering criteria.")
    else:
        # Store in the database
        store_prices(cursor, conn, spot_prices)

    # Close database connection
    conn.close()
    logging.info("Closed database connection.")

if __name__ == "__main__":
    main()
