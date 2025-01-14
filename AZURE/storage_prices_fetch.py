import requests
import json
from datetime import datetime, timezone
import logging
import pymongo
from pymongo import MongoClient

# Azure Retail Prices API URL
api_url = "https://prices.azure.com/api/retail/prices"

# Query parameters for Managed Disks
params = {
    "currencyCode": "EUR",  # Change to your preferred currency (e.g., EUR)
    "pageSize": 1000,       # Number of results per page
    '$filter': (
                "serviceFamily eq 'Storage' and "
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
                "armRegionName eq 'germanynorth')"
            )
}
all_prices = []
while api_url:
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        data = response.json()
        all_prices.extend(data['Items'])
        api_url = data.get('nextPageLink')  # Get next page link
    else:
        print(f"Error fetching data: {response.status_code}")
        break

all_storage_prices = []
# Parse and filter the data
if response.status_code == 200:
    data = response.json()
    managed_disk_prices = [
        item for item in data['Items'] if "Managed Disks" in item['productName']
    ]
    files_prices = [
        # Filter for items with "Files" in productName and exclude those with a filled "reservationTerm"
        item for item in data['Items']
        if "Files" in item['productName'] and ('reservationTerm' not in item or not item['reservationTerm'])
    ]
    all_storage_prices = [
        item for item in data['Items']
        if ("Files" in item['productName'] or "Managed Disks" in item['productName']) and  ('reservationTerm' not in item or not item['reservationTerm'])
    ]
    # print(json.dumps(managed_disk_prices, indent=4))
else:
    print(f"Error fetching data: {response.status_code}")

print(json.dumps(all_storage_prices, indent=4))
print(len(all_storage_prices))


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

batch_size = 1000  # Adjust batch size based on your needs and MongoDB configuration
    batch = []
    for item in all_prices:
        current_ts = datetime.now(timezone.utc)
        transformed_data = {
            'region': item.get('armRegionName', ''),
            'storage_type': item.get('productName', ''),
            'price': item.get('unitPrice', 0),
            'unitOfMeasure': item.get('unitOfMeasure', ''),
            'skuName': item.get('skuName', ''),
            'timestamp': current_ts
        }
        batch.append(transformed_data)

        # Insert batch into MongoDB
        if len(batch) >= batch_size:
            insert_spot_prices_bulk(client, "AzureSpotPricesDB", "SpotPrices", batch)
            batch.clear()  # Clear the batch after insertion

    # Insert any remaining data
    if batch:
        insert_spot_prices_bulk(client, "AzureSpotPricesDB", "SpotPrices", batch)