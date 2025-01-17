import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import pymongo
import requests
from pymongo import MongoClient
import os

load_dotenv()
# ------------------------
#       CONFIG / CONSTANTS
# ------------------------
AZURE_PRICES_API = "https://prices.azure.com/api/retail/prices"
EUROPE_REGIONS = [
    "westeurope",
    "germanywestcentral",
    "northeurope",
    "swedencentral",
    "uksouth",
    "francecentral",
    "italynorth",
    "norwayeast",
    "polandcentral",
    "spaincentral",
    "switzerlandnorth",
    "europe",
    "francesouth",
    "norwaywest",
    "switzerlandwest",
    "ukwest",
    "germanynorth",
]



# ------------------------
#       HELPER FUNCTIONS
# ------------------------
def connect_to_mongodb(connection_string: str) -> MongoClient | None:
    """
    Connect to MongoDB using the provided connection string.
    Returns a MongoClient instance if successful; otherwise returns None.
    """
    try:
        client = MongoClient(connection_string)
        logging.info("Successfully connected to MongoDB.")
        return client
    except Exception as exc:
        logging.error(f"Error connecting to MongoDB: {exc}")
        return None


def fetch_all_pages(api_url: str, params: dict) -> list[dict]:
    """
    Fetch all pages from a paginated Azure Retail Prices API endpoint using
    the given query parameters. Returns a list of all items from all pages.
    """
    all_items = []
    next_page = api_url

    while next_page:
        response = requests.get(next_page, params=params)
        if response.status_code != 200:
            logging.error(f"Error fetching data: {response.status_code}")
            break

        data = response.json()
        items = data.get("Items", [])
        all_items.extend(items)

        # The API may provide a 'nextPageLink' with query params embedded, so
        # after the first request, we typically don't need to pass params again.
        next_page = data.get("nextPageLink")

    return all_items


# ------------------------
#       PRICE FETCHERS
# ------------------------
def fetch_storage_prices(api_url: str = AZURE_PRICES_API) -> list[dict]:
    """
    Fetch all Azure Storage (Managed Disks + Files) prices in EUR for relevant
    European regions. Returns a filtered list of storage price items.
    """
    region_filter = " or ".join([f"armRegionName eq '{r}'" for r in EUROPE_REGIONS])
    # region_filter will look like:
    #   armRegionName eq 'westeurope' or armRegionName eq 'germanywestcentral' or ...
    storage_filter = f"serviceFamily eq 'Storage' and ({region_filter})"

    params = {
        "currencyCode": "EUR",
        "pageSize": 1000,
        "$filter": storage_filter,
    }

    all_items = fetch_all_pages(api_url, params)

    # Filter for Managed Disks or Files that are not reserved
    filtered = [
        item
        for item in all_items
        if (
            ("Files" in item.get("productName", "") or "Managed Disks" in item.get("productName", ""))
            and not item.get("reservationTerm")
        )
    ]

    logging.info(f"Fetched {len(filtered)} storage price items.")
    return filtered


def fetch_transfer_prices(api_url: str = AZURE_PRICES_API) -> list[dict]:
    """
    Fetch all Azure Data Transfer (Inter-Region) prices in EUR for relevant
    European regions. Returns a filtered list of transfer price items.
    """
    region_filter = " or ".join([f"armRegionName eq '{r}'" for r in EUROPE_REGIONS])
    # region_filter will look like:
    #   armRegionName eq 'westeurope' or armRegionName eq 'germanywestcentral' or ...
    transfer_filter = f"serviceName eq 'Bandwidth' and ({region_filter})"
    params = {
        "currencyCode": "EUR",
        "pageSize": 1000,
        "$filter": transfer_filter,
    }

    all_items = fetch_all_pages(api_url, params)

    # Filter for Inter-Region Data Transfer but exclude China / Internet
    filtered = [
        item
        for item in all_items
        if (
            "Data Transfer" in item.get("meterName", "")
            and "China" not in item.get("meterName", "")
            and "Internet" not in item.get("productName", "")
            and "Inter-Region Data Transfer" in item.get("meterName", "")
        )
    ]

    logging.info(f"Fetched {len(filtered)} transfer price items.")
    return filtered


# ------------------------
#       DATABASE INSERT
# ------------------------
def insert_storage_prices_bulk(
    client: MongoClient,
    database_name: str,
    collection_name: str,
    spot_prices: list[dict],
    batch_size: int = 1000
) -> None:
    """
    Insert spot pricing data into MongoDB in batches. Transforms the items
    before insertion. Logs the outcome of the operation.
    """
    if not spot_prices:
        logging.info("No data to insert.")
        return

    db = client[database_name]
    collection = db[collection_name]

    # Transform the data in memory
    current_ts = datetime.now(timezone.utc)
    transformed_documents = []
    for item in spot_prices:
        transformed_documents.append({
            "region": item.get("armRegionName", ""),
            "storage_type": item.get("productName", ""),
            "price": item.get("unitPrice", 0.0),
            "unitOfMeasure": item.get("unitOfMeasure", ""),
            "skuName": item.get("skuName", ""),
            "timestamp": current_ts,
        })

    # Insert in batches
    start_index = 0
    total_inserted = 0

    while start_index < len(transformed_documents):
        end_index = min(start_index + batch_size, len(transformed_documents))
        batch = transformed_documents[start_index:end_index]

        try:
            result = collection.insert_many(batch, ordered=False)
            total_inserted += len(result.inserted_ids)
        except pymongo.errors.BulkWriteError as bwe:
            logging.warning(f"Bulk write error: {bwe.details}")
        except Exception as exc:
            logging.error(f"Error during bulk insert: {exc}")

        start_index += batch_size

    logging.info(f"Inserted {total_inserted} documents into {database_name}.{collection_name}.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # 1) Connect to Mongo
    MongoDB_URL = os.getenv("MONGODB_URI2")
    client = connect_to_mongodb(MongoDB_URL)

    if client:
        # 2) Fetch data
        storage_data = fetch_storage_prices()
        transfer_data = fetch_transfer_prices()

        # 3) Insert data
        """
        Datenbank name muss geändeert werden
        """
        db_name = "azure_storage_pricing_db"
        insert_storage_prices_bulk(client, db_name, "StoragePrices", storage_data)
        insert_storage_prices_bulk(client, db_name, "TransferPrices", transfer_data)

        # Close the connection if desired
        client.close()
