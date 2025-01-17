import os
import sys
import json
import boto3
from pymongo import MongoClient
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

def get_pricing_client(aws_access_key, aws_secret_key):
    return boto3.client(
        'pricing',
        region_name='us-east-1',  # Pricing API only operates in us-east-1
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

def fetch_storage_pricing_data(pricing_client, service_code, region_name, product_family):
    paginator = pricing_client.get_paginator('get_products')
    pricing_data = []

    location_map = {
        'eu-central-1': 'EU (Frankfurt)',
        'eu-west-1': 'EU (Ireland)',
        'eu-west-2': 'EU (London)',
        'eu-west-3': 'EU (Paris)',
        'eu-north-1': 'EU (Stockholm)',
    }

    location = location_map.get(region_name, None)
    if not location:
        print(f"Region {region_name} is not mapped to a valid location.")
        return []

    for page in paginator.paginate(
        ServiceCode=service_code,
        Filters=[
            {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': location},
            {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': product_family}
        ]
    ):
        pricing_data.extend(page['PriceList'])

    if not pricing_data:
        print(f"No pricing data found for {service_code} in {region_name} ({location}).")

    return pricing_data

def fetch_transfer_pricing_data(client, region, regions):
    try:
        for to_region in regions:
            if to_region != region:
                response = client.get_products(
                    ServiceCode='AWSDataTransfer',
                    Filters=[
                        {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': 'Data Transfer'},
                        {'Type': 'TERM_MATCH', 'Field': 'fromRegionCode', 'Value': region},
                        {'Type': 'TERM_MATCH', 'Field': 'toRegionCode', 'Value': to_region}
                    ],
                    MaxResults=100
                )
                if not response['PriceList']:
                    print("No products found for the given filters.")
                else:
                    for price in response['PriceList']:
                        data = json.loads(price)
                        print(json.dumps(data, indent=4))
    except Exception as e:
        print(f"Error: {e}")


def transform_pricing_data(pricing_data, region):
    transformed_data = []
    for price_item in pricing_data:
        product = json.loads(price_item)
        attributes = product['product']['attributes']
        terms = product['terms']['OnDemand']

        # Only include data where storageClass is "EFS Storage"
        if attributes.get('storageClass') == "EFS Storage":
            for term_key, term_value in terms.items():
                for price_key, price_value in term_value['priceDimensions'].items():
                    if "read" in price_value.get('description', 'N/A'):
                        description = "read"
                    else:
                        description = "write"
                    transformed_data.append({
                        'region': region,
                        'storageClass': attributes.get('storageClass', 'N/A'),
                        'usageType': attributes.get('usagetype', 'N/A'),
                        'description': description,
                        'unit': price_value.get('unit', 'N/A'),
                        'price': float(price_value['pricePerUnit'].get('USD', '0.0')),
                        'effectiveDate': term_value.get('effectiveDate', 'N/A'),
                        'sku': product['product'].get('sku', 'N/A'),
                    })
    return transformed_data


def transform_storage_data(storage_data, region):
    transformed_data = []
    for storage_item in storage_data:
        product = json.loads(storage_item)
        attributes = product['product']['attributes']
        terms = product['terms']['OnDemand']

        # Only include data where volumeType is "General Purpose"
        if attributes.get('volumeType') == "General Purpose":
            for term_key, term_value in terms.items():
                for price_key, price_value in term_value['priceDimensions'].items():
                    transformed_data.append({
                        'region': region,
                        'volumeType': attributes.get('volumeType', 'N/A'),
                        'usageType': attributes.get('usagetype', 'N/A'),
                        'description': price_value.get('description', ''),
                        'unit': price_value.get('unit', 'N/A'),
                        'price': float(price_value['pricePerUnit'].get('USD', '0.0')),
                        'effectiveDate': term_value.get('effectiveDate', 'N/A'),
                        'sku': product['product'].get('sku', 'N/A'),
                    })
    return transformed_data



def insert_data_to_db(collection, data):
    if data:
        collection.insert_many(data)
        return len(data)
    return 0

def main():
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    mongo_uri = os.getenv('MONGODB_URI')

    if not aws_access_key or not aws_secret_key or not mongo_uri:
        print("Missing environment variables. Make sure AWS and MongoDB credentials are set.")
        sys.exit(1)

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client['aws_pricing_db']

    # Collections for EFS and EBS prices
    efs_collection = db['aws_efs_prices']
    ebs_collection = db['aws_ebs_prices']

    # Hardcoded list of European regions
    regions = [
        'eu-central-1', 'eu-west-1', 'eu-west-2',
        'eu-west-3', 'eu-north-1',
    ]

    pricing_client = get_pricing_client(aws_access_key, aws_secret_key)

    total_inserted_efs = 0
    total_inserted_ebs = 0
    total_inserted_transfer = 0

    for region in regions:
        fetch_transfer_pricing_data(pricing_client, region, regions)


    for region in regions:
        print(f"Processing region: {region}")

        # Fetch and insert EFS prices
        print(f"Fetching EFS prices for region {region}...")
        efs_data = fetch_storage_pricing_data(pricing_client, 'AmazonEFS', region, 'Storage')
        transformed_efs_data = transform_pricing_data(efs_data, region)
        inserted_efs = insert_data_to_db(efs_collection, transformed_efs_data)
        total_inserted_efs += inserted_efs

        # Fetch and insert EBS prices
        print(f"Fetching EBS prices for region {region}...")
        ebs_data = fetch_storage_pricing_data(pricing_client, 'AmazonEC2', region, 'Storage')
        transformed_ebs_data = transform_pricing_data(ebs_data, region)
        inserted_ebs = insert_data_to_db(ebs_collection, transformed_ebs_data)
        total_inserted_ebs += inserted_ebs

        #Fetch transfer prices
        print(f"Fetching transfer prices for region {region}...")
        transfer_data= fetch_transfer_pricing_data(pricing_client, region, regions)
        transformed_transfer_data = transform_storage_data(transfer_data, region)
        inserted_transfers = insert_data_to_db(ebs_collection, transformed_transfer_data)
        total_inserted_transfer += inserted_transfers

    print(f"Inserted {total_inserted_efs} EFS price records.")
    print(f"Inserted {total_inserted_ebs} EBS price records.")

if __name__ == "__main__":
    main()
