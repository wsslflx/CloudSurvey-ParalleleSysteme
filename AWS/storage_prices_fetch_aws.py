import os
import sys
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

def fetch_pricing_data(pricing_client, service_code, region, product_family):
    paginator = pricing_client.get_paginator('get_products')
    pricing_data = []
    for page in paginator.paginate(
        ServiceCode=service_code,
        Filters=[
            {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region},
            {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': product_family}
        ]
    ):
        pricing_data.extend(page['PriceList'])
    return pricing_data

def transform_pricing_data(pricing_data, region):
    return [
        {
            'region': region,
            'price': price_item.get('terms', {}).get('OnDemand', {}),
            'fetched_at': datetime.now(timezone.utc)
        }
        for price_item in pricing_data
    ]

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

    for region in regions:
        print(f"Processing region: {region}")

        # Fetch and insert EFS prices
        print(f"Fetching EFS prices for region {region}...")
        efs_data = fetch_pricing_data(pricing_client, 'AmazonEFS', region, 'Storage')
        transformed_efs_data = transform_pricing_data(efs_data, region)
        # inserted_efs = insert_data_to_db(efs_collection, transformed_efs_data)
        # total_inserted_efs += inserted_efs
        print(len(transformed_efs_data))
        # Fetch and insert EBS prices
        print(f"Fetching EBS prices for region {region}...")
        ebs_data = fetch_pricing_data(pricing_client, 'AmazonEC2', region, 'Storage')
        transformed_ebs_data = transform_pricing_data(ebs_data, region)
        # inserted_ebs = insert_data_to_db(ebs_collection, transformed_ebs_data)
        # total_inserted_ebs += inserted_ebs

    print(efs_data)
    print(f"Inserted {total_inserted_efs} EFS price records.")
    print(f"Inserted {total_inserted_ebs} EBS price records.")

if __name__ == "__main__":
    main()
