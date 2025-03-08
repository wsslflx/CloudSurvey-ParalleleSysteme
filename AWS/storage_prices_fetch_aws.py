import os
import sys
import json
import boto3
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def get_pricing_client(aws_access_key, aws_secret_key):
    """
        Creates a boto3 pricing client configured to interact with AWS pricing API.

        Parameters:
          - aws_access_key: AWS access key ID.
          - aws_secret_key: AWS secret access key.

        Returns:
          A boto3 client instance for the AWS pricing service in the 'us-east-1' region.
        """
    return boto3.client(
        'pricing',
        region_name='us-east-1',  # Pricing API only operates in us-east-1
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

def fetch_storage_pricing_data(pricing_client, service_code, region_name, product_family):
    """
        Fetches raw storage pricing data from the AWS Pricing API for a given service, region, and product family.

        Parameters:
          - pricing_client: The AWS pricing client instance.
          - service_code: The AWS service code (e.g., 'AmazonEFS' or 'AmazonEC2').
          - region_name: The AWS region identifier (e.g., 'eu-central-1').
          - product_family: The product family to filter pricing data (e.g., 'Storage').

        Returns:
          A list of JSON strings containing raw pricing data. If the region is unmapped or no data is found, returns an empty list.
        """
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
        print(f"No {service_code} pricing data found for {region_name} ({location}).")

    return pricing_data

def fetch_transfer_pricing_data(client, from_region, to_region):
    """
        Retrieves raw data transfer pricing information from the AWS Pricing API between two specified regions.

        Parameters:
          - client: The AWS pricing client instance.
          - from_region: The source AWS region code.
          - to_region: The destination AWS region code.

        Returns:
          A list of JSON strings containing raw data transfer pricing information.
          In case of an error during the fetch, returns an empty list.
        """
    try:
        response = client.get_products(
            ServiceCode='AWSDataTransfer',
            Filters=[
                {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': 'Data Transfer'},
                {'Type': 'TERM_MATCH', 'Field': 'fromRegionCode', 'Value': from_region},
                {'Type': 'TERM_MATCH', 'Field': 'toRegionCode', 'Value': to_region}
            ],
            MaxResults=100
        )
        return response.get('PriceList', [])
    except Exception as e:
        print(f"Error fetching transfer data from {from_region} to {to_region}: {e}")
        return []

def transform_efs_data(pricing_data, region):
    """
    Transforms raw EFS pricing data into a structured list of documents suitable for MongoDB insertion.

    The transformation process:
      - Parses the JSON pricing data.
      - Filters entries where the 'storageClass' attribute is "EFS Storage".
      - Checks the price description to classify the operation as either 'read' or 'write'.

    Parameters:
      - pricing_data: A list of raw JSON strings with pricing information.
      - region: The AWS region identifier for the pricing data.

    Returns:
      A list of dictionaries with keys:
        - region, storageClass, usageType, description, unit, price, effectiveDate, sku.
    """
    transformed_data = []
    for price_item in pricing_data:
        product = json.loads(price_item)
        attributes = product['product']['attributes']
        terms = product['terms']['OnDemand']

        # Only include data where storageClass is "EFS Storage"
        if attributes.get('storageClass') == "EFS Storage":
            for term_key, term_value in terms.items():
                for price_key, price_value in term_value['priceDimensions'].items():
                    print(price_value.get('description'))
                    if "read" in price_value.get('description', 'N/A').lower():
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

def transform_ebs_data(storage_data, region):
    """
    Transforms raw EBS pricing data into a structured list of documents suitable for MongoDB insertion.

    The transformation process:
      - Parses the JSON pricing data.
      - Filters entries where the 'volumeType' attribute is "General Purpose".
      - Extracts pricing details and other attributes.

    Parameters:
      - storage_data: A list of raw JSON strings with EBS pricing information.
      - region: The AWS region identifier for the pricing data.

    Returns:
      A list of dictionaries with keys:
        - region, volumeType, usageType, description, unit, price, effectiveDate, sku.
    """
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

def transform_transfer_data(transfer_data, from_region, to_region):
    """
        Transforms raw data transfer pricing data into a structured list of documents suitable for MongoDB insertion.

        The transformation process:
          - Parses the JSON pricing data.
          - Iterates over the on-demand terms to extract pricing dimensions.

        Parameters:
          - transfer_data: A list of raw JSON strings with data transfer pricing information.
          - from_region: The source AWS region code.
          - to_region: The destination AWS region code.

        Returns:
          A list of dictionaries with keys:
            - fromRegion, toRegion, description, unit, price, effectiveDate, sku.
    """
    transformed_data = []
    for raw_item in transfer_data:
        product = json.loads(raw_item)
        terms = product.get('terms', {}).get('OnDemand', {})

        # Add any checks or filters you need here. For now, we store everything.
        for term_key, term_value in terms.items():
            for price_key, price_value in term_value['priceDimensions'].items():
                transformed_data.append({
                    'fromRegion': from_region,
                    'toRegion': to_region,
                    'description': price_value.get('description', ''),
                    'unit': price_value.get('unit', 'N/A'),
                    'price': float(price_value['pricePerUnit'].get('USD', '0.0')),
                    'effectiveDate': term_value.get('effectiveDate', 'N/A'),
                    'sku': product['product'].get('sku', 'N/A'),
                })
    return transformed_data

def insert_data_to_db(collection, data):
    """
        Inserts a list of documents into a MongoDB collection using the insert_many method.

        Parameters:
          - collection: The MongoDB collection where documents should be inserted.
          - data: A list of dictionaries representing the documents to be inserted.

        Returns:
          The number of documents successfully inserted.
        """
    if data:
        collection.insert_many(data)
        return len(data)
    return 0

def main():
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    mongo_uri = os.getenv('MONGODB_URI2')

    if not aws_access_key or not aws_secret_key or not mongo_uri:
        print(aws_access_key, aws_secret_key, mongo_uri)
        print("Missing environment variables. Make sure AWS and MongoDB credentials are set.")
        sys.exit(1)

    mongo_client = MongoClient(mongo_uri)
    db = mongo_client['aws_storage_pricing_db']

    # Collections
    efs_collection = db['aws_efs_prices']
    ebs_collection = db['aws_ebs_prices']
    transfer_collection = db['aws_data_transfer_prices']

    # Hardcoded list of European regions
    regions = [
        'eu-central-1', 'eu-west-1', 'eu-west-2',
        'eu-west-3', 'eu-north-1',
    ]

    pricing_client = get_pricing_client(aws_access_key, aws_secret_key)

    # Prepare accumulators
    all_efs_docs = []
    all_ebs_docs = []
    all_transfer_docs = []

    # 1. Gather EFS & EBS data for all regions
    for region in regions:
        print(f"Processing storage data for region: {region}")

        # Fetch & transform EFS
        efs_data = fetch_storage_pricing_data(pricing_client, 'AmazonEFS', region, 'Storage')
        transformed_efs_data = transform_efs_data(efs_data, region)
        all_efs_docs.extendx(transformed_efs_data)

        # Fetch & transform EBS
        ebs_data = fetch_storage_pricing_data(pricing_client, 'AmazonEC2', region, 'Storage')
        transformed_ebs_data = transform_ebs_data(ebs_data, region)
        all_ebs_docs.extend(transformed_ebs_data)

    # 2. Gather Data Transfer pricing among all region pairs
    for from_region in regions:
        for to_region in regions:
            if to_region == from_region:
                continue
            transfer_data = fetch_transfer_pricing_data(pricing_client, from_region, to_region)
            if not transfer_data:
                print(f"No transfer pricing data found from {from_region} to {to_region}.")
            transformed = transform_transfer_data(transfer_data, from_region, to_region)
            all_transfer_docs.extend(transformed)

    """
    # 3. Batch insert for each collection
    inserted_efs = insert_data_to_db(efs_collection, all_efs_docs)
    inserted_ebs = insert_data_to_db(ebs_collection, all_ebs_docs)
    inserted_transfer = insert_data_to_db(transfer_collection, all_transfer_docs)

    print(f"\n--- Insert Summary ---")
    print(f"EFS price records inserted: {inserted_efs}")
    print(f"EBS price records inserted: {inserted_ebs}")
    print(f"Data Transfer records inserted: {inserted_transfer}")
    print("Done.")
    """
if __name__ == "__main__":
    main()
