import os
import sys
import boto3
from pymongo import MongoClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

def main():
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')
    mongo_uri = os.getenv('MONGODB_URI')

    if not aws_access_key or not aws_secret_key or not mongo_uri:
        print("Missing environment variables. Make sure AWS and MongoDB credentials are set.")
        sys.exit(1)

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client['aws_spot_prices_db']
    collection = db['aws_spot_prices']

    # Initialize a generic EC2 client (just to test connectivity, optional)
    ec2 = boto3.client(
        'ec2',
        region_name=aws_region,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    # Hardcode the list of European regions
    regions = [
        'eu-central-1', 'eu-west-1', 'eu-west-2',
        'eu-west-3', 'eu-north-1',
    ]

    inserted_count_total = 0

    start_time = datetime.now(timezone.utc) - timedelta(hours=1)

    for region in regions:
        print(f"Processing region: {region}")

        # Create a region-specific EC2 client
        ec2_region = boto3.client(
            'ec2',
            region_name=region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

        # Get all instance types for this region
        all_instance_types = []
        itypes_paginator = ec2_region.get_paginator('describe_instance_types')
        for page in itypes_paginator.paginate():
            for itype in page['InstanceTypes']:
                all_instance_types.append(itype['InstanceType'])

        # Deduplicate instance types
        all_instance_types = list(set(all_instance_types))
        print(f"Found {len(all_instance_types)} instance types in region {region}")

        # Fetch spot prices for these instance types
        batch_size = 100
        inserted_count_region = 0

        for i in range(0, len(all_instance_types), batch_size):
            batch = all_instance_types[i : i + batch_size]

            # Paginate through spot price history
            paginator = ec2_region.get_paginator('describe_spot_price_history')
            for page in paginator.paginate(
                InstanceTypes=batch,
                StartTime=start_time,
                ProductDescriptions=['Linux/UNIX'],
                PaginationConfig={'PageSize': 1000}
            ):
                spot_prices = page.get('SpotPriceHistory', [])
                # Transform the data
                transformed_data = [
                    {
                        'region': region,
                        'instance_type': entry['InstanceType'],
                        'spot_price_eur': float(entry['SpotPrice']),
                        'timestamp': entry['Timestamp'],
                        'hour': entry['Timestamp'].hour, # later easier to compare to other days
                        'fetched_at': datetime.now(timezone.utc)
                    }
                    for entry in spot_prices
                ]

                if transformed_data:
                    collection.insert_many(transformed_data)
                    inserted_count_region += len(transformed_data)

        print(f"Inserted {inserted_count_region} spot price records for region {region}.")
        inserted_count_total += inserted_count_region

    print(f"Inserted a total of {inserted_count_total} spot price records across all specified regions.")

if __name__ == "__main__":
    main()
