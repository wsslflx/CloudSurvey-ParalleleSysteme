import boto3
import json
from dotenv import load_dotenv
import os

load_dotenv()
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
def get_product_families(service_code, region='us-east-1'):
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    pricing_client = boto3.client('pricing', region_name=region, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    paginator = pricing_client.get_paginator('get_products')
    product_families = set()

    for page in paginator.paginate(ServiceCode=service_code):
        for price_item in page['PriceList']:
            product = json.loads(price_item)
            if 'productFamily' in product['product']:
                product_families.add(product['product']['productFamily'])
                print(len(product_families))
                if len(product_families) > 5:
                    return product_families

    return product_families

if __name__ == "__main__":
    service_code = "AWSDataTransfer"  # Change to your desired service, e.g., "AmazonEFS"
    families = get_product_families(service_code)
    print(f"Product families for {service_code}:")
    for family in sorted(families):
        print(f"- {family}")
