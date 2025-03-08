import boto3
import csv
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Retrieve credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

def get_all_regions_instance_info():
    """
        Fetches all EC2 instance type information across all available AWS regions.

        For each region obtained from the EC2 client, this method:
          - Creates a regional EC2 client.
          - Uses a paginator to retrieve all instance types.
          - Extracts key information including:
              - InstanceType: The name of the instance type.
              - vCPUs: Number of virtual CPUs provided by the instance.
              - MemoryGiB: Amount of memory in GiB.
              - Storage: Total instance storage (if available, otherwise 'N/A').
              - BaseClockSpeedGhz: Sustained processor clock speed (if available, otherwise 'N/A').
              - Region: The AWS region from which the data was fetched.

        Returns:
          A list of dictionaries, each representing an instance type with the attributes mentioned above.
    """
    ec2_client = boto3.client('ec2', region_name='eu-central-1', aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key)
    regions = [region['RegionName'] for region in ec2_client.describe_regions()['Regions']]
    all_instance_types = []

    for region in regions:
        print(f"Fetching data for region: {region}")
        regional_client = boto3.client('ec2', region_name=region)
        paginator = regional_client.get_paginator('describe_instance_types')

        for page in paginator.paginate():
            for instance_type in page['InstanceTypes']:
                instance_info = {
                    'InstanceType': instance_type['InstanceType'],
                    'vCPUs': instance_type['VCpuInfo']['DefaultVCpus'],
                    'MemoryGiB': instance_type['MemoryInfo']['SizeInMiB'] / 1024,
                    'Storage': instance_type.get('InstanceStorageInfo', {}).get('TotalSizeInGB', 'N/A'),
                    'BaseClockSpeedGhz': instance_type.get('ProcessorInfo', {}).get('SustainedClockSpeedInGhz', 'N/A'),
                    'Region': region,
                }
                all_instance_types.append(instance_info)

    return all_instance_types


def save_to_csv(data, filename='all_instance_types.csv'):
    """
        Saves instance type data to a CSV file.

        This method writes the provided data (a list of dictionaries) to a CSV file with the following structure:
          - InstanceType: The instance type name.
          - vCPUs: The number of virtual CPUs.
          - MemoryGiB: The memory size in GiB.
          - Storage: The total storage size in GB or 'N/A' if not available.
          - BaseClockSpeedGhz: The base clock speed in GHz or 'N/A' if not provided.
          - Region: The AWS region from which the data was fetched.

        The CSV file is created (or overwritten) with a header row followed by one row per instance type.
        After writing, a confirmation message is printed to the console.

        Parameters:
          data (list): A list of dictionaries, each containing instance type information.
          filename (str): The name of the CSV file to which the data will be saved (default is 'all_instance_types.csv').
    """
    # Define CSV headers
    headers = ['InstanceType', 'vCPUs', 'MemoryGiB', 'Storage', 'BaseClockSpeedGhz', 'Region']

    # Write to CSV file
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data successfully saved to {filename}")


if __name__ == "__main__":
    """
        Main entry point of the script.

        This section performs the following actions:
          1. Prints a starting message for fetching EC2 instance type information.
          2. Calls get_all_regions_instance_info() to retrieve instance data from all regions.
          3. Prints the number of instance types retrieved.
          4. Calls save_to_csv() to write the instance data into a CSV file.
          5. Prints completion messages for each major step.
    """
    print("Fetching all EC2 instance type information...")
    instance_data = get_all_regions_instance_info()
    print(f"Retrieved {len(instance_data)} instance types in the current region.")

    print("Saving data to CSV file...")
    save_to_csv(instance_data)
    print("Done!")
