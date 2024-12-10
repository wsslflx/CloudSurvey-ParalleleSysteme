import boto3
import csv
def get_all_regions_instance_info():
    ec2_client = boto3.client('ec2', region_name='eu-central-1')
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
    # Define CSV headers
    headers = ['InstanceType', 'vCPUs', 'MemoryGiB', 'Storage', 'BaseClockSpeedGhz', 'Region']

    # Write to CSV file
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data successfully saved to {filename}")


if __name__ == "__main__":
    print("Fetching all EC2 instance type information...")
    instance_data = get_all_regions_instance_info()
    print(f"Retrieved {len(instance_data)} instance types in the current region.")

    print("Saving data to CSV file...")
    save_to_csv(instance_data)
    print("Done!")
