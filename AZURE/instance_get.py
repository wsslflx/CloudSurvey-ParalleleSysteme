import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
from urllib.parse import urlparse

file_path = "Azure URLS.xlsx"
df = pd.read_excel(file_path)
# urls = df.iloc[:, 1].astype(str).tolist()

# """
urls = [
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/high-performance-compute/hb-series?tabs=sizeaccelerators",
]
# """

# Function to extract the instance name from the URL
def extract_instance_name(url):
    path = urlparse(url).path
    last_segment = path.split('/')[-1]
    instance_name = last_segment.split('?')[0]
    return instance_name

# Initialize an empty dictionary to store data by instance
instance_data = {}

for url in urls:
    # Extract the instance name from the URL
    instance = extract_instance_name(url)

    # Fetch the content from the URL
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all tables in the page
    tables = soup.find_all('table')
    print(tables)

    for table in tables:
        print("ab jetzt einzelne Table")
        print(table)
        table_str = str(table)
        # Read the HTML table into a DataFrame
        table_io = StringIO(table_str)
        # Use the StringIO object with pd.read_html
        df = pd.read_html(table_io)[0]

        # Add an 'Instance' column
        df['Instance'] = instance

        # Append data to the respective instance DataFrame
        if instance in instance_data:
            instance_data[instance] = pd.concat([instance_data[instance], df], ignore_index=True)
        else:
            instance_data[instance] = df

# Combine all data into a single DataFrame
# combined_df = pd.concat(instance_data.values(), ignore_index=True)

# Save the combined DataFrame to a CSV file
# combined_df.to_csv('azure_vm_sizes_unified.csv', index=False)

print("Data has been successfully exported to 'azure_vm_sizes_unified.csv'.")

