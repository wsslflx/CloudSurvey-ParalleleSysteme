import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
from urllib.parse import urlparse
import csv

file_path = "Azure URLS.xlsx"
df = pd.read_excel(file_path)
urls = df.iloc[:, 1].astype(str).tolist()

"""
urls = [
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/high-performance-compute/hb-series?tabs=sizeaccelerators",
]
"""
def normalize_header(header_name):
    # If the header is "Name Größe", rename it to "Name der Größe"
    if header_name == "Name Größe":
        return "Name der Größe"
    return header_name


for url in urls:
    # Fetch the content from the URL
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all tables in the page
    tables = soup.find_all('table')

    tables = tables[1:]
    data = {}

    # Parse the first table
    header_1 = [normalize_header(th.get_text(strip=True)) for th in tables[0].find('thead').find_all('th')]
    rows_1 = tables[0].find('tbody').find_all('tr')

    for row in rows_1:
        cells = [td.get_text(strip=True) for td in row.find_all('td')]
        name = cells[0]
        if name not in data:
            data[name] = {}
        for h, c in zip(header_1[1:], cells[1:]):
            data[name][h] = c

    # Parse the second table
    header_2 = [normalize_header(th.get_text(strip=True)) for th in tables[1].find('thead').find_all('th')]
    rows_2 = tables[1].find('tbody').find_all('tr')

    for row in rows_2:
        cells = [td.get_text(strip=True) for td in row.find_all('td')]
        name = cells[0]
        if name not in data:
            data[name] = {}
        for h, c in zip(header_2[1:], cells[1:]):
            data[name][h] = c

    # Parse the third table
    header_3 = [normalize_header(th.get_text(strip=True)) for th in tables[2].find('thead').find_all('th')]
    rows_3 = tables[2].find('tbody').find_all('tr')

    for row in rows_3:
        cells = [td.get_text(strip=True) for td in row.find_all('td')]
        name = cells[0]
        if name not in data:
            data[name] = {}
        for h, c in zip(header_3[1:], cells[1:]):
            data[name][h] = c

    # Parse the fourth table
    header_4 = [normalize_header(th.get_text(strip=True)) for th in tables[3].find('thead').find_all('th')]
    rows_4 = tables[3].find('tbody').find_all('tr')

    for row in rows_4:
        cells = [td.get_text(strip=True) for td in row.find_all('td')]
        name = cells[0]
        if name not in data:
            data[name] = {}
        for h, c in zip(header_4[1:], cells[1:]):
            data[name][h] = c



# First, determine all unique columns across all instances
all_columns = set()
for info in data.values():
    all_columns.update(info.keys())

# The first column will be "Name der Größe" (the instance name),
# followed by all other columns we gathered
columns = ["Name der Größe"] + list(all_columns)

# Write to CSV
with open('vm_sizes.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()

    for name, info in data.items():
        # Build the row for this instance
        row = {"Name der Größe": name}
        # Update with the other columns/values
        row.update(info)
        writer.writerow(row)

# Combine all data into a single DataFrame
# combined_df = pd.concat(instance_data.values(), ignore_index=True)

# Save the combined DataFrame to a CSV file
# combined_df.to_csv('azure_vm_sizes_unified.csv', index=False)

print("Data has been successfully exported to 'azure_vm_sizes_unified.csv'.")

