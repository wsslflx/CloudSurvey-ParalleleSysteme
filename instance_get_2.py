import requests
from bs4 import BeautifulSoup
import pandas as pd
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

def parse_table(table, data):
    # Parse the first table
    header_1 = [normalize_header(th.get_text(strip=True)) for th in table.find('thead').find_all('th')]
    rows_1 = table.find('tbody').find_all('tr')

    for row in rows_1:
        cells = [td.get_text(strip=True) for td in row.find_all('td')]
        name = cells[0]
        if name not in data:
            data[name] = {}
        for h, c in zip(header_1[1:], cells[1:]):
            data[name][h] = c

data = {}

for url in urls:
    print(url)
    # Fetch the content from the URL
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all tables in the page
    tables = soup.find_all('table')
    tables = tables[1:]

    for table in tables:
        parse_table(table, data)

# First, determine all unique columns across all instances
all_columns = set()
for info in data.values():
    all_columns.update(info.keys())

columns = ["Name der Größe"] + list(all_columns)

# Write to CSV
with open('vm_sizes.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()

    for name, info in data.items():
        row = {"Name der Größe": name}
        row.update(info)
        writer.writerow(row)

print("Data has been successfully exported to 'vm_sizes.csv'.")

