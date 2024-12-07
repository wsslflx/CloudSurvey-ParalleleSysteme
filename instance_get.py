import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO

file_path = "Azure URLS.xlsx"
df = pd.read_excel(file_path)
urls = df.iloc[:, 1].astype(str).tolist()

"""
urls = [
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/a-family",
]
"""
# Initialize an empty list to store dataframes
df_list = []

for url in urls:
    print( str(url) + " in progess")
    # Fetch the content from the URL
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all tables in the page
    tables = soup.find_all('table')

    for table in tables:
        table_str = str(table)  # Convert to string if necessary
        # Wrap the string in a StringIO object
        table_io = StringIO(table_str)
        # Use the StringIO object with pd.read_html
        df = pd.read_html(table_io)[0]
        # Read the HTML table into a DataFrame
        # df = pd.read_html(str(table))[0]
        # Append the DataFrame to the list
        df_list.append(df)

# Concatenate all DataFrames into one
combined_df = pd.concat(df_list, ignore_index=True)

# Save the combined DataFrame to a CSV file
combined_df.to_csv('azure_vm_sizes.csv', index=False)

print("Data has been successfully exported to 'azure_vm_sizes.csv'.")
