import requests
from bs4 import BeautifulSoup
import pandas as pd

file_path = "Azure URLS.xlsx"
df = pd.read_excel(file_path)
urls = df.iloc[:, 1].astype(str).tolist()

"""
urls = [
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/a-family",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/b-family",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/ddsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dlsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dldsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dasv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dadsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dalsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/daldsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dpsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dpdsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dplsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dpldsv6-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dsv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/ddv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/ddsv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dasv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dadsv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dpsv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dpdsv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dplsv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dpldsv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dlsv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dldsv5-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dv4-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dsv4-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dav4-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dasv4-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/ddv4-series?tabs=sizebasic",
    "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/ddsv4-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dcasv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dcadsv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dcesv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dcedsv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dcasccv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dcadsccv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dcsv3-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dcdsv3-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/general-purpose/dcsv2-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/compute-optimized/fasv6-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/compute-optimized/falsv6-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/compute-optimized/famsv6-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/compute-optimized/fsv2-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/compute-optimized/fxmsv2-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/compute-optimized/fxmdsv2-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/compute-optimized/fx-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/esv6-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/edsv6-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/epsv6-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/epdsv6-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/easv6-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/esv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/eadsv6-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/ev5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/edv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/edsv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/easv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/eadsv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/epsv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/epdsv5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/edv4-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/edsv4-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/eav4-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/easv4-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/ev4-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/esv4-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/ebdsv5-ebsv5-series", "https://learn.microsoft.com/de-de/azure/virtual-machines/ecasv5-ecadsv5-series", "https://learn.microsoft.com/de-de/azure/virtual-machines/ecesv5-ecedsv5-series", "https://learn.microsoft.com/de-de/azure/virtual-machines/ecasccv5-ecadsccv5-series", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/mbsv3-mbdsv3-series", "https://learn.microsoft.com/de-de/azure/virtual-machines/msv3-mdsv3-medium-series", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/msv3-mdsv3-high-memory-series", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/memory-optimized/mdsv3-very-high-memory-series", "https://learn.microsoft.com/de-de/azure/virtual-machines/msv2-mdsv2-series", "https://learn.microsoft.com/de-de/azure/virtual-machines/mv2-series", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/storage-optimized/lsv3-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/storage-optimized/lasv3-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/ncadsh100v5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/nccadsh100v5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/ncv2-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/ncv3-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/ncast4v3-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/nca100v4-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/ndasra100v4-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/ndma100v4-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/ndv2-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/ndh100v5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/nd-h200-v5-series?tabs=sizebasic", "https://learn.microsoft.com/de-de/azure/virtual-machines/sizes/gpu-accelerated/ndmi300xv5-series?tabs=sizebasic"

]
"""
# Initialize an empty list to store dataframes
df_list = []

for url in urls:
    # Fetch the content from the URL
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all tables in the page
    tables = soup.find_all('table')

    for table in tables:
        # Read the HTML table into a DataFrame
        df = pd.read_html(str(table))[0]
        # Append the DataFrame to the list
        df_list.append(df)

# Concatenate all DataFrames into one
combined_df = pd.concat(df_list, ignore_index=True)

# Save the combined DataFrame to a CSV file
combined_df.to_csv('azure_vm_sizes.csv', index=False)

print("Data has been successfully exported to 'azure_vm_sizes.csv'.")
