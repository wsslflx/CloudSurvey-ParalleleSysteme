import requests

url = "http://192.168.178.20:5087/optimize"
data = {
    "provider": "AWS",
    "konfidenzgrad": 95,
    "volume": 500,
    "premium": True,
    "lrs": True,
    "parallelization": [1, 2, 4],
    "partition": "normal",
    "nnodes": 1,
    "ncpus": 4,
    "io_usage": 2.5,
    "memory_usage": 128.0,
    "data_input_size": 50.0,
    "data_output_size": 10.0,
    "elapsed_time": 60
}
'''
provider = "azure"
mips = "1000"

url2 = "http://192.168.178.28:8080/simulate/" + provider + "?cloudletLength=" + str(mips)
response = requests.get(url2)
'''
response = requests.post(url, json=data)
#response = response.json()
print(response)