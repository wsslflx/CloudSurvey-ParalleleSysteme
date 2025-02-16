import requests

url = "http://136.172.13.164:5087/optimize"
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

response = requests.post(url, json=data)
#response = response.json()
print(response)