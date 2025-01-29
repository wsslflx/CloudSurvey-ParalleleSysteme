import requests

url = "http://127.0.0.1:5087/optimize"
data = {
    "provider": "Azure",
    "konfidenzgrad": 95,
    "volume": 500,
    "premium": True,
    "lrs": True,
    "parallelization": [1, 2, 4],
    "partition": "normal",
    "nnodes": 4,
    "ncpus": 32,
    "io_usage": 2.5,
    "memory_usage": 128.0,
    "data_input_size": 50.0,
    "data_output_size": 10.0,
    "elapsed_time": 3600
}
response = requests.post(url, json=data)
print(response.json())