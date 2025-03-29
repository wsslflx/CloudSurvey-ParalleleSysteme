import requests
import json

url = "http://136.172.13.164:5087/optimize"

with open("input_parameter.json", "r") as f:
    data = json.load(f)

response = requests.post(url, json=data)
print(response)
with open("output_instance.json", "w") as out_file:
    json.dump(response.json(), out_file, indent=4)