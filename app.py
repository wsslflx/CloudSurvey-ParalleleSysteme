from flask import Flask, request, jsonify
from CloudSurvey_Package.optimization_solution import *
from dotenv import load_dotenv
import os
import joblib
from mipsDb_new.guessMIPS import predict_mips
import requests
import logging

load_dotenv()
connection_string_storage = os.getenv('MONGODB_URI2')
connection_string_compute = os.getenv('MONGODB_URI')

app = Flask(__name__)

"""
instance_list = [["FX48-12mds v2 Spot", 3600],["E2s v5 Spot", 3000]]

list_test = ([["FX48-12mds v2 Spot", 3600],["E2s v5 Spot", 3000]])
parallelization_set = [1, 2, 4]
"""

@app.route('/optimize', methods=['POST'])
def optimize():
    # Extract parameters from the HTTP request
    data = request.json

    provider = data['provider']
    instance_list = data['instance_list']
    konfidenzgrad = data['konfidenzgrad']
    volume = data['volume']
    premium = data['premium']
    lrs = data['lrs']
    parallelization = data['parallelization']

    encoder = joblib.load("mipsDb_new/partition_encoder.pkl")
    partition_columns = list(encoder.get_feature_names_out(['partition']))

    partition = data['partition']
    nnodes = data['nnodes']
    ncpus = data['ncpus']
    io_usage = data['io_usage']
    memory_usage = data['memory_usage']
    data_input_size = data['data_input_size']
    data_output_size = data['data_output_size']
    elapsed_time = data['elapsed_time']

    model_path = "mipsDb_new/mips_model.pkl"

    mips = predict_mips(model_path, partition, nnodes, ncpus, io_usage, memory_usage, data_input_size, data_output_size,
                 elapsed_time, encoder, partition_columns)


    mips = int(mips)

    logging.basicConfig(level=logging.INFO)

    url_simulate = "http://192.168.178.28:8080/simulate/" + provider.lower() + "?cloudletLength=" + str(mips)
    instances = requests.get(url_simulate)

    instances2 = instances.json()

    instance_list = [[item['instance_name'], item['execution_time']] for item in instances2]

    filtered_instances = [
        item for item in instance_list if not any(
            str(value).lower() == 'nan' for value in item
        )
    ]

    result = main_optimization(
        provider,
        filtered_instances[:1],
        konfidenzgrad,
        volume,
        premium,
        lrs,
        parallelization,
    )

    logging.info(result)

    # Return the result as a JSON response
    return jsonify({"result": result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5087)