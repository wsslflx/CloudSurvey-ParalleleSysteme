from flask import Flask, request, jsonify
from CloudSurvey_Package.optimization_solution import *
from dotenv import load_dotenv
import os

load_dotenv()
connection_string_storage = os.getenv('MONGODB_URI2')
connection_string_compute = os.getenv('MONGODB_URI')

app = Flask(__name__)

"""
list_test = ([["FX48-12mds v2 Spot", 3600],["E2s v5 Spot", 3000]])
parallelization_set = [1, 2, 4]
"""

@app.route('/optimize', methods=['POST'])
def optimize():
    # Extract parameters from the HTTP request
    data = request.json


    result = main_optimization(
        provider=data['provider'],
        instance_list=data['instance_list'],
        konfidenzgrad=data['konfidenzgrad'],
        volume=data['volume'],
        premium=data['premium'],
        lrs=data['lrs'],
        parallelization=data['parallelization'],
    )

    # Return the result as a JSON response
    return jsonify({"result": result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5087)