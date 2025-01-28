from flask import Flask, request, jsonify
from CloudSurvey_Package.optimization_solution import main_optimization
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get connection strings from environment variables
connection_string_compute = os.getenv('MONGODB_URI')
connection_string_storage = os.getenv('MONGODB_URI2')

# Initialize Flask app
app = Flask(__name__)

@app.route('/run-optimization', methods=['POST'])
def run_optimization():
    try:
        # Parse input from the request
        data = request.get_json()

        # Extract required parameters from the request payload
        provider = data.get('provider', 'Azure')  # Default to "Azure"
        instance_list = data.get('instance_list', [])
        konfidenzgrad = data.get('konfidenzgrad', 95)  # Default to 95
        data_volume = data.get('data_volume', 200)  # Default to 200
        premium = data.get('premium', False)
        lrs = data.get('lrs', False)
        parallelization_set = data.get('parallelization_set', [1, 2, 4])

        # Call the main_optimization function with provided parameters
        response = main_optimization(
            provider,
            instance_list,
            konfidenzgrad,
            data_volume,
            premium,
            lrs,
            parallelization_set
        )

        # Return success response
        return jsonify({
            'status': 'success',
            'result': response
        })
    except Exception as e:
        # Handle errors and return error response
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5870)
