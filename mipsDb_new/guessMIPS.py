import joblib
import pandas as pd

def predict_mips(model_path, partition, nnodes, ncpus, io_usage, memory_usage, data_input_size, data_output_size, elapsed_time, encoder, partition_columns):
    """
    Predict MIPS for a job based on input parameters using the trained model.

    Parameters:
    - model_path: Path to the trained model file.
    - partition: Partition name (string).
    - nnodes: Number of nodes (integer).
    - ncpus: Number of CPUs (integer).
    - io_usage: IO usage (float).
    - memory_usage: Memory usage (float).
    - data_input_size: Data input size (float).
    - data_output_size: Data output size (float).
    - elapsed_time: Elapsed time (integer).
    - encoder: OneHotEncoder used for encoding the partition column.
    - partition_columns: List of encoded partition column names.

    Returns:
    - Predicted MIPS value.
    """
    BASE_CPU_TAKT = 2.45  # GHz, default value for EPYC processors

    '''
    try:
        with open(json_path, 'r') as file:
            job_data = json.load(file)
    except FileNotFoundError:
        print(f"Error: JSON file not found at {json_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: JSON file at {json_path} is not properly formatted.")
        return None
    '''

    # Prepare input data as a dictionary
    try:
        input_data = {
            'partition': [partition],
            'nnodes': [int(nnodes)],
            'ncpus': [int(ncpus)],
            'cpu_takt': [BASE_CPU_TAKT],  # Use default CPU Takt
            'io_usage': [float(io_usage)],
            'memory_usage': [float(memory_usage)],
            'data_input_size': [float(data_input_size)],
            'data_output_size': [float(data_output_size)],
            'elapsed_time': [int(elapsed_time)]
        }
    except ValueError as e:
        print(f"Error: Invalid value type in input parameters: {e}")
        return None

    # Convert input data to a DataFrame
    input_df = pd.DataFrame(input_data)

    # One-hot encode the partition column
    partition_encoded = encoder.transform(input_df[['partition']])
    partition_df = pd.DataFrame(partition_encoded, columns=partition_columns)

    # Drop the original partition column and combine with encoded columns
    input_df = pd.concat([input_df.drop(columns=['partition']), partition_df], axis=1)

    # Load the trained model
    model = joblib.load(model_path)
    model_columns = model.feature_names_in_  # Get feature names from the trained model

    # Ensure all required columns are present in input_df
    for col in model_columns:
        if col not in input_df.columns:
            input_df[col] = 0

    # Reorder columns to match the model's training data
    input_df = input_df[model_columns]

    # Predict MIPS
    predicted_mips = model.predict(input_df)

    return predicted_mips[0]

'''
# Example usage
if __name__ == "__main__":
    # Load encoder and partition columns (assuming they are available from training)
    encoder = joblib.load("partition_encoder.pkl")
    partition_columns = list(encoder.get_feature_names_out(['partition']))

    # Path to the trained model and JSON file
    model_path = "mips_model.pkl"
    json_path = "job_parameters.json"

    # Predict MIPS
    predicted_mips = predict_mips_from_json(model_path, json_path, encoder, partition_columns)

    if predicted_mips is not None:
        print(f"Predicted MIPS: {predicted_mips}")
'''