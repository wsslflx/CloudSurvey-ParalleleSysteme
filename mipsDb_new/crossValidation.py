import sqlite3
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
import numpy as np


# Load the data from the database
def load_data(db_path='jobs.db'):
    conn = sqlite3.connect(db_path)
    query = "SELECT partition, nnodes, ncpus, io_usage, memory_usage, data_input_size, data_output_size, elapsed_time, mips_estimate FROM generated_jobs"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Prepare the data for training
def prepare_data(df):
    # One-hot encode the partition column
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    partition_encoded = encoder.fit_transform(df[['partition']])
    partition_columns = encoder.get_feature_names_out(['partition'])
    df_encoded = pd.DataFrame(partition_encoded, columns=partition_columns)

    # Combine encoded columns with the rest of the data
    df = pd.concat([df.drop(columns=['partition']), df_encoded], axis=1)

    # Define features and target variable
    X = df.drop(columns=['mips_estimate'])
    y = df['mips_estimate']

    return X, y, encoder, partition_columns


# Train the model and perform cross-validation
def train_and_validate_model(X, y):
    # Initialize the model
    model = RandomForestRegressor(n_estimators=150, random_state=42)

    # Perform cross-validation
    scores = cross_val_score(model, X, y, cv=5, scoring='r2')

    print("Cross-validation R^2 scores:", scores)
    print("Mean R^2 score:", np.mean(scores))

    # Train on the full dataset
    model.fit(X, y)

    return model


if __name__ == "__main__":
    # Load data
    data = load_data()

    # Prepare data
    X, y, encoder, partition_columns = prepare_data(data)

    # Train model and validate
    model = train_and_validate_model(X, y)

    # Save the encoder and model for future use
    import joblib

    joblib.dump(encoder, 'partition_encoder.pkl')
    joblib.dump(model, 'hpc_mips_model.pkl')
    print("Model and encoder saved.")
