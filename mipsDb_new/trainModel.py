import sqlite3
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
import joblib

def prepare_data(db_path="jobs.db"):
    """Loads and prepares data from the generated_jobs database."""
    conn = sqlite3.connect(db_path)

    query = """
    SELECT 
        partition, nnodes, ncpus, cpu_takt, io_usage, memory_usage, 
        data_input_size, data_output_size, elapsed_time, mips_estimate
    FROM generated_jobs
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # One-hot encode Partition column
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    partition_encoded = encoder.fit_transform(df[['partition']])
    partition_columns = encoder.get_feature_names_out(['partition'])
    df_encoded = pd.DataFrame(partition_encoded, columns=partition_columns)

    df = pd.concat([df.drop(columns=['partition']), df_encoded], axis=1)

    return df, encoder, partition_columns

def train_random_forest(df, model_path="mips_model.pkl", encoder_path="partition_encoder.pkl"):
    """Train a Random Forest model to predict MIPS and save the encoder."""
    X = df.drop(columns=['mips_estimate'])
    y = df['mips_estimate']

    # Split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train a Random Forest Regressor
    model = RandomForestRegressor(n_estimators=150, random_state=42)
    model.fit(X_train, y_train)

    # Evaluate model performance
    score = model.score(X_test, y_test)
    print(f"Model R^2 score on test data: {score:.2f}")

    # Display feature importances
    feature_importances = model.feature_importances_
    feature_names = X.columns
    importance_df = pd.DataFrame({
        'Feature': feature_names,
        'Importance': feature_importances
    }).sort_values(by='Importance', ascending=False)
    print("Feature Importances:")
    print(importance_df)

    # Save the trained model and encoder
    joblib.dump(model, model_path)
    joblib.dump(encoder, encoder_path)
    print(f"Model saved as {model_path}")
    print(f"Encoder saved as {encoder_path}")

    return model

if __name__ == "__main__":
    # Prepare data
    data, encoder, partition_columns = prepare_data()

    # Train Random Forest model and save it along with the encoder
    model = train_random_forest(data, model_path="mips_model.pkl", encoder_path="partition_encoder.pkl")

    print("Trained model and encoder have been saved.")
