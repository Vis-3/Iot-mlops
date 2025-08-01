import os
import logging
import pandas as pd
import mlflow
import joblib
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec, DataType
from mlflow.pyfunc import PythonModel
from autogluon.timeseries import TimeSeriesPredictor

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
HISTORICAL_DATA_PATH = 'data/historical_iot_data.parquet'
ARTIFACT_PATH = 'autogluon_model'
REGISTERED_MODEL_NAME = "AutoGluonIoTAnomalyDetector"
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_URI_FILE_PATH = "detect_anomalies/latest_model_uri.txt"

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("IoT Anomaly Detection Training")


# --- Custom MLflow Wrapper ---
class AutoGluonTimeSeriesWrapper(PythonModel):
    def load_context(self, context):
        from autogluon.timeseries import TimeSeriesPredictor
        self.predictor = TimeSeriesPredictor.load(context.artifacts["model_dir"])
        #import joblib
        #self.predictor = joblib.load(os.path.join(context.artifacts["model_dir"], "predictor.pkl"))

    def predict(self, context, model_input):
        model_input = model_input.copy()
        model_input['target'] = 0
        return self.predictor.predict(model_input)


# --- Training Function ---
def train_anomaly_detector(data_path):
    logging.info(f"Training model with data from: {data_path}")
    if not os.path.exists(data_path):
        logging.error(f"Missing data file at {data_path}")
        return

    try:
        data = pd.read_parquet(data_path)
        logging.info(f"Loaded {len(data)} records.")
    except Exception as e:
        logging.error(f"Failed to read data: {e}")
        return

    # Preprocess
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    data = data.sort_values(by=['device_id', 'timestamp']).reset_index(drop=True)
    data = data.rename(columns={
        'device_id': 'item_id',
        'timestamp': 'timestamp',
        'metric_value': 'target'
    })
    data = data.drop(columns=['is_anomaly', 'metric_name', 'device_type'])

    predictor = TimeSeriesPredictor(
        prediction_length=1,
        path=ARTIFACT_PATH,
        freq='S',
        target='target',
        eval_metric='MAPE',
    )

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        logging.info(f"MLflow Run ID: {run_id}")

        mlflow.log_params({
            "prediction_length": 1,
            "data_path": data_path,
            "training_data_rows": len(data),
            "target_metric": "metric_value",
            "forecast_freq": "S"
        })

        # Train
        logging.info("Fitting model...")
        predictor.fit(train_data=data, presets="fast_training", time_limit=300)
        logging.info("Model training complete.")

        leaderboard = predictor.leaderboard(data, silent=True)
        if not leaderboard.empty:
            top_model_score = leaderboard.iloc[0]['score_test']
            mlflow.log_metric("top_model_score", top_model_score)
            logging.info(f"Top model MAPE: {top_model_score}")

        # Save predictor manually
        #artifact_model_path = "autogluon_model_artifact"
        #os.makedirs(artifact_model_path, exist_ok=True)
        #joblib.dump(predictor, os.path.join(artifact_model_path, "predictor.pkl"))
        artifact_model_path = predictor.path

        # Schema
        input_schema = Schema([
            ColSpec(DataType.string, "item_id"),
            ColSpec(DataType.datetime, "timestamp"),
        ])
        output_schema = Schema([ColSpec(DataType.double, "target")])
        signature = ModelSignature(inputs=input_schema, outputs=output_schema)
        input_example = data.head(1)[['item_id', 'timestamp']]

        # Log the wrapped model
        mlflow.pyfunc.log_model(
            artifact_path=ARTIFACT_PATH,
            python_model=AutoGluonTimeSeriesWrapper(),
            artifacts={"model_dir": artifact_model_path},
            input_example=input_example,
            signature=signature,
            registered_model_name=REGISTERED_MODEL_NAME
        )

        logged_model_uri = f"runs:/{run_id}/{ARTIFACT_PATH}"
        try:
            os.makedirs(os.path.dirname(MODEL_URI_FILE_PATH), exist_ok=True)
            with open(MODEL_URI_FILE_PATH, "w") as f:
                f.write(logged_model_uri)
            logging.info(f"Model URI saved to {MODEL_URI_FILE_PATH}")
        except Exception as e:
            logging.warning(f"Could not save model URI: {e}")

        logging.info(f"Run complete. View at: {MLFLOW_TRACKING_URI}/#/experiments/{run.info.experiment_id}/runs/{run_id}")


if __name__ == "__main__":
    train_anomaly_detector(HISTORICAL_DATA_PATH)
