from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
import os
from datetime import datetime
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Wildfire Risk Prediction API",
    description="API for predicting wildfire risk across German states using BigQuery ML",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration - will use gcloud auth application-default login
PROJECT_ID = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
DATASET_ID = os.getenv("DATASET_ID", "weather_data")
MODEL_NAME = os.getenv("MODEL_NAME", "wildfire_risk_model")

# Ensure PROJECT_ID is set
if not PROJECT_ID:
    logger.error("PROJECT_ID not set. Please set GCP_PROJECT_ID environment variable.")
    PROJECT_ID = "your-project-id"  # Fallback, will likely cause errors but allows startup

# Initialize BigQuery client
client = None


def get_bigquery_client():
    """Initialize BigQuery client using Application Default Credentials"""
    global client
    if client is None:
        try:
            # Use Application Default Credentials (from gcloud auth application-default login)
            client = bigquery.Client(project=PROJECT_ID)
            logger.info(f"BigQuery client initialized with ADC for project: {PROJECT_ID}")

            # Test the connection
            query = "SELECT 1 as test_connection"
            test_job = client.query(query)
            test_job.result()  # Wait for the job to complete
            logger.info("BigQuery connection test successful")

        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client with ADC: {e}")
            logger.error("Make sure you have run: gcloud auth application-default login")
            raise
    return client


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("Starting Wildfire Risk Prediction API")
    get_bigquery_client()
    logger.info("API ready to serve requests")


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "Wildfire Risk Prediction API",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "project_id": PROJECT_ID,
        "dataset_id": DATASET_ID,
        "model_name": MODEL_NAME
    }


@app.get("/health")
async def health_check():
    """Detailed health check including BigQuery connectivity"""
    try:
        bq_client = get_bigquery_client()
        # Test BigQuery connection
        query = f"SELECT 1 as test"
        bq_client.query(query).result()

        return {
            "status": "healthy",
            "bigquery": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "bigquery": "disconnected",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@app.get("/predictions", response_model=List[Dict[str, Any]])
async def get_current_predictions():
    """Get current wildfire predictions for all German states"""
    try:
        bq_client = get_bigquery_client()

        query = f"""
        SELECT
            station_id,
            state_province,
            temperature_celsius,
            humidity_percent,
            wind_speed_ms,
            precipitation_mm,
            weather_condition,
            predicted_wildfire_risk_label,
            predicted_wildfire_risk_label_probs[OFFSET(0)].label as high_risk_label,
            predicted_wildfire_risk_label_probs[OFFSET(0)].prob as high_risk_probability,
            timestamp
        FROM `{PROJECT_ID}.{DATASET_ID}.next_day_wildfire_predictions`
        ORDER BY high_risk_probability DESC
        """

        logger.info(f"Executing query for current predictions")
        query_job = bq_client.query(query)
        results = query_job.result()

        predictions = []
        for row in results:
            predictions.append({
                "station_id": row.station_id,
                "state_province": row.state_province,
                "temperature_celsius": float(row.temperature_celsius) if row.temperature_celsius else None,
                "humidity_percent": float(row.humidity_percent) if row.humidity_percent else None,
                "wind_speed_ms": float(row.wind_speed_ms) if row.wind_speed_ms else None,
                "precipitation_mm": float(row.precipitation_mm) if row.precipitation_mm else None,
                "weather_condition": row.weather_condition,
                "predicted_risk_level": row.predicted_wildfire_risk_label,
                "risk_probability": float(row.high_risk_probability) if row.high_risk_probability else None,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None
            })

        logger.info(f"Retrieved {len(predictions)} predictions")
        return predictions

    except Exception as e:
        logger.error(f"Error retrieving predictions: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve predictions: {str(e)}")


@app.get("/predictions/{state_province}")
async def get_state_prediction(state_province: str):
    """Get wildfire prediction for a specific German state"""
    try:
        bq_client = get_bigquery_client()

        query = f"""
        SELECT
            station_id,
            state_province,
            temperature_celsius,
            humidity_percent,
            wind_speed_ms,
            precipitation_mm,
            weather_condition,
            predicted_wildfire_risk_label,
            predicted_wildfire_risk_label_probs[OFFSET(0)].label as high_risk_label,
            predicted_wildfire_risk_label_probs[OFFSET(0)].prob as high_risk_probability,
            timestamp
        FROM `{PROJECT_ID}.{DATASET_ID}.next_day_wildfire_predictions`
        WHERE LOWER(state_province) = LOWER(@state_name)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("state_name", "STRING", state_province)
            ]
        )

        logger.info(f"Executing query for state: {state_province}")
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result()

        predictions = []
        for row in results:
            predictions.append({
                "station_id": row.station_id,
                "state_province": row.state_province,
                "temperature_celsius": float(row.temperature_celsius) if row.temperature_celsius else None,
                "humidity_percent": float(row.humidity_percent) if row.humidity_percent else None,
                "wind_speed_ms": float(row.wind_speed_ms) if row.wind_speed_ms else None,
                "precipitation_mm": float(row.precipitation_mm) if row.precipitation_mm else None,
                "weather_condition": row.weather_condition,
                "predicted_risk_level": row.predicted_wildfire_risk_label,
                "risk_probability": float(row.high_risk_probability) if row.high_risk_probability else None,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None
            })

        if not predictions:
            raise HTTPException(status_code=404, detail=f"No predictions found for state: {state_province}")

        logger.info(f"Retrieved {len(predictions)} predictions for {state_province}")
        return predictions[0]  # Return single prediction for the state

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving prediction for {state_province}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve prediction: {str(e)}")


@app.get("/states")
async def get_available_states():
    """Get list of all available German states with predictions"""
    try:
        bq_client = get_bigquery_client()

        query = f"""
        SELECT DISTINCT state_province
        FROM `{PROJECT_ID}.{DATASET_ID}.next_day_wildfire_predictions`
        ORDER BY state_province
        """

        logger.info("Retrieving available states")
        query_job = bq_client.query(query)
        results = query_job.result()

        states = [row.state_province for row in results]

        logger.info(f"Found {len(states)} states with predictions")
        return {"states": states, "count": len(states)}

    except Exception as e:
        logger.error(f"Error retrieving states: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve states: {str(e)}")


@app.get("/model/info")
async def get_model_info():
    """Get information about the ML model"""
    try:
        bq_client = get_bigquery_client()

        query = f"""
        SELECT 
            * 
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.MODELS`
        WHERE model_name = '{MODEL_NAME}'
        """

        logger.info("Retrieving model information")
        query_job = bq_client.query(query)
        results = query_job.result()

        model_info = []
        for row in results:
            model_info.append({
                "model_catalog": row.model_catalog,
                "model_schema": row.model_schema,
                "model_name": row.model_name,
                "model_type": row.model_type,
                "creation_time": row.creation_time.isoformat() if row.creation_time else None,
                "last_modified_time": row.last_modified_time.isoformat() if row.last_modified_time else None
            })

        if not model_info:
            return {"error": "Model not found", "model_name": MODEL_NAME}

        return model_info[0]

    except Exception as e:
        logger.error(f"Error retrieving model info: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve model info: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)