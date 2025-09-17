"""
Simple configuration for weather data generation
"""
import os

# Basic settings
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "your-project-id")
DATASET_ID = "weather_data"

# Generation settings
DEFAULT_STATIONS = 8
DEFAULT_DAYS = 30
DEFAULT_OBS_PER_DAY = 4

# Data quality
MISSING_DATA_RATE = 0.05  # 5% missing observations