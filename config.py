"""
Configuration file for weather data generation
Modify these values to customize your data generation
"""

import os

# BigQuery Configuration
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "your-project-id-here")
DATASET_ID = "weather_data"

# Data Generation Parameters
NUM_STATIONS = 8  # Number of weather stations to create
DAYS_OF_DATA = 30  # Number of days of historical data to generate
OBSERVATIONS_PER_DAY = 4  # How many observations per day per station (max 24)

# Data Quality Settings
MISSING_DATA_PROBABILITY = 0.05  # 5% chance of missing observations
INACTIVE_STATION_PROBABILITY = 0.1  # 10% chance a station is inactive

# Weather Generation Settings
ENABLE_SEASONAL_VARIATION = True
ENABLE_DAILY_TEMPERATURE_CYCLE = True
ENABLE_ELEVATION_ADJUSTMENT = True

# Additional German Weather Stations (can be extended)
ADDITIONAL_STATIONS = [
    {
        "name": "Düsseldorf Airport", "country": "Germany", "state_province": "North Rhine-Westphalia",
        "city": "Düsseldorf", "lat": 51.2895, "lon": 6.7668, "elevation": 45,
        "climate": "temperate"
    },
    {
        "name": "Leipzig Weather Center", "country": "Germany", "state_province": "Saxony",
        "city": "Leipzig", "lat": 51.3397, "lon": 12.3731, "elevation": 113,
        "climate": "continental"
    },
    {
        "name": "Hannover Station", "country": "Germany", "state_province": "Lower Saxony",
        "city": "Hannover", "lat": 52.3759, "lon": 9.7320, "elevation": 55,
        "climate": "temperate"
    },
    {
        "name": "Bremen Port", "country": "Germany", "state_province": "Bremen",
        "city": "Bremen", "lat": 53.0793, "lon": 8.8017, "elevation": 11,
        "climate": "maritime"
    }
]