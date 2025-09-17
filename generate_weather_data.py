#!/usr/bin/env python3
"""
Weather Data Generator for BigQuery
Generates realistic dummy data for weather_stations and weather_observations tables
"""

import random
import uuid
import math
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os


class WeatherDataGenerator:
    def __init__(self, project_id: str, dataset_id: str = "weather_data"):
        """
        Initialize the weather data generator

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)

        # Weather station templates for different climates
        self.station_templates = [
            {
                "name": "Berlin Tempelhof", "country": "Germany", "state_province": "Berlin",
                "city": "Berlin", "lat": 52.4675, "lon": 13.4021, "elevation": 50,
                "climate": "temperate"
            },
            {
                "name": "Munich Airport", "country": "Germany", "state_province": "Bavaria",
                "city": "Munich", "lat": 48.3538, "lon": 11.7861, "elevation": 448,
                "climate": "temperate"
            },
            {
                "name": "Hamburg Harbor", "country": "Germany", "state_province": "Hamburg",
                "city": "Hamburg", "lat": 53.5488, "lon": 9.9872, "elevation": 8,
                "climate": "maritime"
            },
            {
                "name": "Frankfurt Central", "country": "Germany", "state_province": "Hesse",
                "city": "Frankfurt", "lat": 50.1109, "lon": 8.6821, "elevation": 112,
                "climate": "temperate"
            },
            {
                "name": "Cologne Weather Station", "country": "Germany", "state_province": "North Rhine-Westphalia",
                "city": "Cologne", "lat": 50.9375, "lon": 6.9603, "elevation": 37,
                "climate": "temperate"
            },
            {
                "name": "Stuttgart Observatory", "country": "Germany", "state_province": "Baden-Württemberg",
                "city": "Stuttgart", "lat": 48.7758, "lon": 9.1829, "elevation": 245,
                "climate": "temperate"
            },
            {
                "name": "Dresden Elbe", "country": "Germany", "state_province": "Saxony",
                "city": "Dresden", "lat": 51.0504, "lon": 13.7373, "elevation": 113,
                "climate": "continental"
            },
            {
                "name": "Nuremberg Central", "country": "Germany", "state_province": "Bavaria",
                "city": "Nuremberg", "lat": 49.4521, "lon": 11.0767, "elevation": 302,
                "climate": "temperate"
            },
        ]

        # Weather conditions based on season and temperature
        self.weather_conditions = {
            "sunny": {"temp_range": (15, 35), "humidity_range": (30, 60), "precip_max": 0},
            "partly_cloudy": {"temp_range": (10, 30), "humidity_range": (40, 70), "precip_max": 2},
            "cloudy": {"temp_range": (5, 25), "humidity_range": (50, 80), "precip_max": 5},
            "rainy": {"temp_range": (5, 20), "humidity_range": (70, 95), "precip_max": 50},
            "stormy": {"temp_range": (8, 22), "humidity_range": (75, 95), "precip_max": 100},
            "snowy": {"temp_range": (-10, 3), "humidity_range": (80, 95), "precip_max": 30},
            "foggy": {"temp_range": (0, 15), "humidity_range": (85, 98), "precip_max": 1}
        }

    def generate_weather_stations(self, num_stations: int = None) -> List[Dict[str, Any]]:
        """Generate weather station data"""
        if num_stations is None:
            num_stations = len(self.station_templates)

        stations = []
        now = datetime.now()

        for i in range(min(num_stations, len(self.station_templates))):
            template = self.station_templates[i]
            station_id = f"WS_{template['name'].replace(' ', '_').upper()}_{i + 1:03d}"

            station = {
                "station_id": station_id,
                "station_name": template["name"],
                "location": f"POINT({template['lon']} {template['lat']})",
                "elevation_meters": template["elevation"],
                "country": template["country"],
                "state_province": template["state_province"],
                "city": template["city"],
                "active": random.choice([True] * 9 + [False]),  # 90% active
                "created_at": now - timedelta(days=random.randint(30, 365)),
                "updated_at": now - timedelta(days=random.randint(1, 30)) if random.random() > 0.3 else None
            }
            stations.append(station)

        return stations

    def get_seasonal_params(self, date: datetime) -> Dict[str, float]:
        """Get seasonal temperature and weather parameters"""
        # Simple seasonal model for Central Europe
        day_of_year = date.timetuple().tm_yday

        # Temperature varies seasonally (sine wave with peak in summer)
        base_temp = 10 + 15 * math.sin(2 * math.pi * (day_of_year - 80) / 365)

        # Determine season
        if 80 <= day_of_year <= 172:  # Spring
            season = "spring"
        elif 173 <= day_of_year <= 266:  # Summer
            season = "summer"
        elif 267 <= day_of_year <= 355:  # Autumn
            season = "autumn"
        else:  # Winter
            season = "winter"

        return {
            "base_temp": base_temp,
            "season": season,
            "temp_variance": 8 if season in ["spring", "autumn"] else 5
        }

    def generate_weather_condition(self, temp: float, season: str) -> str:
        """Generate weather condition based on temperature and season"""
        if temp < -5:
            return random.choice(["snowy", "cloudy", "foggy"])
        elif temp < 5:
            return random.choice(["cloudy", "foggy", "rainy", "partly_cloudy"])
        elif temp < 15:
            return random.choice(["cloudy", "rainy", "partly_cloudy", "sunny"])
        elif temp < 25:
            return random.choice(["sunny", "partly_cloudy", "cloudy"])
        else:
            return random.choice(["sunny", "partly_cloudy"])

    def generate_weather_observations(self, stations: List[Dict], days: int = 30,
                                      observations_per_day: int = 24) -> List[Dict[str, Any]]:
        """Generate weather observation data"""

        observations = []
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        for station in stations:
            if not station["active"]:
                continue

            station_id = station["station_id"]

            # Parse coordinates from POINT(lon lat) format
            location_str = station["location"]
            try:
                if location_str.startswith("POINT(") and location_str.endswith(")"):
                    # Extract coordinates from "POINT(lon lat)" format
                    coords_str = location_str[6:-1]  # Remove "POINT(" and ")"
                    coords_parts = coords_str.split()
                    if len(coords_parts) >= 2:
                        lon, lat = float(coords_parts[0]), float(coords_parts[1])
                    else:
                        raise ValueError("Not enough coordinate parts")
                else:
                    # Fallback: try to parse as space-separated coordinates
                    parts = location_str.replace("POINT(", "").replace(")", "").split()
                    if len(parts) >= 2:
                        lon, lat = float(parts[0]), float(parts[1])
                    else:
                        raise ValueError("Could not parse coordinates")
            except Exception as e:
                print(f"Warning: Could not parse location for station {station_id}: {location_str} - {e}")
                lat = 52.0  # Default latitude for Germany
                lon = 10.0  # Default longitude for Germany

            elevation = station["elevation_meters"]

            # Generate observations for each day
            current_date = start_date
            while current_date < end_date:
                seasonal_params = self.get_seasonal_params(current_date)
                base_temp = seasonal_params["base_temp"]

                # Add elevation adjustment (-6.5°C per 1000m)
                base_temp -= (elevation / 1000) * 6.5

                # Generate observations for the day
                for hour in range(0, 24, 24 // observations_per_day):
                    if random.random() < 0.05:  # 5% chance of missing data
                        continue

                    observation_time = current_date + timedelta(hours=hour)

                    # Daily temperature variation (warmer in afternoon)
                    hour_temp_adjustment = 5 * math.sin(math.pi * (hour - 6) / 12)
                    temp = base_temp + hour_temp_adjustment + random.gauss(0, seasonal_params["temp_variance"])

                    # Generate weather condition
                    condition = self.generate_weather_condition(temp, seasonal_params["season"])
                    condition_params = self.weather_conditions[condition]

                    # Adjust parameters based on condition
                    if temp < condition_params["temp_range"][0]:
                        temp = condition_params["temp_range"][0] + random.uniform(-2, 2)
                    elif temp > condition_params["temp_range"][1]:
                        temp = condition_params["temp_range"][1] + random.uniform(-2, 2)

                    observation = {
                        "observation_id": str(uuid.uuid4()),
                        "station_id": station_id,
                        "timestamp": observation_time,
                        "location": station["location"],
                        "temperature_celsius": round(temp, 1),
                        "humidity_percent": round(random.uniform(*condition_params["humidity_range"]), 1),
                        "pressure_hpa": round(random.gauss(1013.25, 15), 1),
                        "wind_speed_ms": round(random.lognormvariate(2, 0.5), 1),
                        "wind_direction_degrees": round(random.uniform(0, 360), 1),
                        "precipitation_mm": round(
                            max(0, random.expovariate(5 / max(1, condition_params["precip_max"]))), 2),
                        "visibility_km": round(random.uniform(0.1 if condition == "foggy" else 5, 50), 1),
                        "weather_condition": condition
                    }

                    observations.append(observation)

                current_date += timedelta(days=1)

        return observations

    def insert_data_to_bigquery(self, table_name: str, data: List[Dict[str, Any]]):
        """Insert data into BigQuery table with upsert logic for stations"""
        table_ref = self.client.dataset(self.dataset_id).table(table_name)

        try:
            table = self.client.get_table(table_ref)
        except NotFound:
            print(f"Table {table_name} not found. Make sure to run terraform apply first.")
            return

        # Convert datetime objects to strings for BigQuery
        for row in data:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()

        # Special handling for weather_stations to prevent duplicates
        if table_name == "weather_stations":
            self._upsert_weather_stations(data)
        else:
            # For observations, use regular append
            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = "WRITE_APPEND"

            job = self.client.load_table_from_json(data, table, job_config=job_config)
            job.result()  # Wait for the job to complete

            print(f"Inserted {len(data)} rows into {table_name}")

    def _upsert_weather_stations(self, stations_data: List[Dict[str, Any]]):
        """Upsert weather stations to prevent duplicates"""
        print("Checking for existing weather stations...")

        # Get existing station IDs
        existing_query = f"""
        SELECT station_id 
        FROM `{self.project_id}.{self.dataset_id}.weather_stations`
        """

        try:
            existing_job = self.client.query(existing_query)
            existing_stations = {row.station_id for row in existing_job.result()}
            print(f"Found {len(existing_stations)} existing stations")
        except Exception as e:
            print(f"No existing stations found or error querying: {e}")
            existing_stations = set()

        # Filter out stations that already exist
        new_stations = []
        updated_stations = []

        for station in stations_data:
            if station["station_id"] in existing_stations:
                updated_stations.append(station)
            else:
                new_stations.append(station)

        # Insert new stations
        if new_stations:
            table_ref = self.client.dataset(self.dataset_id).table("weather_stations")
            table = self.client.get_table(table_ref)

            job_config = bigquery.LoadJobConfig()
            job_config.write_disposition = "WRITE_APPEND"

            job = self.client.load_table_from_json(new_stations, table, job_config=job_config)
            job.result()
            print(f"Inserted {len(new_stations)} NEW weather stations")

        # Update existing stations (merge logic)
        if updated_stations:
            print(f"Found {len(updated_stations)} existing stations - updating them...")
            self._update_existing_stations(updated_stations)

        if not new_stations and not updated_stations:
            print("No new stations to insert and no stations to update")

    def _update_existing_stations(self, stations_data: List[Dict[str, Any]]):
        """Update existing weather stations with new data"""
        for station in stations_data:
            update_query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.weather_stations`
            SET 
                station_name = @station_name,
                location = ST_GEOGPOINT(@lon, @lat),
                elevation_meters = @elevation_meters,
                country = @country,
                state_province = @state_province,
                city = @city,
                active = @active,
                updated_at = CURRENT_TIMESTAMP()
            WHERE station_id = @station_id
            """

            # Parse coordinates for the update
            location_str = station["location"]
            if location_str.startswith("POINT(") and location_str.endswith(")"):
                coords_str = location_str[6:-1]
                coords_parts = coords_str.split()
                lon, lat = float(coords_parts[0]), float(coords_parts[1])
            else:
                lon, lat = 10.0, 52.0  # Default for Germany

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("station_id", "STRING", station["station_id"]),
                    bigquery.ScalarQueryParameter("station_name", "STRING", station["station_name"]),
                    bigquery.ScalarQueryParameter("lon", "FLOAT", lon),
                    bigquery.ScalarQueryParameter("lat", "FLOAT", lat),
                    bigquery.ScalarQueryParameter("elevation_meters", "FLOAT", station["elevation_meters"]),
                    bigquery.ScalarQueryParameter("country", "STRING", station["country"]),
                    bigquery.ScalarQueryParameter("state_province", "STRING", station["state_province"]),
                    bigquery.ScalarQueryParameter("city", "STRING", station["city"]),
                    bigquery.ScalarQueryParameter("active", "BOOL", station["active"]),
                ]
            )

            query_job = self.client.query(update_query, job_config=job_config)
            query_job.result()

    def generate_and_insert_all_data(self, num_stations: int = 8, days: int = 30,
                                     observations_per_day: int = 4):
        """Generate and insert all weather data"""
        # Show current status
        print("Checking current database status...")
        self.get_data_status()

        print("Generating weather stations...")
        stations = self.generate_weather_stations(num_stations)

        print(f"Processing {len(stations)} weather stations...")
        self.insert_data_to_bigquery("weather_stations", stations)

        print("Generating weather observations...")
        observations = self.generate_weather_observations(
            stations, days=days, observations_per_day=observations_per_day
        )

        print(f"Inserting {len(observations)} weather observations...")
        self.insert_data_to_bigquery("weather_observations", observations)

        # Show final status
        print("Data generation and insertion completed!")
        self.get_data_status()

    def cleanup_duplicate_stations(self):
        """Remove duplicate weather stations, keeping the most recent"""
        print("Checking for duplicate weather stations...")

        # Find duplicates based on station_name and location
        duplicate_query = f"""
        WITH station_counts AS (
            SELECT 
                station_name,
                location,
                COUNT(*) as count,
                ARRAY_AGG(station_id ORDER BY created_at DESC) as station_ids
            FROM `{self.project_id}.{self.dataset_id}.weather_stations`
            GROUP BY station_name, location
            HAVING COUNT(*) > 1
        )
        SELECT station_name, location, count, station_ids
        FROM station_counts
        """

        try:
            query_job = self.client.query(duplicate_query)
            duplicates = list(query_job.result())

            if not duplicates:
                print("No duplicate stations found!")
                return

            print(f"Found {len(duplicates)} sets of duplicate stations")

            stations_to_delete = []
            for row in duplicates:
                # Keep the first (most recent) station, mark others for deletion
                keep_station = row.station_ids[0]
                delete_stations = row.station_ids[1:]
                print(f"Keeping {keep_station}, will delete {len(delete_stations)} duplicates of '{row.station_name}'")
                stations_to_delete.extend(delete_stations)

            # Delete duplicate stations
            if stations_to_delete:
                station_ids_str = "', '".join(stations_to_delete)
                delete_query = f"""
                DELETE FROM `{self.project_id}.{self.dataset_id}.weather_stations`
                WHERE station_id IN ('{station_ids_str}')
                """

                delete_job = self.client.query(delete_query)
                delete_job.result()
                print(f"Deleted {len(stations_to_delete)} duplicate weather stations")

        except Exception as e:
            print(f"Error cleaning up duplicates: {e}")

    def get_data_status(self):
        """Get current status of data in the database"""
        try:
            # Count stations
            stations_query = f"""
            SELECT 
                COUNT(*) as total_stations,
                COUNT(*) FILTER (WHERE active = true) as active_stations,
                MIN(created_at) as oldest_station,
                MAX(created_at) as newest_station
            FROM `{self.project_id}.{self.dataset_id}.weather_stations`
            """

            stations_job = self.client.query(stations_query)
            station_stats = list(stations_job.result())[0]

            # Count observations
            obs_query = f"""
            SELECT 
                COUNT(*) as total_observations,
                COUNT(DISTINCT station_id) as stations_with_data,
                MIN(timestamp) as earliest_obs,
                MAX(timestamp) as latest_obs
            FROM `{self.project_id}.{self.dataset_id}.weather_observations`
            """

            obs_job = self.client.query(obs_query)
            obs_stats = list(obs_job.result())[0]

            print("\n" + "=" * 50)
            print("📊 CURRENT DATA STATUS")
            print("=" * 50)
            print(f"Weather Stations: {station_stats.total_stations} total, {station_stats.active_stations} active")
            if station_stats.oldest_station:
                print(f"Station Date Range: {station_stats.oldest_station} to {station_stats.newest_station}")

            print(f"Weather Observations: {obs_stats.total_observations:,}")
            print(f"Stations with Data: {obs_stats.stations_with_data}")
            if obs_stats.earliest_obs:
                print(f"Observation Date Range: {obs_stats.earliest_obs} to {obs_stats.latest_obs}")
            print("=" * 50 + "\n")

        except Exception as e:
            print(f"Error getting data status: {e}")


def main():
    # Configuration
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "your-project-id")
    DATASET_ID = "weather_data"

    if PROJECT_ID == "your-project-id":
        print("Please set the GOOGLE_CLOUD_PROJECT environment variable or modify PROJECT_ID in the script")
        return

    # Initialize generator
    generator = WeatherDataGenerator(PROJECT_ID, DATASET_ID)

    # Generate and insert data
    # This will create 8 stations with 30 days of data (4 observations per day)
    generator.generate_and_insert_all_data(
        num_stations=8,
        days=30,
        observations_per_day=4
    )


if __name__ == "__main__":
    main()