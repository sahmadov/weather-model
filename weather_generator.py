#!/usr/bin/env python3
"""
Simplified Weather Data Generator for BigQuery
Generates realistic dummy data for weather_stations and weather_observations tables
"""

import random
import uuid
import math
from datetime import datetime, timedelta
from typing import List, Dict, Any
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os


class WeatherGenerator:
    def __init__(self, project_id: str, dataset_id: str = "weather_data"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)

        # Simple station templates
        self.stations_data = [
            {"name": "Berlin Tempelhof", "city": "Berlin", "lat": 52.4675, "lon": 13.4021, "elevation": 50},
            {"name": "Munich Airport", "city": "Munich", "lat": 48.3538, "lon": 11.7861, "elevation": 448},
            {"name": "Hamburg Harbor", "city": "Hamburg", "lat": 53.5488, "lon": 9.9872, "elevation": 8},
            {"name": "Frankfurt Central", "city": "Frankfurt", "lat": 50.1109, "lon": 8.6821, "elevation": 112},
            {"name": "Cologne Weather", "city": "Cologne", "lat": 50.9375, "lon": 6.9603, "elevation": 37},
            {"name": "Stuttgart Observatory", "city": "Stuttgart", "lat": 48.7758, "lon": 9.1829, "elevation": 245},
            {"name": "Dresden Elbe", "city": "Dresden", "lat": 51.0504, "lon": 13.7373, "elevation": 113},
            {"name": "Nuremberg Central", "city": "Nuremberg", "lat": 49.4521, "lon": 11.0767, "elevation": 302},
        ]

    def create_stations(self, count: int = None) -> List[Dict]:
        """Create weather station records"""
        if count is None:
            count = len(self.stations_data)

        stations = []
        now = datetime.now()

        for i in range(min(count, len(self.stations_data))):
            station = self.stations_data[i]
            station_id = f"WS_{i + 1:03d}_{station['name'].replace(' ', '_').upper()}"

            stations.append({
                "station_id": station_id,
                "station_name": station["name"],
                "location": f"POINT({station['lon']} {station['lat']})",
                "elevation_meters": station["elevation"],
                "country": "Germany",
                "state_province": self._get_state(station["city"]),
                "city": station["city"],
                "active": True,
                "created_at": now - timedelta(days=random.randint(30, 365)),
                "updated_at": None
            })

        return stations

    def create_observations(self, stations: List[Dict], days: int = 30) -> List[Dict]:
        """Create weather observation records"""
        observations = []
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        for station in stations:
            current_date = start_date
            elevation = station["elevation_meters"]

            while current_date < end_date:
                # 4 observations per day (6 hours apart)
                for hour in [6, 12, 18, 24]:
                    # Skip some observations randomly (missing data)
                    if random.random() < 0.05:
                        continue

                    timestamp = current_date + timedelta(hours=hour)

                    # Simple temperature model
                    base_temp = self._get_seasonal_temp(current_date)
                    daily_variation = 5 * math.sin(math.pi * (hour - 6) / 12)  # Warmer in afternoon
                    elevation_adjustment = -(elevation / 1000) * 6.5  # Colder at altitude
                    temp = base_temp + daily_variation + elevation_adjustment + random.gauss(0, 3)

                    # Weather condition based on temperature
                    condition = self._get_weather_condition(temp)

                    observations.append({
                        "observation_id": str(uuid.uuid4()),
                        "station_id": station["station_id"],
                        "timestamp": timestamp,
                        "location": station["location"],
                        "temperature_celsius": round(temp, 1),
                        "humidity_percent": round(random.uniform(40, 85), 1),
                        "pressure_hpa": round(random.gauss(1013, 15), 1),
                        "wind_speed_ms": round(random.lognormvariate(2, 0.5), 1),
                        "wind_direction_degrees": round(random.uniform(0, 360), 1),
                        "precipitation_mm": round(self._get_precipitation(condition), 2),
                        "visibility_km": round(random.uniform(5, 50), 1),
                        "weather_condition": condition
                    })

                current_date += timedelta(days=1)

        return observations

    def insert_to_bigquery(self, table_name: str, data: List[Dict]):
        """Insert data into BigQuery table"""
        if not data:
            print(f"No data to insert into {table_name}")
            return

        table_ref = self.client.dataset(self.dataset_id).table(table_name)

        try:
            table = self.client.get_table(table_ref)
        except NotFound:
            print(f"Table {table_name} not found. Run terraform apply first.")
            return

        # Convert datetime objects to strings
        for row in data:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()

        # Insert data
        job = self.client.load_table_from_json(data, table)
        job.result()
        print(f"✅ Inserted {len(data)} rows into {table_name}")

    def generate_all(self, num_stations: int = 8, days: int = 30):
        """Generate and insert all data"""
        print(f"🏭 Creating {num_stations} weather stations...")
        stations = self.create_stations(num_stations)
        self.insert_to_bigquery("weather_stations", stations)

        print(f"🌤️  Generating {days} days of weather observations...")
        observations = self.create_observations(stations, days)
        self.insert_to_bigquery("weather_observations", observations)

        print("🎉 Data generation complete!")
        self.show_summary()

    def show_summary(self):
        """Show data summary"""
        try:
            stations_query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.weather_stations`"
            obs_query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.dataset_id}.weather_observations`"

            stations_count = list(self.client.query(stations_query).result())[0].count
            obs_count = list(self.client.query(obs_query).result())[0].count

            print(f"\n📊 Summary: {stations_count} stations, {obs_count:,} observations")
        except Exception as e:
            print(f"Could not get summary: {e}")

    # Helper methods
    def _get_seasonal_temp(self, date: datetime) -> float:
        """Get base temperature for season"""
        day_of_year = date.timetuple().tm_yday
        return 10 + 15 * math.sin(2 * math.pi * (day_of_year - 80) / 365)

    def _get_weather_condition(self, temp: float) -> str:
        """Get weather condition based on temperature"""
        if temp < 0:
            return random.choice(["snowy", "cloudy"])
        elif temp < 10:
            return random.choice(["cloudy", "rainy", "partly_cloudy"])
        elif temp < 20:
            return random.choice(["partly_cloudy", "cloudy", "sunny"])
        else:
            return random.choice(["sunny", "partly_cloudy"])

    def _get_precipitation(self, condition: str) -> float:
        """Get precipitation amount based on weather condition"""
        precip_map = {
            "sunny": 0,
            "partly_cloudy": random.uniform(0, 2) if random.random() < 0.3 else 0,
            "cloudy": random.uniform(0, 5) if random.random() < 0.5 else 0,
            "rainy": random.uniform(2, 20),
            "snowy": random.uniform(1, 10),
        }
        return max(0, precip_map.get(condition, 0))

    def _get_state(self, city: str) -> str:
        """Map city to German state"""
        state_map = {
            "Berlin": "Berlin", "Munich": "Bavaria", "Hamburg": "Hamburg",
            "Frankfurt": "Hesse", "Cologne": "North Rhine-Westphalia",
            "Stuttgart": "Baden-Württemberg", "Dresden": "Saxony", "Nuremberg": "Bavaria"
        }
        return state_map.get(city, "Unknown")


def main():
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id or project_id == "your-project-id":
        print("❌ Please set GOOGLE_CLOUD_PROJECT environment variable")
        print("   export GOOGLE_CLOUD_PROJECT=your-project-id")
        return

    generator = WeatherGenerator(project_id)
    generator.generate_all(num_stations=8, days=30)


if __name__ == "__main__":
    main()