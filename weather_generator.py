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
from datetime import datetime, timedelta, date



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

        for row in data:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
                elif isinstance(value, date):
                    row[key] = value.isoformat()

        # Insert data
        job = self.client.load_table_from_json(data, table)
        job.result()
        print(f"✅ Inserted {len(data)} rows into {table_name}")

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
        """Generate and insert all data including fire records"""
        print(f"🏭 Creating {num_stations} weather stations...")
        stations = self.create_stations(num_stations)
        self.insert_to_bigquery("weather_stations", stations)

        print(f"🌤️  Generating {days} days of weather observations...")
        observations = self.create_observations(stations, days)
        self.insert_to_bigquery("weather_observations", observations)

        print(f"🔥 Generating fire records for {days} days...")
        fire_records = self.create_fire_records(stations, days)
        if fire_records:
            self.insert_to_bigquery("fire_records", fire_records)
            print(f"✅ Generated {len(fire_records)} fire records")
        else:
            print("ℹ️  No fire records generated (low risk conditions)")

        print("🎉 Data generation complete!")

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

    def create_fire_records(self, stations: List[Dict], days: int = 30) -> List[Dict]:
        """Create fire records based on weather conditions and geographic patterns"""
        fire_records = []
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # German fire-prone regions (simplified)
        high_risk_regions = {
            "Brandenburg": {"base_risk": 0.15},
            "Lower Saxony": {"base_risk": 0.12},
            "Bavaria": {"base_risk": 0.10},
            "Saxony": {"base_risk": 0.08},
            "Hesse": {"base_risk": 0.07},
            "North Rhine-Westphalia": {"base_risk": 0.06},
            "Baden-Württemberg": {"base_risk": 0.06}
        }

        fire_id_counter = 1

        # Generate fires based on weather patterns
        current_date = start_date
        while current_date < end_date:
            for station in stations:
                # Get fire risk probability for this region
                state = station.get("state_province", "Unknown")
                base_risk = high_risk_regions.get(state, {"base_risk": 0.05})["base_risk"]

                # Higher risk in summer months
                month = current_date.month
                seasonal_multiplier = self._get_seasonal_fire_multiplier(month)

                # Check weather conditions for fire risk
                weather_multiplier = self._calculate_fire_weather_risk(current_date, station)

                # Combined risk probability
                daily_fire_risk = base_risk * seasonal_multiplier * weather_multiplier

                # Generate fire if conditions are met (random chance)
                if random.random() < daily_fire_risk:
                    fire_record = self._generate_single_fire_record(
                        fire_id_counter, station, current_date
                    )
                    fire_records.append(fire_record)
                    fire_id_counter += 1

            current_date += timedelta(days=1)

        return fire_records

    def _get_seasonal_fire_multiplier(self, month: int) -> float:
        """Get seasonal fire risk multiplier"""
        # Higher risk in late spring/summer/early fall
        seasonal_risks = {
            1: 0.3, 2: 0.4, 3: 0.7, 4: 1.2, 5: 1.8, 6: 2.2,
            7: 2.5, 8: 2.8, 9: 2.0, 10: 1.3, 11: 0.6, 12: 0.4
        }
        return seasonal_risks.get(month, 1.0)

    def _calculate_fire_weather_risk(self, date: datetime, station: Dict) -> float:
        """Calculate fire weather risk multiplier based on conditions"""
        # Simulate weather conditions for this date/station
        temp = self._get_seasonal_temp(date) + random.gauss(0, 5)
        humidity = random.uniform(30, 80)
        wind_speed = random.lognormvariate(2, 0.5)
        days_since_rain = random.randint(0, 20)  # Simplified

        risk_multiplier = 1.0

        # High temperature increases risk
        if temp > 25:
            risk_multiplier *= 1.5
        if temp > 30:
            risk_multiplier *= 2.0

        # Low humidity increases risk
        if humidity < 30:
            risk_multiplier *= 2.0
        elif humidity < 50:
            risk_multiplier *= 1.3

        # High wind increases risk
        if wind_speed > 10:
            risk_multiplier *= 1.4
        if wind_speed > 15:
            risk_multiplier *= 2.0

        # Dry spell increases risk
        if days_since_rain > 10:
            risk_multiplier *= 1.6
        if days_since_rain > 20:
            risk_multiplier *= 2.5

        return min(risk_multiplier, 10.0)  # Cap the multiplier

    def _generate_single_fire_record(self, fire_id: int, station: Dict, fire_date: datetime) -> Dict:
        """Generate a single fire record"""
        # Create location near the weather station (within ~50km radius)
        lat_offset = random.uniform(-0.5, 0.5)  # ~55km at this latitude
        lon_offset = random.uniform(-0.5, 0.5)

        # Extract station coordinates from POINT string
        station_coords = station["location"].replace("POINT(", "").replace(")", "").split()
        base_lon = float(station_coords[0])
        base_lat = float(station_coords[1])

        fire_location = f"POINT({base_lon + lon_offset} {base_lat + lat_offset})"

        # Fire characteristics
        fire_causes = ["lightning", "human", "equipment", "arson", "unknown"]
        fire_statuses = ["contained", "controlled", "out"]

        # Fire size (log-normal distribution, most fires are small)
        fire_size = max(0.1, random.lognormvariate(1, 1.5))  # hectares

        # Containment date (1-14 days after start)
        containment_days = random.randint(1, 14)
        containment_date = fire_date + timedelta(days=containment_days)

        # Generate fire name
        area_names = ["Forest", "Hill", "Ridge", "Valley", "Creek", "Lake"]
        fire_name = f"{station['city']} {random.choice(area_names)} Fire {fire_id}"

        return {
            "fire_id": f"FIRE_{fire_id:04d}",
            "fire_name": fire_name,
            "location": fire_location,
            "fire_date": fire_date.date(),
            "fire_size_hectares": round(fire_size, 2),
            "cause": random.choice(fire_causes),
            "containment_date": containment_date.date(),
            "fire_status": random.choice(fire_statuses),
            "country": "Germany",
            "state_province": station.get("state_province", "Unknown"),
            "city": station["city"],
            "created_at": datetime.now(),
            "updated_at": None
        }

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