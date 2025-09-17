#!/usr/bin/env python3
"""
Batch Weather Data Generator
Provides different options for generating weather data
"""

import argparse
import sys
import os
import random
import uuid
from datetime import datetime, timedelta
from generate_weather_data import WeatherDataGenerator
import config


def generate_stations_only(generator, num_stations):
    """Generate only weather stations"""
    print(f"Generating {num_stations} weather stations...")
    stations = generator.generate_weather_stations(num_stations)
    generator.insert_data_to_bigquery("weather_stations", stations)
    print("Weather stations inserted successfully!")


def generate_observations_only(generator, days, obs_per_day):
    """Generate observations for existing stations"""
    from google.cloud import bigquery

    # Get existing stations
    query = f"""
    SELECT station_id, station_name, location, elevation_meters, country, 
           state_province, city, active, created_at, updated_at
    FROM `{generator.project_id}.{generator.dataset_id}.weather_stations`
    WHERE active = true
    """

    try:
        query_job = generator.client.query(query)
        results = query_job.result()

        stations = []
        for row in results:
            station = {
                "station_id": row.station_id,
                "station_name": row.station_name,
                "location": f"POINT({row.location})" if not str(row.location).startswith("POINT") else str(
                    row.location),
                "elevation_meters": row.elevation_meters,
                "country": row.country,
                "state_province": row.state_province,
                "city": row.city,
                "active": row.active,
                "created_at": row.created_at,
                "updated_at": row.updated_at
            }
            stations.append(station)

        if not stations:
            print("No active weather stations found. Please generate stations first.")
            return

        print(f"Found {len(stations)} active weather stations")
        print(f"Generating {days} days of observations with {obs_per_day} observations per day...")

        observations = generator.generate_weather_observations(
            stations, days=days, observations_per_day=obs_per_day
        )

        generator.insert_data_to_bigquery("weather_observations", observations)
        print("Weather observations inserted successfully!")

    except Exception as e:
        print(f"Error querying existing stations: {e}")
        print("Make sure the weather_stations table exists and contains data.")


def generate_historical_data(generator, start_date_str, end_date_str, obs_per_day):
    """Generate data for a specific date range"""
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        if start_date >= end_date:
            print("Start date must be before end date")
            return

        days = (end_date - start_date).days
        print(f"Generating data from {start_date_str} to {end_date_str} ({days} days)")

        # Get existing stations first
        from google.cloud import bigquery

        query = f"""
        SELECT station_id, station_name, location, elevation_meters, country, 
               state_province, city, active, created_at, updated_at
        FROM `{generator.project_id}.{generator.dataset_id}.weather_stations`
        WHERE active = true
        """

        try:
            query_job = generator.client.query(query)
            results = query_job.result()

            stations = []
            for row in results:
                station = {
                    "station_id": row.station_id,
                    "station_name": row.station_name,
                    "location": f"POINT({row.location})" if not str(row.location).startswith("POINT") else str(
                        row.location),
                    "elevation_meters": row.elevation_meters,
                    "country": row.country,
                    "state_province": row.state_province,
                    "city": row.city,
                    "active": row.active,
                    "created_at": row.created_at,
                    "updated_at": row.updated_at
                }
                stations.append(station)

            if not stations:
                print("No active weather stations found. Please generate stations first.")
                return

            # Create a custom generator that uses the specific date range
            observations = []
            for station in stations:
                if not station["active"]:
                    continue

                station_id = station["station_id"]
                current_date = start_date

                while current_date < end_date:
                    seasonal_params = generator.get_seasonal_params(current_date)
                    base_temp = seasonal_params["base_temp"]

                    # Add elevation adjustment
                    elevation = station["elevation_meters"]
                    base_temp -= (elevation / 1000) * 6.5

                    for hour in range(0, 24, 24 // obs_per_day):
                        if random.random() < 0.05:  # 5% missing data
                            continue

                        observation_time = current_date + timedelta(hours=hour)

                        # Daily temperature variation
                        import math
                        hour_temp_adjustment = 5 * math.sin(math.pi * (hour - 6) / 12)
                        temp = base_temp + hour_temp_adjustment + random.gauss(0, seasonal_params["temp_variance"])

                        # Generate weather condition
                        condition = generator.generate_weather_condition(temp, seasonal_params["season"])
                        condition_params = generator.weather_conditions[condition]

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

            print(f"Generated {len(observations)} observations for date range")
            generator.insert_data_to_bigquery("weather_observations", observations)
            print("Historical data inserted successfully!")

        except Exception as e:
            print(f"Error generating historical data: {e}")

    except ValueError as e:
        print(f"Invalid date format. Please use YYYY-MM-DD format. Error: {e}")


def clear_all_data(generator):
    """Clear all data from both tables"""
    confirm = input(
        "This will delete ALL data from weather_stations and weather_observations tables. Are you sure? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Operation cancelled")
        return

    try:
        # Clear observations first (foreign key dependency)
        query1 = f"DELETE FROM `{generator.project_id}.{generator.dataset_id}.weather_observations` WHERE TRUE"
        job1 = generator.client.query(query1)
        job1.result()
        print("Cleared all weather observations")

        # Clear stations
        query2 = f"DELETE FROM `{generator.project_id}.{generator.dataset_id}.weather_stations` WHERE TRUE"
        job2 = generator.client.query(query2)
        job2.result()
        print("Cleared all weather stations")

    except Exception as e:
        print(f"Error clearing data: {e}")


def show_data_summary(generator):
    """Show summary of existing data"""
    try:
        # Count stations
        stations_query = f"SELECT COUNT(*) as count, COUNT(*) FILTER (WHERE active = true) as active_count FROM `{generator.project_id}.{generator.dataset_id}.weather_stations`"
        stations_job = generator.client.query(stations_query)
        stations_result = list(stations_job.result())[0]

        # Count observations
        obs_query = f"""
        SELECT 
            COUNT(*) as total_observations,
            COUNT(DISTINCT station_id) as stations_with_data,
            MIN(timestamp) as earliest_observation,
            MAX(timestamp) as latest_observation
        FROM `{generator.project_id}.{generator.dataset_id}.weather_observations`
        """
        obs_job = generator.client.query(obs_query)
        obs_result = list(obs_job.result())[0]

        print("\n=== Data Summary ===")
        print(f"Weather Stations: {stations_result.count} total, {stations_result.active_count} active")
        print(f"Weather Observations: {obs_result.total_observations}")
        print(f"Stations with data: {obs_result.stations_with_data}")
        if obs_result.earliest_observation:
            print(f"Date range: {obs_result.earliest_observation} to {obs_result.latest_observation}")
        print("===================\n")

    except Exception as e:
        print(f"Error getting data summary: {e}")


def main():
    parser = argparse.ArgumentParser(description="Batch Weather Data Generator")
    parser.add_argument("--project", default=config.PROJECT_ID, help="GCP Project ID")
    parser.add_argument("--dataset", default=config.DATASET_ID, help="BigQuery Dataset ID")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Generate all data
    all_parser = subparsers.add_parser("all", help="Generate stations and observations")
    all_parser.add_argument("--stations", type=int, default=config.NUM_STATIONS, help="Number of stations")
    all_parser.add_argument("--days", type=int, default=config.DAYS_OF_DATA, help="Days of data")
    all_parser.add_argument("--obs-per-day", type=int, default=config.OBSERVATIONS_PER_DAY, help="Observations per day")

    # Generate stations only
    stations_parser = subparsers.add_parser("stations", help="Generate weather stations only")
    stations_parser.add_argument("--count", type=int, default=config.NUM_STATIONS, help="Number of stations")

    # Generate observations only
    obs_parser = subparsers.add_parser("observations", help="Generate observations for existing stations")
    obs_parser.add_argument("--days", type=int, default=config.DAYS_OF_DATA, help="Days of data")
    obs_parser.add_argument("--obs-per-day", type=int, default=config.OBSERVATIONS_PER_DAY, help="Observations per day")

    # Generate historical data
    hist_parser = subparsers.add_parser("historical", help="Generate data for specific date range")
    hist_parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    hist_parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    hist_parser.add_argument("--obs-per-day", type=int, default=config.OBSERVATIONS_PER_DAY,
                             help="Observations per day")

    # Clear data
    subparsers.add_parser("clear", help="Clear all data from tables")

    # Clean up duplicates
    subparsers.add_parser("cleanup", help="Remove duplicate weather stations")

    # Show summary
    subparsers.add_parser("summary", help="Show data summary")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    if args.project == "your-project-id-here":
        print("Please set the GOOGLE_CLOUD_PROJECT environment variable or specify --project")
        return

    # Initialize generator
    generator = WeatherDataGenerator(args.project, args.dataset)

    # Execute command
    if args.command == "all":
        generator.generate_and_insert_all_data(args.stations, args.days, args.obs_per_day)
    elif args.command == "stations":
        generate_stations_only(generator, args.count)
    elif args.command == "observations":
        generate_observations_only(generator, args.days, args.obs_per_day)
    elif args.command == "historical":
        generate_historical_data(generator, args.start_date, args.end_date, args.obs_per_day)
    elif args.command == "clear":
        clear_all_data(generator)
    elif args.command == "cleanup":
        generator.cleanup_duplicate_stations()
    elif args.command == "summary":
        show_data_summary(generator)


if __name__ == "__main__":
    main()