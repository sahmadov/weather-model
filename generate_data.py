#!/usr/bin/env python3
"""
Simple batch script for weather data generation
"""
import argparse
import sys
import config
from weather_generator import WeatherGenerator


def main():
    parser = argparse.ArgumentParser(description="Generate weather data")
    parser.add_argument("--project", default=config.PROJECT_ID, help="GCP Project ID")
    parser.add_argument("--dataset", default=config.DATASET_ID, help="BigQuery Dataset ID")
    parser.add_argument("--stations", type=int, default=config.DEFAULT_STATIONS, help="Number of stations")
    parser.add_argument("--days", type=int, default=config.DEFAULT_DAYS, help="Days of data")

    # Commands
    parser.add_argument("--clear", action="store_true", help="Clear all data first")
    parser.add_argument("--summary", action="store_true", help="Show data summary only")

    args = parser.parse_args()

    if not args.project or args.project == "your-project-id":
        print("❌ Please set project ID: --project YOUR_PROJECT_ID")
        return

    generator = WeatherGenerator(args.project, args.dataset)

    if args.summary:
        generator.show_summary()
        return

    if args.clear:
        confirm = input("Clear all data? (yes/no): ")
        if confirm.lower() == "yes":
            clear_data(generator)

    generator.generate_all(args.stations, args.days)


def clear_data(generator):
    """Clear all data from tables"""
    try:
        # Clear observations first
        query1 = f"DELETE FROM `{generator.project_id}.{generator.dataset_id}.weather_observations` WHERE TRUE"
        generator.client.query(query1).result()
        print("✅ Cleared observations")

        # Clear stations
        query2 = f"DELETE FROM `{generator.project_id}.{generator.dataset_id}.weather_stations` WHERE TRUE"
        generator.client.query(query2).result()
        print("✅ Cleared stations")

    except Exception as e:
        print(f"❌ Error clearing data: {e}")


if __name__ == "__main__":
    main()