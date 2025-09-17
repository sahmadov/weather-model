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

    # Only keep clear parameter
    parser.add_argument("--clear", action="store_true", help="Clear all data first")

    args = parser.parse_args()

    if not args.project or args.project == "your-project-id":
        print("❌ Please set project ID: --project YOUR_PROJECT_ID")
        return

    generator = WeatherGenerator(args.project, args.dataset)

    if args.clear:
        confirm = input("Clear all data? (yes/no): ")
        if confirm.lower() == "yes":
            clear_data(generator)
            print("✅ Data cleared. Run again without --clear to generate fresh data.")
            return

    # Generate default data
    generator.generate_all()


def clear_data(generator):
    """Clear all data from tables"""
    try:
        # Clear in dependency order
        tables_to_clear = ["weather_observations", "fire_records", "weather_stations"]

        for table in tables_to_clear:
            try:
                query = f"DELETE FROM `{generator.project_id}.{generator.dataset_id}.{table}` WHERE TRUE"
                generator.client.query(query).result()
                print(f"✅ Cleared {table}")
            except Exception as e:
                if "not found" not in str(e).lower():
                    print(f"⚠️  Could not clear {table}: {e}")

    except Exception as e:
        print(f"❌ Error clearing data: {e}")


if __name__ == "__main__":
    main()