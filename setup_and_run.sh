#!/bin/bash

# Setup and run weather data generation script
set -e

echo "Setting up Weather Data Generator..."

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is required but not installed. Please install Python 3."
    exit 1
fi

# Check if gcloud CLI is installed and authenticated
if ! command -v gcloud &> /dev/null; then
    echo "Google Cloud CLI is required but not installed."
    echo "Please install it from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check authentication
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n 1 > /dev/null; then
    echo "Please authenticate with Google Cloud:"
    echo "gcloud auth login"
    echo "gcloud auth application-default login"
    exit 1
fi

# Get project ID
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT_ID" ]; then
    echo "No default project set. Please set it with:"
    echo "gcloud config set project YOUR-PROJECT-ID"
    exit 1
fi

echo "Using project: $PROJECT_ID"

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install requirements
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Set environment variable
export GOOGLE_CLOUD_PROJECT=$PROJECT_ID

# Run the data generator
echo "Running weather data generator..."
python generate_weather_data.py

echo "Setup and execution completed!"