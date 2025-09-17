#!/bin/bash
# Simple setup script for weather data generator

set -e

echo "🚀 Setting up Weather Data Generator..."

# Check requirements
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 required. Please install Python 3."
    exit 1
fi

if ! command -v gcloud &> /dev/null; then
    echo "❌ Google Cloud CLI required."
    echo "   Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check project
PROJECT_ID=$(gcloud config get-value project 2>/dev/null || echo "")
if [ -z "$PROJECT_ID" ]; then
    echo "❌ Please set your GCP project:"
    echo "   gcloud config set project YOUR-PROJECT-ID"
    exit 1
fi

echo "📋 Using project: $PROJECT_ID"

# Setup Python environment
if [ ! -d "venv" ]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
echo "📦 Installing dependencies..."
pip install -q -r requirements.txt

# Export project
export GOOGLE_CLOUD_PROJECT=$PROJECT_ID

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Deploy infrastructure: cd terraform && terraform apply"
echo "2. Generate data: python generate_data.py"
echo "3. Or run directly: python weather_generator.py"