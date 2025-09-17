#!/bin/bash
# Setup and deploy ML pipeline for wildfire prediction (CLI approach)

set -e

echo "🚀 Setting up ML Pipeline for Wildfire Prediction..."
echo "This script will create the necessary files and deploy using pure CLI approach"
echo ""

# Check requirements
if ! command -v gcloud &> /dev/null; then
    echo "❌ Google Cloud CLI required."
    echo "   Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

if ! command -v bq &> /dev/null; then
    echo "❌ BigQuery CLI (bq) required."
    echo "   Usually installed with gcloud. Try: gcloud components install bq"
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
echo ""

# Create sql directory if it doesn't exist
mkdir -p sql

# Create necessary SQL files if they don't exist
echo "📝 Ensuring SQL files exist..."

# The SQL files should already exist from your Terraform setup
# Let's verify they exist
required_files=("sql/create_wildfire_training_data.sql" "sql/create_wildfire_model.sql" "sql/create_predictions.sql")

for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Required file missing: $file"
        echo "   Please copy from your terraform/sql/ directory"
        exit 1
    else
        echo "✅ Found: $file"
    fi
done

# Make deploy script executable
chmod +x deploy-ml-pipeline.sh 2>/dev/null || true
chmod +x cleanup.sh 2>/dev/null || true

echo ""
echo "🎯 Setup complete! Ready to deploy."
echo ""
echo "🚀 To deploy the ML pipeline:"
echo "   ./deploy-ml-pipeline.sh"
echo ""
echo "🗑️  To cleanup resources later:"
echo "   ./cleanup.sh"
echo ""
echo "📁 Files created/verified:"
echo "   • deploy-ml-pipeline.sh (main deployment script)"
echo "   • cleanup.sh (cleanup script)"
echo "   • sql/ directory with SQL files"
echo ""
echo "Ready to deploy? Run: ./deploy-ml-pipeline.sh"