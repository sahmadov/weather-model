#!/bin/bash
# Deploy ML pipeline for wildfire prediction

set -e

echo "🚀 Deploying ML Pipeline for Wildfire Prediction..."

# Check requirements
if ! command -v terraform &> /dev/null; then
    echo "❌ Terraform required. Please install Terraform."
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

# Check if terraform directory exists
if [ ! -d "terraform" ]; then
    echo "❌ terraform directory not found. Please run from project root."
    exit 1
fi

cd terraform

# Create SQL directory if it doesn't exist
mkdir -p sql

echo "🏗️  Initializing Terraform..."
terraform init

echo "📝 Planning deployment..."
terraform plan -var="project_id=$PROJECT_ID"

echo ""
read -p "Deploy infrastructure and ML models? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🚀 Deploying infrastructure..."
    terraform apply -var="project_id=$PROJECT_ID" -auto-approve

    echo ""
    echo "✅ ML Pipeline deployed successfully!"
    echo ""
    echo "📊 Created resources:"
    echo "   - BigQuery dataset: weather_data"
    echo "   - Training data table: wildfire_training_data"
    echo "   - ML model: synthetic_wildfire_risk_model"
    echo "   - Predictions table: next_day_wildfire_predictions"
    echo ""
    echo "🔍 Next steps:"
    echo "   1. View training data: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
    echo "   2. Check model performance in BigQuery ML"
    echo "   3. Review predictions in next_day_wildfire_predictions table"
else
    echo "❌ Deployment cancelled"
fi

cd ..