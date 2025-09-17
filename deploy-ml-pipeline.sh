#!/bin/bash
# Deploy ML pipeline for wildfire prediction using pure CLI approach

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID=${1:-$(gcloud config get-value project 2>/dev/null || echo "")}
DATASET_ID=${2:-weather_data}
REGION=${3:-europe-west3}
ML_MODEL_NAME=${4:-wildfire_risk_model}
TRAINING_DATA_SIZE=${5:-1000}

# Function to print colored output
print_status() {
    echo -e "${BLUE}🔵${NC} $1"
}

print_success() {
    echo -e "${GREEN}✅${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠️${NC} $1"
}

print_error() {
    echo -e "${RED}❌${NC} $1"
}

# Header
echo -e "${BLUE}🚀 Deploying ML Pipeline for Wildfire Prediction${NC}"
echo "=================================================="

# Validation
if [ -z "$PROJECT_ID" ]; then
    print_error "Please set your GCP project:"
    echo "   gcloud config set project YOUR-PROJECT-ID"
    echo "   or pass it as first argument: ./deploy-ml-pipeline.sh YOUR-PROJECT-ID"
    exit 1
fi

if ! command -v gcloud &> /dev/null; then
    print_error "Google Cloud CLI required."
    echo "   Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

if ! command -v bq &> /dev/null; then
    print_error "BigQuery CLI (bq) required."
    echo "   Usually installed with gcloud. Try: gcloud components install bq"
    exit 1
fi

print_status "Using configuration:"
echo "   Project ID: $PROJECT_ID"
echo "   Dataset ID: $DATASET_ID"
echo "   Region: $REGION"
echo "   ML Model: $ML_MODEL_NAME"
echo "   Training Data Size: $TRAINING_DATA_SIZE"
echo ""

# Check if sql directory exists
if [ ! -d "sql" ]; then
    print_error "sql directory not found. Please run from project root or ensure sql/ directory exists."
    exit 1
fi

# Prompt for confirmation
read -p "Deploy ML pipeline? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_warning "Deployment cancelled"
    exit 0
fi

echo ""
print_status "Starting deployment..."

# Step 1: Create BigQuery dataset
print_status "Creating BigQuery dataset: $DATASET_ID"
if bq ls -d $PROJECT_ID:$DATASET_ID > /dev/null 2>&1; then
    print_warning "Dataset $DATASET_ID already exists, skipping creation"
else
    bq mk \
        --dataset \
        --location=$REGION \
        --description="Weather data for wildfire ML analysis - Created $(date)" \
        --label=environment:dev \
        --label=purpose:weather-analytics \
        --label=managed-by:cli-script \
        $PROJECT_ID:$DATASET_ID

    if [ $? -eq 0 ]; then
        print_success "Dataset created successfully"
    else
        print_error "Failed to create dataset"
        exit 1
    fi
fi

# Step 2: Create training data table
print_status "Creating wildfire training data table..."

# Substitute variables in SQL file and execute
sed "s/\${project_id}/$PROJECT_ID/g; s/\${dataset_id}/$DATASET_ID/g; s/\${training_data_size}/$TRAINING_DATA_SIZE/g" \
    sql/create_wildfire_training_data.sql | \
bq query \
    --project_id=$PROJECT_ID \
    --location=$REGION \
    --use_legacy_sql=false \
    --replace

if [ $? -eq 0 ]; then
    print_success "Training data created successfully"

    # Check row count
    ROW_COUNT=$(bq query --use_legacy_sql=false --quiet --format=csv \
        "SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET_ID.wildfire_training_data\`" | tail -n 1)
    print_status "Training data contains $ROW_COUNT rows"
else
    print_error "Failed to create training data"
    exit 1
fi

# Step 3: Create and train ML model
print_status "Creating and training ML model: $ML_MODEL_NAME"
print_warning "This may take a few minutes..."

sed "s/\${project_id}/$PROJECT_ID/g; s/\${dataset_id}/$DATASET_ID/g; s/\${model_name}/$ML_MODEL_NAME/g" \
    sql/create_wildfire_model.sql | \
bq query \
    --project_id=$PROJECT_ID \
    --location=$REGION \
    --use_legacy_sql=false

if [ $? -eq 0 ]; then
    print_success "ML model trained successfully"
else
    print_error "Failed to create ML model"
    exit 1
fi

# Step 4: Generate predictions
print_status "Generating wildfire predictions..."

sed "s/\${project_id}/$PROJECT_ID/g; s/\${dataset_id}/$DATASET_ID/g; s/\${model_name}/$ML_MODEL_NAME/g" \
    sql/create_predictions.sql | \
bq query \
    --project_id=$PROJECT_ID \
    --location=$REGION \
    --use_legacy_sql=false \
    --replace

if [ $? -eq 0 ]; then
    print_success "Predictions generated successfully"

    # Check prediction count
    PRED_COUNT=$(bq query --use_legacy_sql=false --quiet --format=csv \
        "SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET_ID.next_day_wildfire_predictions\`" | tail -n 1)
    print_status "Generated $PRED_COUNT predictions"
else
    print_error "Failed to generate predictions"
    exit 1
fi

# Step 5: Evaluate model performance
print_status "Evaluating model performance..."

bq query \
    --project_id=$PROJECT_ID \
    --use_legacy_sql=false \
    --format=prettyjson \
    "SELECT * FROM ML.EVALUATE(MODEL \`$PROJECT_ID.$DATASET_ID.$ML_MODEL_NAME\`)" > model_evaluation.json

if [ $? -eq 0 ]; then
    print_success "Model evaluation completed (saved to model_evaluation.json)"

    # Extract key metrics if possible
    if command -v jq &> /dev/null; then
        ACCURACY=$(jq -r '.[0].accuracy // "N/A"' model_evaluation.json)
        AUC=$(jq -r '.[0].roc_auc // "N/A"' model_evaluation.json)
        echo "   Accuracy: $ACCURACY"
        echo "   ROC AUC: $AUC"
    fi
else
    print_warning "Could not evaluate model performance"
fi

# Step 6: Show sample predictions
print_status "Sample predictions:"
bq query \
    --project_id=$PROJECT_ID \
    --use_legacy_sql=false \
    --format=pretty \
    --max_rows=5 \
    "SELECT
        station_id,
        state_province,
        temperature_celsius,
        humidity_percent,
        predicted_wildfire_risk_label,
        predicted_wildfire_risk_label_probs[OFFSET(0)].prob as high_risk_probability
     FROM \`$PROJECT_ID.$DATASET_ID.next_day_wildfire_predictions\`
     ORDER BY predicted_wildfire_risk_label_probs[OFFSET(0)].prob DESC"

echo ""
echo "=================================================="
print_success "🎉 ML Pipeline deployed successfully!"
echo ""
echo "📊 Resources created:"
echo "   • Dataset: $DATASET_ID"
echo "   • Training data: wildfire_training_data ($ROW_COUNT rows)"
echo "   • ML model: $ML_MODEL_NAME"
echo "   • Predictions: next_day_wildfire_predictions ($PRED_COUNT rows)"
echo ""
echo "🔍 Next steps:"
echo "   1. View in BigQuery Console:"
echo "      https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
echo ""
echo "   2. Check model performance:"
echo "      cat model_evaluation.json"
echo ""
echo "   3. Query predictions:"
echo "      bq query --use_legacy_sql=false \\"
echo "        \"SELECT * FROM \\\`$PROJECT_ID.$DATASET_ID.next_day_wildfire_predictions\\\` LIMIT 10\""
echo ""
echo "   4. Make new predictions:"
echo "      Update sql/create_predictions.sql with new data and re-run step 4"
echo ""
print_success "Deployment complete! 🚀"