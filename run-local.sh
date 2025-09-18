#!/bin/bash
# Run the Wildfire Risk Prediction API locally (without Docker)

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

echo -e "${BLUE}🚀 Starting Wildfire Risk Prediction API (Local Mode)${NC}"
echo "======================================================="

# Check requirements
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is required but not installed."
    exit 1
fi

if ! command -v gcloud &> /dev/null; then
    print_error "Google Cloud CLI is required but not installed."
    echo "   Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check for environment file
if [ ! -f ".env" ]; then
    print_warning "No .env file found. Creating from template..."
    cp .env.template .env

    # Try to auto-detect project ID
    PROJECT_AUTO=$(gcloud config get-value project 2>/dev/null || echo "")
    if [ -n "$PROJECT_AUTO" ]; then
        sed -i "s/your-project-id/$PROJECT_AUTO/g" .env
        print_success "Auto-detected project ID: $PROJECT_AUTO"
    fi

    echo "Current .env content:"
    cat .env
    echo ""
fi

# Load environment variables
source .env

# Auto-detect project ID if not set
if [ "$GCP_PROJECT_ID" = "your-project-id" ] || [ -z "$GCP_PROJECT_ID" ]; then
    PROJECT_AUTO=$(gcloud config get-value project 2>/dev/null || echo "")
    if [ -n "$PROJECT_AUTO" ]; then
        export GCP_PROJECT_ID=$PROJECT_AUTO
        sed -i "s/GCP_PROJECT_ID=.*/GCP_PROJECT_ID=$PROJECT_AUTO/g" .env
        print_success "Using auto-detected project ID: $PROJECT_AUTO"
    else
        print_error "Please set your GCP project ID in .env file or via:"
        echo "   gcloud config set project YOUR-PROJECT-ID"
        exit 1
    fi
fi

print_status "Configuration:"
echo "   Project ID: $GCP_PROJECT_ID"
echo "   Dataset ID: $DATASET_ID"
echo "   Model Name: $MODEL_NAME"
echo ""

# Check Google Cloud authentication
print_status "Checking Google Cloud authentication..."

# Check if user is logged in
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null; then
    print_error "Not authenticated with Google Cloud"
    echo "Please run: gcloud auth login"
    exit 1
fi

# Check Application Default Credentials
print_status "Checking Application Default Credentials..."
if ! gcloud auth application-default print-access-token &>/dev/null; then
    print_warning "Application Default Credentials not configured"
    print_status "Setting up Application Default Credentials..."

    if gcloud auth application-default login; then
        print_success "Application Default Credentials configured"
    else
        print_error "Failed to configure Application Default Credentials"
        exit 1
    fi
else
    print_success "Application Default Credentials are configured"
fi

# Verify BigQuery access
print_status "Verifying BigQuery access..."
if gcloud --quiet --project="$GCP_PROJECT_ID" bq ls "$DATASET_ID" &>/dev/null; then
    print_success "BigQuery access confirmed"
else
    print_warning "Could not verify BigQuery dataset access"
    print_warning "Make sure the dataset '$DATASET_ID' exists and you have access"
fi

print_success "All authentication checks passed!"
echo ""

# Install Python dependencies
print_status "Installing Python dependencies..."
if [ ! -d "venv" ]; then
    print_status "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

print_success "Dependencies installed!"

# Export environment variables
export GCP_PROJECT_ID
export DATASET_ID
export MODEL_NAME

# Start the API
print_status "Starting the API server..."
echo ""
print_success "🎉 Wildfire Risk Prediction API is starting!"
echo ""
echo "🔗 API will be available at:"
echo "   Health Check:     http://localhost:8000/health"
echo "   All Predictions:  http://localhost:8000/predictions"
echo "   State Prediction: http://localhost:8000/predictions/{state}"
echo "   Available States: http://localhost:8000/states"
echo "   Model Info:       http://localhost:8000/model/info"
echo "   API Docs:         http://localhost:8000/docs"
echo ""
echo "🛑 Press Ctrl+C to stop the server"
echo ""

# Run the API
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000