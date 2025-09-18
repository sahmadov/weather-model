#!/bin/bash
# Deployment script for Wildfire Risk Prediction API (using Application Default Credentials)

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

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Deploy Wildfire Risk Prediction API"
    echo ""
    echo "Options:"
    echo "  --local, -l     Run locally without Docker"
    echo "  --docker, -d    Run with Docker (default)"
    echo "  --help, -h      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0              # Run with Docker (default)"
    echo "  $0 --docker     # Run with Docker"
    echo "  $0 --local      # Run locally without Docker"
}

# Parse command line arguments
USE_DOCKER=true
while [[ $# -gt 0 ]]; do
    case $1 in
        --local|-l)
            USE_DOCKER=false
            shift
            ;;
        --docker|-d)
            USE_DOCKER=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

if [ "$USE_DOCKER" = true ]; then
    echo -e "${BLUE}🚀 Deploying Wildfire Risk Prediction API (Docker Mode)${NC}"
else
    echo -e "${BLUE}🚀 Starting Wildfire Risk Prediction API (Local Mode)${NC}"
fi
echo "=============================================="

# Check common requirements
if ! command -v gcloud &> /dev/null; then
    print_error "Google Cloud CLI is required but not installed."
    echo "   Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check Docker requirements if using Docker
if [ "$USE_DOCKER" = true ]; then
    if ! command -v docker &> /dev/null; then
        print_error "Docker is required for Docker mode but not installed."
        echo "   Install from: https://docs.docker.com/get-docker/"
        echo "   Or run with: $0 --local"
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is required for Docker mode but not installed."
        echo "   Install from: https://docs.docker.com/compose/install/"
        echo "   Or run with: $0 --local"
        exit 1
    fi
else
    # Check Python requirements for local mode
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is required for local mode but not installed."
        exit 1
    fi
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
echo "   Mode: $([ "$USE_DOCKER" = true ] && echo "Docker" || echo "Local")"
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

if [ "$USE_DOCKER" = true ]; then
    # Docker deployment
    print_status "Building and starting Docker container..."
    docker-compose down --remove-orphans 2>/dev/null || true

    # Export variables for docker-compose
    export GCP_PROJECT_ID
    export DATASET_ID
    export MODEL_NAME

    if docker-compose up --build -d; then
        print_success "API container started successfully!"
    else
        print_error "Failed to start container"
        echo "Check logs with: docker-compose logs wildfire-api"
        exit 1
    fi

    # Wait for service to be ready
    print_status "Waiting for API to be ready..."
    sleep 8

    # Health check
    for i in {1..12}; do
        if curl -s http://localhost:8000/health >/dev/null; then
            print_success "API is healthy and ready!"
            break
        fi

        if [ $i -eq 12 ]; then
            print_error "API failed to start properly"
            echo ""
            echo "Checking container logs..."
            docker-compose logs --tail=20 wildfire-api
            echo ""
            echo "Common issues:"
            echo "1. Check if gcloud auth application-default login is working"
            echo "2. Verify project ID is correct: $GCP_PROJECT_ID"
            echo "3. Ensure BigQuery dataset exists: $DATASET_ID"
            echo "4. Try running locally: $0 --local"
            exit 1
        fi

        print_status "Waiting... ($i/12)"
        sleep 5
    done

    STOP_COMMAND="docker-compose down"
else
    # Local deployment
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

    # Start the API in background for testing
    print_status "Starting API server..."
    python -m uvicorn main:app --host 0.0.0.0 --port 8000 &
    API_PID=$!

    # Wait for API to start
    sleep 5

    # Test if API is running
    if curl -s http://localhost:8000/health >/dev/null; then
        print_success "API started successfully!"
        # Kill the background process since we'll restart it properly
        kill $API_PID 2>/dev/null || true
        wait $API_PID 2>/dev/null || true
    else
        kill $API_PID 2>/dev/null || true
        print_error "API failed to start"
        exit 1
    fi

    STOP_COMMAND="pkill -f uvicorn"
fi

echo ""
print_success "🎉 Wildfire Risk Prediction API deployed successfully!"
echo ""
echo "🔗 API Endpoints:"
echo "   Health Check:     http://localhost:8000/health"
echo "   All Predictions:  http://localhost:8000/predictions"
echo "   State Prediction: http://localhost:8000/predictions/{state}"
echo "   Available States: http://localhost:8000/states"
echo "   Model Info:       http://localhost:8000/model/info"
echo "   API Docs:         http://localhost:8000/docs"
echo ""
echo "🧪 Test the API:"
echo "   curl http://localhost:8000/predictions"
echo "   curl http://localhost:8000/predictions/Bayern"
echo ""

if [ "$USE_DOCKER" = true ]; then
    echo "📊 View logs:"
    echo "   docker-compose logs -f wildfire-api"
    echo ""
    echo "🛑 Stop the API:"
    echo "   docker-compose down"

    # Test a quick API call
    print_status "Testing API with a quick call..."
    if response=$(curl -s http://localhost:8000/states); then
        echo "✅ API Response: $response"
    else
        print_warning "Could not test API call, but service appears to be running"
    fi
else
    echo "🔄 To run the API (in foreground):"
    echo "   source venv/bin/activate && python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000"
    echo ""
    echo "🔄 Or use the local run script:"
    echo "   chmod +x run-local.sh && ./run-local.sh"
    echo ""
    echo "🛑 Stop any background processes:"
    echo "   $STOP_COMMAND"
fi

print_success "Deployment complete! 🚀"