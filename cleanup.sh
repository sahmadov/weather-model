#!/bin/bash
# Cleanup ML pipeline resources

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

echo -e "${YELLOW}🗑️  Cleaning up ML Pipeline Resources${NC}"
echo "==========================================="

if [ -z "$PROJECT_ID" ]; then
    print_error "Please set your GCP project or pass it as first argument"
    exit 1
fi

print_status "Project: $PROJECT_ID"
print_status "Dataset: $DATASET_ID"
echo ""

print_warning "This will DELETE the following resources:"
echo "   • BigQuery dataset: $DATASET_ID"
echo "   • All tables and models within the dataset"
echo "   • Local evaluation files"
echo ""

read -p "Are you sure you want to delete these resources? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_warning "Cleanup cancelled"
    exit 0
fi

# Delete BigQuery dataset (this removes all tables and models)
print_status "Deleting BigQuery dataset and all contents..."
if bq rm -r -f $PROJECT_ID:$DATASET_ID; then
    print_success "Dataset deleted successfully"
else
    print_error "Failed to delete dataset (it may not exist)"
fi

# Remove local files
print_status "Cleaning up local files..."
if [ -f "model_evaluation.json" ]; then
    rm model_evaluation.json
    print_success "Removed model_evaluation.json"
fi

print_success "Cleanup complete! 🧹"