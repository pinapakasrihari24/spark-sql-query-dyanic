#!/bin/bash

##############################################################################
# Cloud Composer Deployment Script for run_sql_query_dag
# This script automates the deployment of the Spark SQL DAG to Cloud Composer
##############################################################################

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
print_info "Checking prerequisites..."

if ! command_exists gcloud; then
    print_error "gcloud CLI is not installed. Please install it first."
    exit 1
fi

if ! command_exists gsutil; then
    print_error "gsutil is not installed. Please install Google Cloud SDK."
    exit 1
fi

print_info "Prerequisites check passed!"

# Get user inputs
echo ""
echo "==============================================="
echo "Cloud Composer Deployment Configuration"
echo "==============================================="
echo ""

read -p "Enter your GCP Project ID: " PROJECT_ID
read -p "Enter Cloud Composer environment name: " COMPOSER_ENV
read -p "Enter Composer environment region: " COMPOSER_REGION
read -p "Enter GCS bucket name (without gs://): " GCS_BUCKET
read -p "Enter Dataproc region: " DATAPROC_REGION
read -p "Enter Dataproc cluster name: " CLUSTER_NAME

echo ""
read -p "Use permanent cluster? (y/n, default: y): " USE_PERMANENT
USE_PERMANENT=${USE_PERMANENT:-y}

# Set the active project
print_info "Setting active GCP project to: $PROJECT_ID"
gcloud config set project "$PROJECT_ID"

# Step 1: Upload PySpark script to GCS
echo ""
print_info "Step 1: Uploading spark_sql_runner.py to GCS..."

if [ ! -f "spark_sql_runner.py" ]; then
    print_error "spark_sql_runner.py not found in current directory!"
    exit 1
fi

gsutil cp spark_sql_runner.py "gs://${GCS_BUCKET}/scripts/"
if [ $? -eq 0 ]; then
    print_info "Successfully uploaded spark_sql_runner.py"
else
    print_error "Failed to upload spark_sql_runner.py"
    exit 1
fi

# Step 2: Set Airflow Variables
echo ""
print_info "Step 2: Setting Airflow variables..."

print_info "Setting gcp_project_id..."
gcloud composer environments run "$COMPOSER_ENV" \
    --location "$COMPOSER_REGION" \
    variables set -- gcp_project_id "$PROJECT_ID" 2>/dev/null || print_warning "Variable may already exist"

print_info "Setting dataproc_region..."
gcloud composer environments run "$COMPOSER_ENV" \
    --location "$COMPOSER_REGION" \
    variables set -- dataproc_region "$DATAPROC_REGION" 2>/dev/null || print_warning "Variable may already exist"

print_info "Setting dataproc_cluster_name..."
gcloud composer environments run "$COMPOSER_ENV" \
    --location "$COMPOSER_REGION" \
    variables set -- dataproc_cluster_name "$CLUSTER_NAME" 2>/dev/null || print_warning "Variable may already exist"

print_info "Setting gcs_bucket..."
gcloud composer environments run "$COMPOSER_ENV" \
    --location "$COMPOSER_REGION" \
    variables set -- gcs_bucket "$GCS_BUCKET" 2>/dev/null || print_warning "Variable may already exist"

# Step 3: Upload DAG file
echo ""
print_info "Step 3: Uploading DAG to Cloud Composer..."

# Get DAGs folder
print_info "Getting Composer DAGs folder..."
DAGS_FOLDER=$(gcloud composer environments describe "$COMPOSER_ENV" \
    --location "$COMPOSER_REGION" \
    --format="get(config.dagGcsPrefix)")

if [ -z "$DAGS_FOLDER" ]; then
    print_error "Failed to get Composer DAGs folder"
    exit 1
fi

print_info "DAGs folder: $DAGS_FOLDER"

# Choose which DAG version to upload
if [[ "$USE_PERMANENT" == "y" || "$USE_PERMANENT" == "Y" ]]; then
    DAG_FILE="run_sql_query_dag_permanent_cluster.py"
    print_info "Using permanent cluster version"
else
    DAG_FILE="run_sql_query_dag.py"
    print_info "Using ephemeral cluster version"
fi

if [ ! -f "$DAG_FILE" ]; then
    print_error "$DAG_FILE not found in current directory!"
    exit 1
fi

print_info "Uploading $DAG_FILE to Composer..."
gsutil cp "$DAG_FILE" "${DAGS_FOLDER}/run_sql_query_dag.py"

if [ $? -eq 0 ]; then
    print_info "Successfully uploaded DAG file"
else
    print_error "Failed to upload DAG file"
    exit 1
fi

# Step 4: Wait for DAG to appear
echo ""
print_info "Waiting for DAG to appear in Airflow (this may take 1-2 minutes)..."
sleep 30

# Step 5: Get Airflow UI URL
echo ""
print_info "Getting Airflow UI URL..."
AIRFLOW_URL=$(gcloud composer environments describe "$COMPOSER_ENV" \
    --location "$COMPOSER_REGION" \
    --format="get(config.airflowUri)")

# Summary
echo ""
echo "==============================================="
echo "Deployment Summary"
echo "==============================================="
echo ""
print_info "✓ PySpark script uploaded to: gs://${GCS_BUCKET}/scripts/spark_sql_runner.py"
print_info "✓ Airflow variables configured"
print_info "✓ DAG uploaded to Composer"
echo ""
echo "Airflow UI: $AIRFLOW_URL"
echo "DAG Name: run_sql_query_dag"
echo ""
echo "==============================================="
echo "Next Steps:"
echo "==============================================="
echo "1. Go to Airflow UI: $AIRFLOW_URL"
echo "2. Find 'run_sql_query_dag' in the DAGs list"
echo "3. Toggle the DAG to ON"
echo "4. Trigger the DAG with configuration:"
echo ""
echo '   {"sql_query": "SELECT * FROM your_table"}'
echo ""
echo "==============================================="
echo "Test Command:"
echo "==============================================="
echo ""
echo "gcloud composer environments run $COMPOSER_ENV \\"
echo "    --location $COMPOSER_REGION \\"
echo "    dags trigger -- \\"
echo "    run_sql_query_dag \\"
echo "    --conf '{\"sql_query\": \"SELECT 1 as test\"}'"
echo ""
echo "==============================================="

print_info "Deployment completed successfully!"
