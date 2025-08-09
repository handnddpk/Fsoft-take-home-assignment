#!/bin/bash

# Customer Transaction ETL Pipeline Setup Script
# This script sets up and runs the ETL pipeline in different modes

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check if Docker Compose is installed
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check if Python is installed (for local execution)
    if ! command -v python3 &> /dev/null; then
        print_warning "Python 3 is not installed. Docker execution only."
    fi
    
    print_success "Prerequisites check completed"
}

# Function to run ETL pipeline locally
run_local() {
    print_status "Running ETL pipeline locally..."
    
    # Check if virtual environment exists
    if [ ! -d "venv" ]; then
        print_status "Creating virtual environment..."
        python3 -m venv venv
    fi
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Install dependencies
    print_status "Installing dependencies..."
    pip install -r requirements.txt
    
    # Run the ETL pipeline
    print_status "Executing ETL pipeline..."
    cd src
    python etl_pipeline.py
    cd ..
    
    print_success "ETL pipeline completed successfully!"
    print_status "Database created at: data/output/retail_data.db"
}

# Function to run ETL pipeline with Docker
run_docker() {
    print_status "Running ETL pipeline with Docker..."
    
    # Build the Docker image
    print_status "Building Docker image..."
    docker-compose build etl-pipeline
    
    # Run the ETL pipeline
    print_status "Executing ETL pipeline in Docker container..."
    docker-compose run --rm etl-pipeline
    
    print_success "ETL pipeline completed successfully!"
    print_status "Database created at: data/output/retail_data.db"
}

# Function to start Airflow
run_airflow() {
    print_status "Starting Airflow environment..."
    
    # Set Airflow UID
    export AIRFLOW_UID=$(id -u)
    
    # Initialize Airflow database and create admin user
    print_status "Initializing Airflow database..."
    docker-compose up airflow-init
    
    # Start Airflow services
    print_status "Starting Airflow services..."
    docker-compose up -d airflow-webserver airflow-scheduler airflow-triggerer
    
    print_success "Airflow started successfully!"
    print_status "Airflow UI available at: http://localhost:8080"
    print_status "Username: admin, Password: admin"
    print_status "The ETL DAG will run daily at 6:00 AM"
}

# Function to stop Airflow
stop_airflow() {
    print_status "Stopping Airflow environment..."
    docker-compose down
    print_success "Airflow stopped successfully!"
}

# Function to view logs
view_logs() {
    print_status "Viewing Docker Compose logs..."
    docker-compose logs -f
}

# Function to clean up
cleanup() {
    print_status "Cleaning up Docker resources..."
    docker-compose down -v --remove-orphans
    docker system prune -f
    print_success "Cleanup completed!"
}

# Function to run data quality checks
check_data_quality() {
    print_status "Running data quality checks..."
    
    if [ ! -f "data/output/retail_data.db" ]; then
        print_error "Database not found. Please run the ETL pipeline first."
        exit 1
    fi
    
    # Simple SQLite query to check data
    sqlite3 data/output/retail_data.db <<EOF
.headers on
.mode column

SELECT 'Customers' as table_name, COUNT(*) as record_count FROM customers
UNION ALL
SELECT 'Products' as table_name, COUNT(*) as record_count FROM products
UNION ALL
SELECT 'Transactions' as table_name, COUNT(*) as record_count FROM transactions
UNION ALL
SELECT 'Customer Revenue' as table_name, COUNT(*) as record_count FROM customer_revenue;

SELECT 'Total Revenue: $' || ROUND(SUM(total_amount), 2) as summary FROM customer_revenue;
EOF
    
    print_success "Data quality check completed!"
}

# Function to show usage
show_usage() {
    echo "Customer Transaction ETL Pipeline"
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  local       Run ETL pipeline locally with Python"
    echo "  docker      Run ETL pipeline with Docker"
    echo "  airflow     Start Airflow environment"
    echo "  stop        Stop Airflow environment"
    echo "  logs        View Docker Compose logs"
    echo "  check       Run data quality checks"
    echo "  cleanup     Clean up Docker resources"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 local          # Run pipeline locally"
    echo "  $0 docker         # Run pipeline with Docker"
    echo "  $0 airflow        # Start Airflow for scheduled execution"
    echo "  $0 check          # Check data quality"
}

# Main script logic
main() {
    case "${1:-help}" in
        "local")
            check_prerequisites
            run_local
            ;;
        "docker")
            check_prerequisites
            run_docker
            ;;
        "airflow")
            check_prerequisites
            run_airflow
            ;;
        "stop")
            stop_airflow
            ;;
        "logs")
            view_logs
            ;;
        "check")
            check_data_quality
            ;;
        "cleanup")
            cleanup
            ;;
        "help"|*)
            show_usage
            ;;
    esac
}

# Run main function with all arguments
main "$@"
