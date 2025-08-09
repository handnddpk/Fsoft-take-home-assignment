# Quick Start Guide

## Prerequisites
- Docker and Docker Compose (for containerized execution)
- Python 3.7+ (for local execution)
- SQLite3 (for database queries)

## Running the ETL Pipeline

### Option 1: Local Execution
```bash
# Run the pipeline locally
./run.sh local
```

### Option 2: Docker Execution
```bash
# Run the pipeline in Docker
./run.sh docker
```

### Option 3: Airflow Scheduled Execution
```bash
# Start Airflow environment
./run.sh airflow

# Access Airflow UI at http://localhost:8080
# Username: admin, Password: admin
```

## Checking Results
```bash
# Run data quality checks
./run.sh check

# Or manually check the database
sqlite3 data/output/retail_data.db
```

## Example SQL Queries
```sql
-- View customer revenue
SELECT * FROM customer_revenue ORDER BY total_amount DESC;

-- View top product categories
SELECT category, COUNT(*) as product_count FROM products GROUP BY category;

-- View recent transactions
SELECT t.*, c.first_name, c.last_name, p.product_name 
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id
JOIN products p ON t.product_id = p.product_id
ORDER BY t.transaction_date DESC;
```

## Project Output
- **Database**: `data/output/retail_data.db`
- **Customer Revenue CSV**: `data/output/customer_revenue.csv`
- **Summary Report**: `data/output/pipeline_summary.txt`
- **Logs**: `etl_pipeline.log`
