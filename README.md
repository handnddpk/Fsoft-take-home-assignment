# Customer Transaction ETL Pipeline

## Overview
This project implements a comprehensive ETL (Extract, Transform, Load) pipeline for processing customer transaction data for a retail company. The pipeline extracts data from multiple CSV sources, cleans and transforms the data, and loads it into a SQLite database for analysis.

## Project Structure
```
├── data/
│   ├── input/           # Raw CSV files
│   │   ├── customers.csv
│   │   ├── transactions.csv
│   │   └── products.csv
│   └── output/          # Processed data and database
│       └── retail_data.db
├── src/
│   ├── etl_pipeline.py  # Main ETL script
│   ├── data_cleaner.py  # Data cleaning utilities
│   └── database.py      # Database operations
├── airflow/
│   └── dags/
│       └── customer_transaction_dag.py
├── sql/
│   └── customer_revenue.sql
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Data Sources
The pipeline processes three CSV files:

1. **customers.csv**: Customer information with potential email validation issues
2. **transactions.csv**: Transaction data with possible invalid dates and duplicates
3. **products.csv**: Product details with inconsistent category formatting

## Data Cleaning and Transformation Rules

### Customers Data
- **Email Validation**: Remove or flag customers with invalid email formats
- **Missing Data**: Handle missing customer information appropriately
- **Data Types**: Ensure proper data types for all fields

### Transactions Data
- **Date Validation**: Remove transactions with invalid dates (e.g., 2024-13-45)
- **Deduplication**: Remove duplicate transaction entries
- **Data Integrity**: Ensure transaction amounts are valid numbers

### Products Data
- **Category Standardization**: Normalize product categories (e.g., "Electronics" and "electronics" → "Electronics")
- **Missing Data**: Handle missing product information
- **Price Validation**: Ensure product prices are valid

## ETL Pipeline Architecture

### Extract Phase
- Read CSV files using pandas
- Initial data validation and structure verification
- Error handling for missing or corrupted files

### Transform Phase
- Apply data cleaning rules for each dataset
- Standardize data formats and types
- Remove invalid records and duplicates
- Normalize categorical data

### Load Phase
- Create SQLite database with proper schema
- Insert cleaned data into respective tables
- Create indexes for performance optimization
- Generate customer revenue aggregation table

## Database Schema

### Tables
1. **customers**: Customer information
   - customer_id (PRIMARY KEY)
   - first_name
   - last_name
   - email
   - registration_date

2. **products**: Product catalog
   - product_id (PRIMARY KEY)
   - product_name
   - category
   - price

3. **transactions**: Transaction records
   - transaction_id (PRIMARY KEY)
   - customer_id (FOREIGN KEY)
   - product_id (FOREIGN KEY)
   - transaction_date
   - quantity
   - amount

4. **customer_revenue**: Aggregated revenue per customer
   - customer_id (FOREIGN KEY)
   - total_amount
   - transaction_count

## Running the Pipeline

### Manual Execution
```bash
# Install dependencies
pip install -r requirements.txt

# Run the ETL pipeline
python src/etl_pipeline.py
```

### Docker Execution
```bash
# Build and run with Docker Compose
docker-compose up --build
```

### Airflow Execution
The pipeline is configured to run automatically using Apache Airflow:
- **Schedule**: Daily at 6:00 AM
- **Retry Policy**: 3 retries with 5-minute intervals
- **Monitoring**: Built-in Airflow monitoring and alerting

## Data Quality Checks
The pipeline includes comprehensive data quality validations:
- Email format validation using regex
- Date format validation
- Duplicate detection and removal
- Missing value handling
- Data type consistency checks

## Performance Optimizations
- Batch processing for large datasets
- Database indexing for query performance
- Memory-efficient data processing with pandas
- Parallel processing where applicable

## Error Handling
- Comprehensive logging for all pipeline stages
- Graceful handling of data quality issues
- Detailed error reporting and notifications
- Rollback capabilities for failed loads

## Monitoring and Alerting
- Airflow dashboard for pipeline monitoring
- Data quality metrics logging
- Email notifications for pipeline failures
- Performance metrics tracking

## Future Enhancements
- Support for additional data sources
- Real-time streaming capabilities
- Advanced data validation rules
- Machine learning-based anomaly detection
- Automated data profiling and documentation
