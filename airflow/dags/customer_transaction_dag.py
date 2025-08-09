"""
Airflow DAG for Customer Transaction ETL Pipeline.

This DAG runs the ETL pipeline on a scheduled basis with proper monitoring,
error handling, and notifications.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add the src directory to Python path
sys.path.append('/opt/airflow/dags/src')

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'email': ['data-team@company.com']
}

# Initialize DAG
dag = DAG(
    'customer_transaction_etl',
    default_args=default_args,
    description='ETL pipeline for customer transaction data processing',
    schedule_interval='0 6 * * *',  # Daily at 6:00 AM
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'customer-data', 'transactions']
)

def check_input_files(**context):
    """Check if all required input files are present."""
    import os
    from pathlib import Path
    
    input_dir = Path('/opt/airflow/data/input')
    required_files = ['customers.csv', 'transactions.csv', 'products.csv']
    
    missing_files = []
    for file_name in required_files:
        file_path = input_dir / file_name
        if not file_path.exists():
            missing_files.append(str(file_path))
    
    if missing_files:
        raise FileNotFoundError(f"Missing required input files: {missing_files}")
    
    print("All required input files are present")
    return True

def run_etl_pipeline(**context):
    """Execute the main ETL pipeline."""
    import subprocess
    import sys
    
    # Run the ETL pipeline script
    cmd = [
        sys.executable,
        '/opt/airflow/dags/src/etl_pipeline.py',
        '--input-dir', '/opt/airflow/data/input',
        '--output-dir', '/opt/airflow/data/output'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print("ETL pipeline completed successfully")
        print("STDOUT:", result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print("ETL pipeline failed")
        print("STDERR:", e.stderr)
        print("STDOUT:", e.stdout)
        raise

def validate_output_data(**context):
    """Validate the output data quality."""
    import sqlite3
    import pandas as pd
    from pathlib import Path
    
    db_path = Path('/opt/airflow/data/output/retail_data.db')
    
    if not db_path.exists():
        raise FileNotFoundError(f"Output database not found: {db_path}")
    
    conn = sqlite3.connect(str(db_path))
    
    try:
        # Check if all tables exist and have data
        tables = ['customers', 'products', 'transactions', 'customer_revenue']
        table_counts = {}
        
        for table in tables:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            table_counts[table] = count
            
            if count == 0:
                raise ValueError(f"Table {table} is empty")
        
        print("Data validation summary:")
        for table, count in table_counts.items():
            print(f"  {table}: {count} records")
        
        # Additional data quality checks
        cursor = conn.cursor()
        
        # Check for customers with invalid emails (should be none after cleaning)
        cursor.execute("SELECT COUNT(*) FROM customers WHERE email NOT LIKE '%@%'")
        invalid_emails = cursor.fetchone()[0]
        if invalid_emails > 0:
            raise ValueError(f"Found {invalid_emails} customers with invalid emails")
        
        # Check for transactions with negative amounts (should be none)
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE amount <= 0")
        invalid_amounts = cursor.fetchone()[0]
        if invalid_amounts > 0:
            raise ValueError(f"Found {invalid_amounts} transactions with invalid amounts")
        
        print("All data validation checks passed")
        return table_counts
        
    finally:
        conn.close()

def generate_data_quality_report(**context):
    """Generate a comprehensive data quality report."""
    import sqlite3
    import json
    from pathlib import Path
    from datetime import datetime
    
    db_path = Path('/opt/airflow/data/output/retail_data.db')
    conn = sqlite3.connect(str(db_path))
    
    try:
        cursor = conn.cursor()
        
        # Gather data quality metrics
        report = {
            'execution_date': context['execution_date'].isoformat(),
            'report_generated': datetime.now().isoformat(),
            'tables': {}
        }
        
        # Get table statistics
        tables = ['customers', 'products', 'transactions', 'customer_revenue']
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            report['tables'][table] = {'record_count': count}
        
        # Get revenue statistics
        cursor.execute("""
            SELECT 
                AVG(total_amount) as avg_revenue,
                MAX(total_amount) as max_revenue,
                MIN(total_amount) as min_revenue,
                SUM(total_amount) as total_revenue
            FROM customer_revenue
        """)
        revenue_stats = cursor.fetchone()
        
        report['revenue_metrics'] = {
            'avg_customer_revenue': round(revenue_stats[0], 2) if revenue_stats[0] else 0,
            'max_customer_revenue': round(revenue_stats[1], 2) if revenue_stats[1] else 0,
            'min_customer_revenue': round(revenue_stats[2], 2) if revenue_stats[2] else 0,
            'total_revenue': round(revenue_stats[3], 2) if revenue_stats[3] else 0
        }
        
        # Save report
        report_path = Path('/opt/airflow/data/output/data_quality_report.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"Data quality report saved to {report_path}")
        return report
        
    finally:
        conn.close()

# Define tasks
check_files_task = PythonOperator(
    task_id='check_input_files',
    python_callable=check_input_files,
    dag=dag
)

etl_task = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=run_etl_pipeline,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_output_data',
    python_callable=validate_output_data,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_data_quality_report',
    python_callable=generate_data_quality_report,
    dag=dag
)

# Archive previous results
archive_task = BashOperator(
    task_id='archive_previous_results',
    bash_command="""
    if [ -f /opt/airflow/data/output/retail_data.db ]; then
        mkdir -p /opt/airflow/data/archive/{{ ds }}
        cp /opt/airflow/data/output/* /opt/airflow/data/archive/{{ ds }}/ || true
    fi
    """,
    dag=dag
)

# Success notification
success_notification = EmailOperator(
    task_id='send_success_notification',
    to=['data-team@company.com'],
    subject='ETL Pipeline Completed Successfully - {{ ds }}',
    html_content="""
    <h3>ETL Pipeline Success Notification</h3>
    <p>The customer transaction ETL pipeline has completed successfully.</p>
    <ul>
        <li><strong>Execution Date:</strong> {{ ds }}</li>
        <li><strong>Duration:</strong> {{ task_instance.duration }}s</li>
        <li><strong>Database:</strong> /opt/airflow/data/output/retail_data.db</li>
    </ul>
    <p>Please check the data quality report for detailed metrics.</p>
    """,
    dag=dag,
    trigger_rule='all_success'
)

# Define task dependencies
check_files_task >> archive_task >> etl_task >> validate_task >> report_task >> success_notification
