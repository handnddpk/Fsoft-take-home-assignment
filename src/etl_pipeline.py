"""
Main ETL Pipeline for Customer Transaction Data Processing.

This script orchestrates the complete ETL process:
1. Extract data from CSV files
2. Clean and transform the data
3. Load data into SQLite database
4. Generate customer revenue aggregation
"""

import pandas as pd
import logging
import sys
from pathlib import Path
import argparse
from datetime import datetime

# Add src directory to path
sys.path.append(str(Path(__file__).parent))

from data_cleaner import (
    clean_customers_data,
    clean_transactions_data,
    clean_products_data,
    validate_data_integrity
)
from database import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ETLPipeline:
    """Main ETL Pipeline class."""
    
    def __init__(self, input_dir, output_dir):
        """
        Initialize ETL Pipeline.
        
        Args:
            input_dir (str): Directory containing input CSV files
            output_dir (str): Directory for output database and logs
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.db_path = self.output_dir / 'retail_data.db'
        self.db_manager = DatabaseManager(str(self.db_path))
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def extract_data(self):
        """
        Extract data from CSV files.
        
        Returns:
            tuple: (customers_df, transactions_df, products_df)
        """
        logger.info("Starting data extraction phase...")
        
        try:
            # Read CSV files
            customers_file = self.input_dir / 'customers.csv'
            transactions_file = self.input_dir / 'transactions.csv'
            products_file = self.input_dir / 'products.csv'
            
            # Check if all files exist
            for file_path in [customers_file, transactions_file, products_file]:
                if not file_path.exists():
                    raise FileNotFoundError(f"Required file not found: {file_path}")
            
            # Load data
            customers_df = pd.read_csv(customers_file)
            transactions_df = pd.read_csv(transactions_file)
            products_df = pd.read_csv(products_file)
            
            logger.info(f"Extracted {len(customers_df)} customer records")
            logger.info(f"Extracted {len(transactions_df)} transaction records")
            logger.info(f"Extracted {len(products_df)} product records")
            
            return customers_df, transactions_df, products_df
            
        except Exception as e:
            logger.error(f"Data extraction failed: {e}")
            raise
    
    def transform_data(self, customers_df, transactions_df, products_df):
        """
        Transform and clean the extracted data.
        
        Args:
            customers_df (pd.DataFrame): Raw customers data
            transactions_df (pd.DataFrame): Raw transactions data
            products_df (pd.DataFrame): Raw products data
            
        Returns:
            tuple: (cleaned_customers_df, cleaned_transactions_df, cleaned_products_df)
        """
        logger.info("Starting data transformation phase...")
        
        try:
            # Clean individual datasets
            cleaned_customers = clean_customers_data(customers_df)
            cleaned_transactions = clean_transactions_data(transactions_df)
            cleaned_products = clean_products_data(products_df)
            
            # Validate data integrity between datasets
            valid_transactions, integrity_report = validate_data_integrity(
                cleaned_customers, cleaned_transactions, cleaned_products
            )
            
            logger.info("Data transformation completed successfully")
            
            return cleaned_customers, valid_transactions, cleaned_products
            
        except Exception as e:
            logger.error(f"Data transformation failed: {e}")
            raise
    
    def load_data(self, customers_df, transactions_df, products_df):
        """
        Load cleaned data into the database.
        
        Args:
            customers_df (pd.DataFrame): Cleaned customers data
            transactions_df (pd.DataFrame): Cleaned transactions data
            products_df (pd.DataFrame): Cleaned products data
        """
        logger.info("Starting data loading phase...")
        
        try:
            # Connect to database
            self.db_manager.connect()
            
            # Create schema
            self.db_manager.create_schema()
            
            # Load data into tables
            self.db_manager.load_customers(customers_df)
            self.db_manager.load_products(products_df)
            self.db_manager.load_transactions(transactions_df)
            
            # Calculate customer revenue
            self.db_manager.calculate_customer_revenue()
            
            logger.info("Data loading completed successfully")
            
        except Exception as e:
            logger.error(f"Data loading failed: {e}")
            raise
        finally:
            self.db_manager.disconnect()
    
    def generate_reports(self):
        """Generate summary reports and export key data."""
        logger.info("Generating reports...")
        
        try:
            self.db_manager.connect()
            
            # Get data summary
            summary = self.db_manager.get_data_summary()
            
            # Log summary
            logger.info("=== ETL Pipeline Summary ===")
            for key, value in summary.items():
                logger.info(f"{key}: {value}")
            
            # Export customer revenue to CSV
            revenue_csv_path = self.output_dir / 'customer_revenue.csv'
            self.db_manager.export_to_csv('customer_revenue', str(revenue_csv_path))
            
            # Export summary to file
            summary_path = self.output_dir / 'pipeline_summary.txt'
            with open(summary_path, 'w') as f:
                f.write(f"ETL Pipeline Summary - {datetime.now()}\n")
                f.write("=" * 50 + "\n")
                for key, value in summary.items():
                    f.write(f"{key}: {value}\n")
            
            logger.info(f"Reports generated in {self.output_dir}")
            
        except Exception as e:
            logger.error(f"Report generation failed: {e}")
            raise
        finally:
            self.db_manager.disconnect()
    
    def run(self):
        """Execute the complete ETL pipeline."""
        start_time = datetime.now()
        logger.info(f"Starting ETL pipeline at {start_time}")
        
        try:
            # Extract phase
            customers_df, transactions_df, products_df = self.extract_data()
            
            # Transform phase
            cleaned_customers, cleaned_transactions, cleaned_products = self.transform_data(
                customers_df, transactions_df, products_df
            )
            
            # Load phase
            self.load_data(cleaned_customers, cleaned_transactions, cleaned_products)
            
            # Generate reports
            self.generate_reports()
            
            # Calculate execution time
            end_time = datetime.now()
            execution_time = end_time - start_time
            
            logger.info(f"ETL pipeline completed successfully in {execution_time}")
            logger.info(f"Database created at: {self.db_path}")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise

def main():
    """Main function to run the ETL pipeline."""
    parser = argparse.ArgumentParser(description='Customer Transaction ETL Pipeline')
    parser.add_argument(
        '--input-dir',
        default='../data/input',
        help='Directory containing input CSV files'
    )
    parser.add_argument(
        '--output-dir',
        default='../data/output',
        help='Directory for output database and reports'
    )
    
    args = parser.parse_args()
    
    # Convert relative paths to absolute
    script_dir = Path(__file__).parent
    input_dir = script_dir / args.input_dir
    output_dir = script_dir / args.output_dir
    
    # Initialize and run pipeline
    pipeline = ETLPipeline(str(input_dir), str(output_dir))
    pipeline.run()

if __name__ == "__main__":
    main()
