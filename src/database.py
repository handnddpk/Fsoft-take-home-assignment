"""
Database operations for the ETL pipeline.
This module handles SQLite database creation, schema setup, and data loading.
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages SQLite database operations for the ETL pipeline."""
    
    def __init__(self, db_path):
        """
        Initialize database manager.
        
        Args:
            db_path (str): Path to SQLite database file
        """
        self.db_path = db_path
        self.connection = None
        
    def connect(self):
        """Establish connection to SQLite database."""
        try:
            # Ensure directory exists
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.connection = sqlite3.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def create_schema(self):
        """Create database schema with all required tables."""
        if not self.connection:
            raise ConnectionError("Database connection not established")
        
        cursor = self.connection.cursor()
        
        try:
            # Create customers table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id INTEGER PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    registration_date DATE NOT NULL
                )
            """)
            
            # Create products table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    product_id INTEGER PRIMARY KEY,
                    product_name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    price DECIMAL(10, 2) NOT NULL CHECK (price > 0)
                )
            """)
            
            # Create transactions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id INTEGER PRIMARY KEY,
                    customer_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    transaction_date DATE NOT NULL,
                    quantity INTEGER NOT NULL CHECK (quantity > 0),
                    amount DECIMAL(10, 2) NOT NULL CHECK (amount > 0),
                    FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
                    FOREIGN KEY (product_id) REFERENCES products (product_id)
                )
            """)
            
            # Create customer_revenue table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customer_revenue (
                    customer_id INTEGER PRIMARY KEY,
                    total_amount DECIMAL(12, 2) NOT NULL,
                    transaction_count INTEGER NOT NULL,
                    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
                )
            """)
            
            # Create indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_customer_id ON transactions(customer_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_product_id ON transactions(product_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)")
            
            self.connection.commit()
            logger.info("Database schema created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create database schema: {e}")
            self.connection.rollback()
            raise
    
    def load_customers(self, customers_df):
        """
        Load customers data into the database.
        
        Args:
            customers_df (pd.DataFrame): Cleaned customers dataframe
        """
        try:
            customers_df.to_sql('customers', self.connection, if_exists='replace', index=False)
            logger.info(f"Loaded {len(customers_df)} customer records")
        except Exception as e:
            logger.error(f"Failed to load customers data: {e}")
            raise
    
    def load_products(self, products_df):
        """
        Load products data into the database.
        
        Args:
            products_df (pd.DataFrame): Cleaned products dataframe
        """
        try:
            products_df.to_sql('products', self.connection, if_exists='replace', index=False)
            logger.info(f"Loaded {len(products_df)} product records")
        except Exception as e:
            logger.error(f"Failed to load products data: {e}")
            raise
    
    def load_transactions(self, transactions_df):
        """
        Load transactions data into the database.
        
        Args:
            transactions_df (pd.DataFrame): Cleaned transactions dataframe
        """
        try:
            transactions_df.to_sql('transactions', self.connection, if_exists='replace', index=False)
            logger.info(f"Loaded {len(transactions_df)} transaction records")
        except Exception as e:
            logger.error(f"Failed to load transactions data: {e}")
            raise
    
    def calculate_customer_revenue(self):
        """Calculate and store customer revenue aggregation."""
        cursor = self.connection.cursor()
        
        try:
            # Calculate customer revenue
            cursor.execute("""
                INSERT OR REPLACE INTO customer_revenue (customer_id, total_amount, transaction_count)
                SELECT 
                    t.customer_id,
                    SUM(t.amount) as total_amount,
                    COUNT(*) as transaction_count
                FROM transactions t
                GROUP BY t.customer_id
            """)
            
            self.connection.commit()
            
            # Get count of revenue records
            cursor.execute("SELECT COUNT(*) FROM customer_revenue")
            count = cursor.fetchone()[0]
            
            logger.info(f"Calculated revenue for {count} customers")
            
        except Exception as e:
            logger.error(f"Failed to calculate customer revenue: {e}")
            self.connection.rollback()
            raise
    
    def get_data_summary(self):
        """
        Get summary statistics of loaded data.
        
        Returns:
            dict: Summary statistics
        """
        cursor = self.connection.cursor()
        
        try:
            # Get table counts
            cursor.execute("SELECT COUNT(*) FROM customers")
            customers_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM products")
            products_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM transactions")
            transactions_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM customer_revenue")
            revenue_count = cursor.fetchone()[0]
            
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
            
            summary = {
                'customers_count': customers_count,
                'products_count': products_count,
                'transactions_count': transactions_count,
                'revenue_records_count': revenue_count,
                'avg_customer_revenue': round(revenue_stats[0], 2) if revenue_stats[0] else 0,
                'max_customer_revenue': round(revenue_stats[1], 2) if revenue_stats[1] else 0,
                'min_customer_revenue': round(revenue_stats[2], 2) if revenue_stats[2] else 0,
                'total_revenue': round(revenue_stats[3], 2) if revenue_stats[3] else 0
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get data summary: {e}")
            raise
    
    def execute_query(self, query):
        """
        Execute a SQL query and return results.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            list: Query results
        """
        cursor = self.connection.cursor()
        
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            
            return {
                'columns': column_names,
                'data': results
            }
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    def export_to_csv(self, table_name, output_path):
        """
        Export table data to CSV file.
        
        Args:
            table_name (str): Name of table to export
            output_path (str): Path to save CSV file
        """
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.connection)
            df.to_csv(output_path, index=False)
            logger.info(f"Exported {table_name} to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export {table_name}: {e}")
            raise
