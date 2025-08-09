"""
Data cleaning utilities for the ETL pipeline.
This module contains functions to clean and validate data from CSV files.
"""

import pandas as pd
import re
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_email(email):
    """
    Validate email format using regex.
    
    Args:
        email (str): Email address to validate
        
    Returns:
        bool: True if email is valid, False otherwise
    """
    if pd.isna(email) or email == '':
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def clean_customers_data(df):
    """
    Clean customers data by validating emails and handling missing values.
    
    Args:
        df (pd.DataFrame): Raw customers dataframe
        
    Returns:
        pd.DataFrame: Cleaned customers dataframe
    """
    logger.info("Starting customers data cleaning...")
    
    # Create a copy to avoid modifying original data
    cleaned_df = df.copy()
    
    # Log initial data shape
    logger.info(f"Initial customers data shape: {cleaned_df.shape}")
    
    # Validate emails and remove invalid ones
    valid_email_mask = cleaned_df['email'].apply(validate_email)
    invalid_emails = cleaned_df[~valid_email_mask]['email'].tolist()
    
    if invalid_emails:
        logger.warning(f"Found {len(invalid_emails)} invalid emails: {invalid_emails}")
    
    # Remove rows with invalid emails
    cleaned_df = cleaned_df[valid_email_mask]
    
    # Handle missing values in other columns
    cleaned_df['first_name'] = cleaned_df['first_name'].fillna('Unknown')
    cleaned_df['last_name'] = cleaned_df['last_name'].fillna('Unknown')
    
    # Convert registration_date to datetime
    cleaned_df['registration_date'] = pd.to_datetime(cleaned_df['registration_date'], errors='coerce')
    
    # Remove rows with invalid registration dates
    cleaned_df = cleaned_df.dropna(subset=['registration_date'])
    
    logger.info(f"Cleaned customers data shape: {cleaned_df.shape}")
    logger.info(f"Removed {df.shape[0] - cleaned_df.shape[0]} invalid customer records")
    
    return cleaned_df

def validate_date(date_str):
    """
    Validate date format and check if date is valid.
    
    Args:
        date_str (str): Date string to validate
        
    Returns:
        bool: True if date is valid, False otherwise
    """
    if pd.isna(date_str) or date_str == '':
        return False
    
    try:
        datetime.strptime(str(date_str), '%Y-%m-%d')
        return True
    except ValueError:
        return False

def clean_transactions_data(df):
    """
    Clean transactions data by validating dates and removing duplicates.
    
    Args:
        df (pd.DataFrame): Raw transactions dataframe
        
    Returns:
        pd.DataFrame: Cleaned transactions dataframe
    """
    logger.info("Starting transactions data cleaning...")
    
    # Create a copy to avoid modifying original data
    cleaned_df = df.copy()
    
    # Log initial data shape
    logger.info(f"Initial transactions data shape: {cleaned_df.shape}")
    
    # Validate transaction dates
    valid_date_mask = cleaned_df['transaction_date'].apply(validate_date)
    invalid_dates = cleaned_df[~valid_date_mask]['transaction_date'].tolist()
    
    if invalid_dates:
        logger.warning(f"Found {len(invalid_dates)} invalid dates: {invalid_dates}")
    
    # Remove rows with invalid dates
    cleaned_df = cleaned_df[valid_date_mask]
    
    # Convert transaction_date to datetime
    cleaned_df['transaction_date'] = pd.to_datetime(cleaned_df['transaction_date'])
    
    # Remove duplicate transactions (same customer, product, date, amount)
    initial_count = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates(
        subset=['customer_id', 'product_id', 'transaction_date', 'amount']
    )
    duplicates_removed = initial_count - len(cleaned_df)
    
    if duplicates_removed > 0:
        logger.warning(f"Removed {duplicates_removed} duplicate transactions")
    
    # Validate quantity and amount
    cleaned_df = cleaned_df[(cleaned_df['quantity'] > 0) & (cleaned_df['amount'] > 0)]
    
    logger.info(f"Cleaned transactions data shape: {cleaned_df.shape}")
    logger.info(f"Removed {df.shape[0] - cleaned_df.shape[0]} invalid transaction records")
    
    return cleaned_df

def standardize_category(category):
    """
    Standardize product category format.
    
    Args:
        category (str): Category to standardize
        
    Returns:
        str: Standardized category
    """
    if pd.isna(category):
        return 'Unknown'
    
    # Convert to title case and handle common variations
    category = str(category).strip()
    category_mapping = {
        'electronics': 'Electronics',
        'ELECTRONICS': 'Electronics',
        'sports': 'Sports',
        'SPORTS': 'Sports',
        'office supplies': 'Office Supplies',
        'OFFICE SUPPLIES': 'Office Supplies',
        'office_supplies': 'Office Supplies'
    }
    
    return category_mapping.get(category.lower(), category.title())

def clean_products_data(df):
    """
    Clean products data by standardizing categories and validating prices.
    
    Args:
        df (pd.DataFrame): Raw products dataframe
        
    Returns:
        pd.DataFrame: Cleaned products dataframe
    """
    logger.info("Starting products data cleaning...")
    
    # Create a copy to avoid modifying original data
    cleaned_df = df.copy()
    
    # Log initial data shape
    logger.info(f"Initial products data shape: {cleaned_df.shape}")
    
    # Standardize product categories
    cleaned_df['category'] = cleaned_df['category'].apply(standardize_category)
    
    # Handle missing product names
    cleaned_df['product_name'] = cleaned_df['product_name'].fillna('Unknown Product')
    
    # Validate and clean prices
    cleaned_df['price'] = pd.to_numeric(cleaned_df['price'], errors='coerce')
    cleaned_df = cleaned_df[cleaned_df['price'] > 0]
    
    logger.info(f"Cleaned products data shape: {cleaned_df.shape}")
    logger.info(f"Removed {df.shape[0] - cleaned_df.shape[0]} invalid product records")
    
    return cleaned_df

def validate_data_integrity(customers_df, transactions_df, products_df):
    """
    Validate referential integrity between datasets.
    
    Args:
        customers_df (pd.DataFrame): Cleaned customers dataframe
        transactions_df (pd.DataFrame): Cleaned transactions dataframe
        products_df (pd.DataFrame): Cleaned products dataframe
        
    Returns:
        tuple: (valid_transactions_df, integrity_report)
    """
    logger.info("Validating data integrity...")
    
    # Check for orphaned transactions (customer_id not in customers)
    valid_customer_ids = set(customers_df['customer_id'])
    orphaned_customers = transactions_df[~transactions_df['customer_id'].isin(valid_customer_ids)]
    
    # Check for orphaned transactions (product_id not in products)
    valid_product_ids = set(products_df['product_id'])
    orphaned_products = transactions_df[~transactions_df['product_id'].isin(valid_product_ids)]
    
    # Remove orphaned transactions
    valid_transactions = transactions_df[
        transactions_df['customer_id'].isin(valid_customer_ids) &
        transactions_df['product_id'].isin(valid_product_ids)
    ]
    
    integrity_report = {
        'orphaned_customer_transactions': len(orphaned_customers),
        'orphaned_product_transactions': len(orphaned_products),
        'valid_transactions': len(valid_transactions),
        'total_transactions': len(transactions_df)
    }
    
    logger.info(f"Data integrity report: {integrity_report}")
    
    return valid_transactions, integrity_report
