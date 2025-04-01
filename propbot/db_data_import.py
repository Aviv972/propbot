#!/usr/bin/env python3
"""
Database Import Script for PropBot

This script creates the necessary database tables for sales and rental properties,
plus investment metrics, and imports data from existing CSV files.
"""

import os
import sys
import logging
import pandas as pd
import psycopg2
from psycopg2 import extras
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import database utilities
try:
    from propbot.database_utils import get_connection, initialize_database
except ImportError:
    # Handle case when running from command line
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from propbot.database_utils import get_connection, initialize_database

# Define paths based on environment
def get_data_paths():
    """Get data paths based on environment (local or Heroku)"""
    if 'DYNO' in os.environ:  # Running on Heroku
        base_path = Path('/app/propbot/data')
    else:  # Running locally
        base_path = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / 'data'
    
    return {
        'processed_dir': base_path / 'processed',
        'output_dir': base_path / 'output' / 'reports',
        'reports_dir': base_path / 'reports'
    }

def create_database_schema(conn):
    """Create database tables for properties and investment metrics"""
    try:
        with conn.cursor() as cur:
            # Create sales properties table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS properties_sales (
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT,
                    price NUMERIC,
                    size NUMERIC,
                    rooms INTEGER,
                    price_per_sqm NUMERIC,
                    location TEXT,
                    neighborhood TEXT,
                    details TEXT,
                    snapshot_date TIMESTAMP,
                    first_seen_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Create rental properties table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS properties_rentals (
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT,
                    price NUMERIC,
                    size NUMERIC,
                    rooms INTEGER,
                    price_per_sqm NUMERIC,
                    location TEXT,
                    neighborhood TEXT,
                    details TEXT,
                    is_furnished BOOLEAN,
                    snapshot_date TIMESTAMP,
                    first_seen_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Create investment metrics table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS investment_metrics (
                    id SERIAL PRIMARY KEY,
                    property_url TEXT NOT NULL,
                    property_price NUMERIC,
                    size NUMERIC,
                    rooms INTEGER,
                    neighborhood TEXT,
                    monthly_rent NUMERIC,
                    price_per_sqm NUMERIC,
                    avg_neighborhood_price_per_sqm NUMERIC,
                    comparable_count INTEGER,
                    gross_rental_yield NUMERIC,
                    cap_rate NUMERIC,
                    cash_on_cash_return NUMERIC,
                    monthly_cash_flow NUMERIC,
                    annual_cash_flow NUMERIC,
                    noi_monthly NUMERIC,
                    noi_annual NUMERIC,
                    analysis_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (property_url) REFERENCES properties_sales (url) ON DELETE CASCADE
                )
            """)
            
            logger.info("Database schema created successfully")
            return True
    except Exception as e:
        logger.error(f"Error creating database schema: {str(e)}")
        return False

def import_sales_data(conn, processed_dir):
    """Import sales data from CSV to database"""
    sales_file = processed_dir / 'sales.csv'
    sales_current_file = processed_dir / 'sales_current.csv'
    
    # Try both potential file locations
    if sales_current_file.exists():
        file_path = sales_current_file
    elif sales_file.exists():
        file_path = sales_file
    else:
        # Check in reports dir for latest investment summary
        paths = get_data_paths()
        report_files = list(Path(paths['reports_dir']).glob('investment_summary_*.csv'))
        if report_files:
            # Get most recent file
            file_path = sorted(report_files)[-1]
            logger.info(f"Using investment summary file: {file_path}")
        else:
            logger.error("No sales data files found")
            return False
    
    try:
        # Read CSV file with pandas
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {file_path}")
        
        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            record = {
                'url': row.get('url', None),
                'price': row.get('price', None),
                'size': row.get('size', None),
                'rooms': row.get('num_rooms', None) if 'num_rooms' in row else row.get('rooms', None),
                'price_per_sqm': row.get('price_per_sqm', None),
                'location': row.get('location', None),
                'neighborhood': row.get('neighborhood', None),
                'details': row.get('details', None),
                'snapshot_date': row.get('snapshot_date', datetime.now().strftime('%Y-%m-%d')),
                'first_seen_date': row.get('first_seen_date', row.get('snapshot_date', datetime.now().strftime('%Y-%m-%d')))
            }
            
            # Filter out None values for url (required field)
            if record['url']:
                records.append(record)
        
        if not records:
            logger.warning("No valid sales records found in CSV file")
            return False
        
        # Insert into database using upsert
        with conn:
            with conn.cursor() as cur:
                # Create a list of column names that match the dict keys
                columns = records[0].keys()
                # Create the SQL placeholders for the VALUES clause
                values_template = '(' + ','.join(['%s'] * len(columns)) + ')'
                
                # Create SQL query for insert with ON CONFLICT DO UPDATE
                upsert_query = (
                    f"INSERT INTO properties_sales ({','.join(columns)}) VALUES {values_template} "
                    f"ON CONFLICT (url) DO UPDATE SET "
                    f"{', '.join(f'{col} = EXCLUDED.{col}' for col in columns if col != 'url')}, "
                    f"updated_at = NOW()"
                )
                
                # Execute for each record
                inserted = 0
                updated = 0
                
                for record in records:
                    try:
                        # Check if record exists
                        cur.execute("SELECT 1 FROM properties_sales WHERE url = %s", (record['url'],))
                        exists = cur.fetchone() is not None
                        
                        # Execute upsert
                        values = [record[col] for col in columns]
                        cur.execute(upsert_query, values)
                        
                        if exists:
                            updated += 1
                        else:
                            inserted += 1
                    except Exception as e:
                        logger.error(f"Error inserting record {record['url']}: {str(e)}")
                
                logger.info(f"Inserted {inserted} new sales records, updated {updated} existing records")
                return True
    except Exception as e:
        logger.error(f"Error importing sales data: {str(e)}")
        return False

def import_rental_data(conn, processed_dir):
    """Import rental data from CSV to database"""
    rentals_file = processed_dir / 'rentals.csv'
    rentals_current_file = processed_dir / 'rentals_current.csv'
    
    # Try both potential file locations
    if rentals_current_file.exists():
        file_path = rentals_current_file
    elif rentals_file.exists():
        file_path = rentals_file
    else:
        # Check for rental income report in output dir
        paths = get_data_paths()
        rental_income_file = paths['output_dir'] / 'rental_income_report_current.csv'
        
        if rental_income_file.exists():
            file_path = rental_income_file
            logger.info(f"Using rental income report: {file_path}")
        else:
            logger.error("No rental data files found")
            return False
    
    try:
        # Read CSV file with pandas
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {file_path}")
        
        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            record = {
                'url': row.get('url', None),
                'price': row.get('price', None),
                'size': row.get('size', None),
                'rooms': row.get('num_rooms', None) if 'num_rooms' in row else row.get('rooms', None),
                'price_per_sqm': row.get('price_per_sqm', None),
                'location': row.get('location', None),
                'neighborhood': row.get('neighborhood', None),
                'details': row.get('details', None),
                'is_furnished': row.get('is_furnished', None),
                'snapshot_date': row.get('snapshot_date', datetime.now().strftime('%Y-%m-%d')),
                'first_seen_date': row.get('first_seen_date', row.get('snapshot_date', datetime.now().strftime('%Y-%m-%d')))
            }
            
            # Filter out None values for url (required field)
            if record['url']:
                records.append(record)
        
        if not records:
            logger.warning("No valid rental records found in CSV file")
            return False
        
        # Insert into database using upsert
        with conn:
            with conn.cursor() as cur:
                # Create a list of column names that match the dict keys
                columns = records[0].keys()
                # Create the SQL placeholders for the VALUES clause
                values_template = '(' + ','.join(['%s'] * len(columns)) + ')'
                
                # Create SQL query for insert with ON CONFLICT DO UPDATE
                upsert_query = (
                    f"INSERT INTO properties_rentals ({','.join(columns)}) VALUES {values_template} "
                    f"ON CONFLICT (url) DO UPDATE SET "
                    f"{', '.join(f'{col} = EXCLUDED.{col}' for col in columns if col != 'url')}, "
                    f"updated_at = NOW()"
                )
                
                # Execute for each record
                inserted = 0
                updated = 0
                
                for record in records:
                    try:
                        # Check if record exists
                        cur.execute("SELECT 1 FROM properties_rentals WHERE url = %s", (record['url'],))
                        exists = cur.fetchone() is not None
                        
                        # Execute upsert
                        values = [record[col] for col in columns]
                        cur.execute(upsert_query, values)
                        
                        if exists:
                            updated += 1
                        else:
                            inserted += 1
                    except Exception as e:
                        logger.error(f"Error inserting record {record['url']}: {str(e)}")
                
                logger.info(f"Inserted {inserted} new rental records, updated {updated} existing records")
                return True
    except Exception as e:
        logger.error(f"Error importing rental data: {str(e)}")
        return False

def import_investment_metrics(conn):
    """Import investment metrics from the latest summary report"""
    paths = get_data_paths()
    report_files = list(Path(paths['reports_dir']).glob('investment_summary_*.csv'))
    
    if not report_files:
        logger.error("No investment summary files found")
        return False
    
    # Get most recent file
    latest_report = sorted(report_files)[-1]
    logger.info(f"Using investment metrics from: {latest_report}")
    
    try:
        # Read CSV file with pandas
        df = pd.read_csv(latest_report)
        logger.info(f"Loaded {len(df)} rows from {latest_report}")
        
        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            # Skip rows without URL
            if 'url' not in row or pd.isna(row['url']):
                continue
                
            record = {
                'property_url': row.get('url'),
                'property_price': row.get('price'),
                'size': row.get('size'),
                'rooms': row.get('rooms', None),
                'neighborhood': row.get('location', None),
                'monthly_rent': row.get('monthly_rent'),
                'price_per_sqm': row.get('price_per_sqm'),
                'avg_neighborhood_price_per_sqm': None,  # May need to calculate this
                'comparable_count': row.get('comparable_count', None),
                'gross_rental_yield': row.get('gross_yield'),
                'cap_rate': row.get('cap_rate'),
                'cash_on_cash_return': row.get('coc_return'),
                'monthly_cash_flow': row.get('monthly_cash_flow'),
                'annual_cash_flow': row.get('annual_cash_flow'),
                'noi_monthly': row.get('noi_monthly'),
                'noi_annual': row.get('noi_annual'),
                'analysis_date': datetime.now().strftime('%Y-%m-%d')
            }
            records.append(record)
        
        if not records:
            logger.warning("No valid metrics found in investment summary")
            return False
        
        # Insert into database
        with conn:
            with conn.cursor() as cur:
                # First, verify that URLs exist in properties_sales
                for record in records[:]:
                    url = record['property_url']
                    cur.execute("SELECT 1 FROM properties_sales WHERE url = %s", (url,))
                    if cur.fetchone() is None:
                        logger.warning(f"URL {url} not found in properties_sales table, skipping")
                        records.remove(record)
                
                if not records:
                    logger.warning("No valid metrics with matching sales property URLs")
                    return False
                
                # Create a list of column names that match the dict keys
                columns = records[0].keys()
                # Create the SQL placeholders for the VALUES clause
                values_template = '(' + ','.join(['%s'] * len(columns)) + ')'
                
                # Delete existing records for these properties to avoid duplicates
                property_urls = [record['property_url'] for record in records]
                placeholders = ','.join(['%s'] * len(property_urls))
                cur.execute(f"DELETE FROM investment_metrics WHERE property_url IN ({placeholders})", 
                           property_urls)
                
                # Create SQL query for insert
                insert_query = f"INSERT INTO investment_metrics ({','.join(columns)}) VALUES {values_template}"
                
                # Execute for each record
                inserted = 0
                for record in records:
                    try:
                        values = [record[col] for col in columns]
                        cur.execute(insert_query, values)
                        inserted += 1
                    except Exception as e:
                        logger.error(f"Error inserting metrics for {record['property_url']}: {str(e)}")
                
                logger.info(f"Inserted {inserted} investment metric records")
                return True
    except Exception as e:
        logger.error(f"Error importing investment metrics: {str(e)}")
        return False

def main():
    """Main entry point for the script"""
    logger.info("Starting PropBot database data import")
    
    # Initialize database schema
    conn = get_connection()
    if not conn:
        logger.error("Failed to connect to database")
        return False
    
    try:
        # Make sure rental_metadata table exists
        if not initialize_database():
            logger.error("Failed to initialize database schema")
            return False
        
        # Create property and metrics tables
        if not create_database_schema(conn):
            logger.error("Failed to create database schema")
            return False
        
        # Get data paths
        paths = get_data_paths()
        
        # Import sales data
        if not import_sales_data(conn, paths['processed_dir']):
            logger.warning("Failed to import sales data")
        
        # Import rental data
        if not import_rental_data(conn, paths['processed_dir']):
            logger.warning("Failed to import rental data")
        
        # Import investment metrics
        if not import_investment_metrics(conn):
            logger.warning("Failed to import investment metrics")
        
        logger.info("PropBot database data import completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error in database import: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 