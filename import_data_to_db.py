#!/usr/bin/env python3
"""
Import CSV data to database

This script imports property listings from both rentals_current.csv and sales_current.csv
files into the database.
"""

import os
import sys
import logging
import psycopg2
import pandas as pd
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_connection():
    """Get a connection to the database."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        return None
    
    # Add SSL mode if not present
    if "sslmode=" not in database_url:
        database_url += "?sslmode=require"
    
    try:
        conn = psycopg2.connect(database_url)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        return None

def create_database_schema(conn):
    """Create database tables for properties if they don't exist."""
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
            
            logger.info("Database schema created successfully")
            return True
    except Exception as e:
        logger.error(f"Error creating database schema: {str(e)}")
        return False

def import_sales_data(conn, sales_file):
    """Import sales data from CSV to database."""
    try:
        # Read CSV file with pandas
        df = pd.read_csv(sales_file)
        logger.info(f"Loaded {len(df)} rows from {sales_file}")
        
        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            # Extract neighborhood from location if available
            neighborhood = None
            location = row.get('location', '')
            if isinstance(location, str) and ', ' in location:
                neighborhood = location.split(', ')[-1]
            
            record = {
                'url': row.get('url', None),
                'title': row.get('title', None),
                'price': row.get('price', None),
                'size': row.get('size', None),
                'rooms': row.get('num_rooms', None) if 'num_rooms' in row else row.get('rooms', None),
                'price_per_sqm': row.get('price_per_sqm', None),
                'location': location,
                'neighborhood': neighborhood,
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

def import_rental_data(conn, rental_file):
    """Import rental data from CSV to database."""
    try:
        # Read CSV file with pandas
        df = pd.read_csv(rental_file)
        logger.info(f"Loaded {len(df)} rows from {rental_file}")
        
        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            # Extract neighborhood from location if available
            neighborhood = None
            location = row.get('location', '')
            if isinstance(location, str) and ', ' in location:
                neighborhood = location.split(', ')[-1]
            
            # Determine if it's furnished from details field
            is_furnished = False
            details = row.get('details', '')
            if isinstance(details, str) and ('furnished' in details.lower() or 'mobilado' in details.lower()):
                is_furnished = True
                
            record = {
                'url': row.get('url', None),
                'title': row.get('title', None),
                'price': row.get('price', None),
                'size': row.get('size', None),
                'rooms': row.get('num_rooms', None) if 'num_rooms' in row else row.get('rooms', None),
                'price_per_sqm': row.get('price_per_sqm', None),
                'location': location,
                'neighborhood': neighborhood,
                'details': details,
                'is_furnished': is_furnished,
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

def main():
    """Main entry point for the script."""
    logger.info("Starting data import to database")
    
    # Define file paths
    base_dir = Path(__file__).parent
    processed_dir = base_dir / "propbot" / "data" / "processed"
    sales_file = processed_dir / "sales_current.csv"
    rental_file = processed_dir / "rentals_current.csv"
    
    # Verify files exist
    if not sales_file.exists():
        logger.error(f"Sales file not found: {sales_file}")
        return False
    
    if not rental_file.exists():
        logger.error(f"Rental file not found: {rental_file}")
        return False
    
    # Get database connection
    conn = get_connection()
    if not conn:
        logger.error("Failed to connect to database")
        return False
    
    success = True
    try:
        # Create database schema if it doesn't exist
        if not create_database_schema(conn):
            logger.error("Failed to create database schema")
            return False
        
        # Import sales data
        if not import_sales_data(conn, sales_file):
            logger.warning("Failed to import sales data")
            success = False
        
        # Import rental data
        if not import_rental_data(conn, rental_file):
            logger.warning("Failed to import rental data")
            success = False
        
        if success:
            logger.info("Data import completed successfully")
        return success
    except Exception as e:
        logger.error(f"Error in data import: {str(e)}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(0 if main() else 1) 