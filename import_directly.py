#!/usr/bin/env python3
"""
Directly import sales data from CSV file to database.

This script reads the local CSV file and directly imports the data into the Heroku database.
"""

import os
import sys
import logging
import pandas as pd
import psycopg2
import subprocess
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_url():
    """Get the database URL from Heroku config."""
    try:
        result = subprocess.run(
            ['heroku', 'config:get', 'DATABASE_URL', '-a', 'propbot-investment-analyzer'],
            check=True, capture_output=True, text=True
        )
        db_url = result.stdout.strip()
        if not db_url:
            logger.error("Failed to retrieve DATABASE_URL")
            return None
        return db_url
    except Exception as e:
        logger.error(f"Error getting database URL: {e}")
        return None

def get_connection(db_url):
    """Connect to the database."""
    if not db_url:
        return None
    
    # Add SSL mode if not present
    if "sslmode=" not in db_url:
        db_url += "?sslmode=require"
    
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def import_sales_data(conn, csv_file):
    """Import sales data from CSV file to database."""
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        logger.info(f"Loaded {len(df)} rows from {csv_file}")
        
        # Print column names for debugging
        logger.info(f"CSV columns: {list(df.columns)}")
        
        # Prepare records for insertion
        records = []
        for _, row in df.iterrows():
            # Extract neighborhood from location if available
            neighborhood = None
            location = row.get('location', '')
            if isinstance(location, str) and ', ' in location:
                neighborhood = location.split(', ')[-1]
            
            # Convert snapshot_date to datetime if present
            snapshot_date = row.get('snapshot_date', None)
            if snapshot_date and isinstance(snapshot_date, str):
                try:
                    snapshot_date = datetime.strptime(snapshot_date, '%Y-%m-%d')
                except ValueError:
                    snapshot_date = datetime.now()
            else:
                snapshot_date = datetime.now()
            
            # Use room_type as details if details is not present
            details = row.get('details', row.get('room_type', ''))
            
            record = {
                'url': row.get('url', None),
                'title': row.get('title', None),
                'price': row.get('price', None),
                'size': row.get('size', None),
                'rooms': row.get('num_rooms', None),
                'price_per_sqm': row.get('price_per_sqm', None),
                'location': location,
                'neighborhood': neighborhood,
                'details': details,
                'snapshot_date': snapshot_date,
                'first_seen_date': snapshot_date
            }
            
            # Only include records with valid URL
            if record['url']:
                records.append(record)
        
        logger.info(f"Prepared {len(records)} records for insertion")
        
        if not records:
            logger.warning("No valid records found in CSV file")
            return False
        
        # Insert records into database
        with conn:
            with conn.cursor() as cur:
                # Create the SQL query template
                columns = list(records[0].keys())
                placeholders = ", ".join(["%s"] * len(columns))
                column_names = ", ".join(columns)
                
                update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'url'])
                update_set += ", updated_at = NOW()"
                
                query = f"""
                INSERT INTO properties_sales ({column_names})
                VALUES ({placeholders})
                ON CONFLICT (url) DO UPDATE SET {update_set}
                """
                
                # Execute batch insert
                inserted = 0
                updated = 0
                errors = 0
                
                for record in records:
                    try:
                        # Check if record exists
                        cur.execute("SELECT 1 FROM properties_sales WHERE url = %s", (record['url'],))
                        exists = cur.fetchone() is not None
                        
                        # Insert or update
                        values = [record[col] for col in columns]
                        cur.execute(query, values)
                        
                        if exists:
                            updated += 1
                        else:
                            inserted += 1
                    except Exception as e:
                        logger.error(f"Error inserting record {record['url']}: {e}")
                        errors += 1
                
                # Get the count after insertion
                cur.execute("SELECT COUNT(*) FROM properties_sales")
                count = cur.fetchone()[0]
                
                logger.info(f"Inserted {inserted} new records, updated {updated} existing records, errors: {errors}")
                logger.info(f"Total records in properties_sales table: {count}")
                
                return True
    except Exception as e:
        logger.error(f"Error importing sales data: {e}")
        return False

def main():
    """Main entry point for the script."""
    logger.info("Starting direct import of sales data to database")
    
    csv_file = "uploads/sales_current.csv"
    
    # Check if file exists
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        return 1
    
    # Get database URL
    db_url = get_db_url()
    if not db_url:
        return 1
    
    # Connect to database
    conn = get_connection(db_url)
    if not conn:
        return 1
    
    try:
        # Import sales data
        if not import_sales_data(conn, csv_file):
            logger.error("Failed to import sales data")
            return 1
        
        logger.info("Sales data import completed successfully")
        return 0
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(main()) 