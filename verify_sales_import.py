#!/usr/bin/env python3
"""
Verify and fix sales data import

This script verifies the sales data CSV content and ensures all records are imported
"""

import os
import sys
import logging
import psycopg2
import pandas as pd
import subprocess
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

def check_local_sales_file():
    """Check the local sales CSV file and report statistics."""
    sales_file = Path("uploads/sales_current.csv")
    
    if not sales_file.exists():
        logger.error(f"Sales file not found: {sales_file}")
        return None
    
    try:
        df = pd.read_csv(sales_file)
        logger.info(f"Local sales CSV contains {len(df)} rows")
        
        # Print columns to debug
        logger.info(f"CSV columns: {list(df.columns)}")
        
        # Check for missing URLs
        missing_urls = df['url'].isna().sum()
        if missing_urls > 0:
            logger.warning(f"Found {missing_urls} rows with missing URLs in local file")
        
        # Check duplicate URLs
        duplicate_urls = df['url'].duplicated().sum()
        if duplicate_urls > 0:
            logger.warning(f"Found {duplicate_urls} duplicate URLs in local file")
            
        return df
    except Exception as e:
        logger.error(f"Error reading local sales file: {str(e)}")
        return None

def split_and_upload_file(app_name, chunk_size=50):
    """Split the CSV file into smaller chunks and upload each chunk."""
    sales_file = Path("uploads/sales_current.csv")
    
    try:
        # Read the full CSV
        df = pd.read_csv(sales_file)
        total_rows = len(df)
        logger.info(f"Splitting CSV with {total_rows} rows into chunks of {chunk_size}")
        
        # Create temp directory for chunks
        temp_dir = Path("temp_chunks")
        temp_dir.mkdir(exist_ok=True)
        
        # Create chunk files
        chunk_files = []
        for i in range(0, total_rows, chunk_size):
            end = min(i + chunk_size, total_rows)
            chunk_df = df.iloc[i:end]
            chunk_file = temp_dir / f"sales_chunk_{i}_{end}.csv"
            chunk_df.to_csv(chunk_file, index=False)
            chunk_files.append(chunk_file)
            logger.info(f"Created chunk file {chunk_file} with {len(chunk_df)} rows")
        
        # Create directory on Heroku
        subprocess.run(['heroku', 'run', 'mkdir -p sales_chunks', '-a', app_name], 
                      check=True, capture_output=True)
        
        # Upload each chunk
        for chunk_file in chunk_files:
            logger.info(f"Uploading chunk {chunk_file}")
            remote_path = f"sales_chunks/{chunk_file.name}"
            with open(chunk_file, 'rb') as f:
                upload_process = subprocess.run(
                    ['heroku', 'run', '--no-tty', f'cat > {remote_path}', '-a', app_name],
                    input=f.read(), check=True, capture_output=True
                )
        
        return chunk_files
    except Exception as e:
        logger.error(f"Error splitting and uploading files: {str(e)}")
        return None

def direct_import_chunks(app_name, chunk_files):
    """Directly import chunks using psql COPY command."""
    try:
        # First create a SQL script to import the chunks
        import_script = Path("import_sales.sql")
        with open(import_script, "w") as f:
            f.write("-- Import sales data from CSV chunks\n")
            f.write("BEGIN;\n")
            
            # Create temp table with same structure as the CSV
            f.write("""
CREATE TEMP TABLE temp_sales (
    url TEXT,
    title TEXT,
    location TEXT,
    price NUMERIC,
    size NUMERIC,
    num_rooms INTEGER,
    price_per_sqm NUMERIC,
    room_type TEXT,
    snapshot_date TEXT,
    details TEXT,
    first_seen_date TEXT
);
""")
            
            # Import from each chunk file
            for chunk_file in chunk_files:
                remote_path = f"/app/sales_chunks/{chunk_file.name}"
                f.write(f"\n\\COPY temp_sales(url, title, location, price, size, num_rooms, price_per_sqm, room_type, snapshot_date) FROM '{remote_path}' WITH (FORMAT csv, HEADER true);\n")
            
            # Insert into actual table with conflict handling
            f.write("""
INSERT INTO properties_sales (
    url, title, price, size, rooms, price_per_sqm, 
    location, neighborhood, details, snapshot_date, first_seen_date
)
SELECT 
    url, 
    title, 
    price, 
    size, 
    num_rooms, 
    price_per_sqm, 
    location, 
    CASE WHEN location LIKE '%,%' THEN split_part(location, ', ', -1) ELSE NULL END as neighborhood,
    room_type as details, 
    CASE 
        WHEN snapshot_date ~ E'^\\\\d{4}-\\\\d{2}-\\\\d{2}$' THEN snapshot_date::timestamp 
        ELSE NOW() 
    END as snapshot_date,
    CASE 
        WHEN snapshot_date ~ E'^\\\\d{4}-\\\\d{2}-\\\\d{2}$' THEN snapshot_date::timestamp 
        ELSE NOW() 
    END as first_seen_date
FROM temp_sales
WHERE url IS NOT NULL
ON CONFLICT (url) DO UPDATE SET
    title = EXCLUDED.title,
    price = EXCLUDED.price,
    size = EXCLUDED.size,
    rooms = EXCLUDED.rooms,
    price_per_sqm = EXCLUDED.price_per_sqm,
    location = EXCLUDED.location,
    neighborhood = EXCLUDED.neighborhood,
    details = EXCLUDED.details,
    snapshot_date = EXCLUDED.snapshot_date,
    updated_at = NOW();
""")
            
            # Check counts and commit
            f.write("SELECT COUNT(*) FROM properties_sales;\n")
            f.write("COMMIT;\n")
        
        # Upload the SQL script
        logger.info("Uploading SQL import script to Heroku")
        with open(import_script, 'rb') as f:
            upload_process = subprocess.run(
                ['heroku', 'run', '--no-tty', 'cat > import_sales.sql', '-a', app_name],
                input=f.read(), check=True, capture_output=True
            )
        
        # Run the SQL script using psql
        logger.info("Running SQL import script on Heroku")
        psql_process = subprocess.run(
            ['heroku', 'pg:psql', '-a', app_name, '-f', 'import_sales.sql'],
            check=True, capture_output=True, text=True
        )
        
        logger.info(f"SQL import result:\n{psql_process.stdout}")
        
        return True
    except Exception as e:
        logger.error(f"Error with direct import: {str(e)}")
        if 'psql_process' in locals() and psql_process.stderr:
            logger.error(f"SQL error: {psql_process.stderr}")
        return False

def main():
    """Main function to verify and fix sales data import."""
    app_name = 'propbot-investment-analyzer'
    
    # Check local file
    logger.info("Checking local sales file")
    local_df = check_local_sales_file()
    if local_df is None:
        return 1
    
    # Split and upload file in chunks
    logger.info("Splitting and uploading files")
    chunk_files = split_and_upload_file(app_name)
    if not chunk_files:
        return 1
    
    # Import chunks directly
    logger.info("Importing chunks directly using psql")
    if not direct_import_chunks(app_name, chunk_files):
        return 1
    
    # Query the database for final count
    logger.info("Checking final record count in database")
    try:
        query_process = subprocess.run(
            ['heroku', 'pg:psql', '-a', app_name, '-c', 'SELECT COUNT(*) FROM properties_sales;'],
            check=True, capture_output=True, text=True
        )
        logger.info(f"Final count result:\n{query_process.stdout}")
    except Exception as e:
        logger.error(f"Error checking final count: {str(e)}")
    
    logger.info("Sales data verification and import completed")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 