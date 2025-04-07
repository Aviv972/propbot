#!/usr/bin/env python3
"""
Database Update Script for PropBot

This script updates the database with the latest sales and rental data
after a scraping operation is completed. It can be integrated into the
data processing pipeline.
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
import json
from propbot.data_processing.data_processor import extract_size
from propbot.utils.extraction_utils import extract_price_improved
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import database utilities and data import functions
try:
    from propbot.database_utils import get_connection, set_rental_last_update
    from propbot.db_data_import import (
        import_sales_data, 
        import_rental_data, 
        import_investment_metrics, 
        get_data_paths
    )
except ImportError:
    # Handle case when running from command line
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from propbot.database_utils import get_connection, set_rental_last_update
    from propbot.db_data_import import (
        import_sales_data, 
        import_rental_data, 
        import_investment_metrics, 
        get_data_paths
    )

def update_database_after_scrape(scrape_type=None):
    """
    Update the database with the latest scraped data.
    
    Args:
        scrape_type: Type of data to update ('sales', 'rentals', or None for both)
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Updating database with {scrape_type or 'all'} data")
    
    conn = get_connection()
    if not conn:
        logger.error("Failed to connect to database")
        return False
    
    # Get paths for data files
    paths = get_data_paths()
    success = True
    
    try:
        # Update sales data if requested or no specific type
        if scrape_type is None or scrape_type.lower() == 'sales':
            # First try to load directly from JSON (raw data)
            raw_sales_file = Path(paths.get('raw_dir', '.')) / 'sales' / 'idealista_listings.json'
            
            # Also look in common locations if not found
            if not raw_sales_file.exists():
                possible_paths = [
                    Path('propbot/data/raw/sales/idealista_listings.json'),
                    Path('data/raw/sales/idealista_listings.json'),
                    Path('idealista_listings.json'),
                    Path('/tmp/propbot_sales/idealista_listings.json'),  # Add temporary directory
                    Path('/tmp/idealista_listings.json')  # Add alternative temporary location
                ]
                
                for path in possible_paths:
                    if path.exists():
                        raw_sales_file = path
                        logger.info(f"Found sales data at: {path}")
                        break
            
            # Try importing from JSON first (faster and more direct)
            if raw_sales_file.exists():
                logger.info(f"Found raw sales data JSON at {raw_sales_file}")
                if import_sales_data_from_json(conn, raw_sales_file):
                    logger.info("Successfully imported sales data from JSON")
                else:
                    # Fall back to CSV if JSON import fails
                    logger.warning("Failed to import sales data from JSON, trying CSV...")
                    if not import_sales_data(conn, paths['processed_dir']):
                        logger.warning("Failed to update sales data in database")
                        success = False
            else:
                # Fall back to CSV if no JSON is found
                logger.info("No raw sales JSON found, trying processed CSV...")
                if not import_sales_data(conn, paths['processed_dir']):
                    logger.warning("Failed to update sales data in database")
                    success = False
        
        # Update rental data if requested or no specific type
        if scrape_type is None or scrape_type.lower() == 'rentals':
            if not import_rental_data(conn, paths['processed_dir']):
                logger.warning("Failed to update rental data in database")
                success = False
        
        return success
    except Exception as e:
        logger.error(f"Error updating database: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        if conn:
            conn.close()

def import_sales_data_from_json(conn, json_file_path):
    """Import sales data from JSON to database"""
    logger.info(f"Importing sales data from {json_file_path}")
    
    try:
        # Read JSON file
        with open(json_file_path, 'r') as f:
            sales_data = json.load(f)
        
        logger.info(f"Loaded {len(sales_data)} items from {json_file_path}")
        
        records = []
        valid_count = 0
        invalid_price_count = 0
        
        for item in sales_data:
            # Skip if missing required fields
            if not item.get('url'):
                logger.warning(f"Skipping item without URL: {item}")
                continue
            
            # Extract and validate price 
            price_str = item.get('price', '')
            price_value = None
            
            # Try to directly extract price 
            if isinstance(price_str, (int, float)):
                price_value = float(price_str)
                logger.info(f"Direct numeric price: {price_value} for {item.get('url')}")
            else:
                # Extract price using improved algorithm
                price_value = extract_price_improved(price_str)
                logger.info(f"Extracted price from string: {price_str} -> {price_value} for {item.get('url')}")
            
            # Ensure price is numeric
            try:
                price_value = float(price_value) if price_value is not None else 0
                logger.info(f"Converted price to numeric: {price_value} for {item.get('url')}")
            except (ValueError, TypeError):
                logger.warning(f"Could not convert price to numeric: {price_value} for {item.get('url')}")
                price_value = 0
            
            # Extract other data
            try:
                size_value, size_confidence = extract_size(item.get('details', ''))
                logger.debug(f"Extracted size: {size_value} (confidence: {size_confidence}) for {item.get('url')}")
            except Exception as e:
                logger.warning(f"Error extracting size: {e} for {item.get('url')}")
                size_value = None
                size_confidence = False
                
            # Create record
            record = {
                'url': item.get('url'),
                'title': item.get('title', ''),
                'price': price_value,  # Now guaranteed to be numeric
                'size': size_value if size_value else 0,
                'rooms': 0,
                'price_per_sqm': (price_value / size_value) if price_value and size_value and size_value > 0 else 0,
                'location': item.get('location', ''),
                'neighborhood': item.get('location', '').split(', ')[-1] if item.get('location') and ', ' in item.get('location', '') else '',
                'details': item.get('details', ''),
                'snapshot_date': item.get('last_updated', datetime.now().strftime('%Y-%m-%d')),
                'first_seen_date': item.get('first_seen_date', item.get('last_updated', datetime.now().strftime('%Y-%m-%d')))
            }
            
            records.append(record)
            if price_value and price_value > 0:
                valid_count += 1
                logger.info(f"Valid price record: {record['url']} - Price: {price_value}")
            else:
                invalid_price_count += 1
                logger.warning(f"Invalid price for property: {item.get('url')} - Raw price: {price_str}")
        
        logger.info(f"Prepared {len(records)} records for import ({valid_count} with valid prices, {invalid_price_count} with invalid prices)")

        if not records:
            logger.warning("No valid sales records found in JSON file")
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
                            logger.info(f"Updated record: {record['url']} - Price: {record['price']}")
                        else:
                            inserted += 1
                            logger.info(f"Inserted record: {record['url']} - Price: {record['price']}")
                    except Exception as e:
                        logger.error(f"Error inserting/updating record for {record['url']}: {e}")
                        continue
                
                logger.info(f"Database import complete: {inserted} inserted, {updated} updated")
                return True
                
    except Exception as e:
        logger.error(f"Error importing sales data: {e}")
        return False

def main():
    """Main entry point for the script"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Update database after scrape')
    parser.add_argument('--type', choices=['sales', 'rentals', 'both'], 
                        default='both', help='Type of scrape')
    
    args = parser.parse_args()
    scrape_type = None if args.type == 'both' else args.type
    
    success = update_database_after_scrape(scrape_type)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 