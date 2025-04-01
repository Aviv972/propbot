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
    Update the database with the latest data after a scrape.
    
    Args:
        scrape_type: Type of scrape ('sales', 'rentals', or None for both)
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Updating database after {scrape_type if scrape_type else 'full'} scrape")
    
    # Get database connection
    conn = get_connection()
    if not conn:
        logger.error("Failed to connect to database")
        return False
    
    success = True
    try:
        # Get data paths
        paths = get_data_paths()
        
        # Update sales data if requested or no specific type
        if scrape_type is None or scrape_type.lower() == 'sales':
            if not import_sales_data(conn, paths['processed_dir']):
                logger.warning("Failed to update sales data in database")
                success = False
        
        # Update rental data if requested or no specific type
        if scrape_type is None or scrape_type.lower() == 'rentals':
            if not import_rental_data(conn, paths['processed_dir']):
                logger.warning("Failed to update rental data in database")
                success = False
            else:
                # Update rental last update timestamp in the database
                set_rental_last_update(datetime.now())
                logger.info("Updated rental last update timestamp")
        
        # Update investment metrics if both types were updated or no specific type
        if (scrape_type is None or 
            (scrape_type.lower() == 'sales' and scrape_type.lower() == 'rentals')):
            if not import_investment_metrics(conn):
                logger.warning("Failed to update investment metrics in database")
                success = False
        
        if success:
            logger.info("Database successfully updated after scrape")
        return success
    except Exception as e:
        logger.error(f"Error updating database after scrape: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

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