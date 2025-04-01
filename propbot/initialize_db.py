#!/usr/bin/env python3
"""
Initialize the database for PropBot
"""

import logging
import sys
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import database utilities
try:
    from propbot.database_utils import (
        initialize_database, 
        set_rental_last_update, 
        set_rental_update_frequency
    )
except ImportError:
    logger.error("Could not import database utilities. Make sure the project is installed properly.")
    sys.exit(1)

def main():
    """Initialize the database and set initial values"""
    logger.info("Initializing PropBot database...")
    
    # Initialize the database schema
    if not initialize_database():
        logger.error("Failed to initialize database schema")
        return False
    
    # Set initial rental last update (30 days ago to trigger an update on first run)
    thirty_days_ago = datetime.now() - timedelta(days=30)
    
    if not set_rental_last_update(thirty_days_ago):
        logger.error("Failed to set initial rental last update")
        return False
    
    # Set rental update frequency (default: 30 days)
    if not set_rental_update_frequency(30):
        logger.error("Failed to set rental update frequency")
        return False
    
    logger.info("PropBot database initialized successfully")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 