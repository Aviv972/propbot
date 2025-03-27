#!/usr/bin/env python3
"""
Script to add first_seen_date to property listings to track when they were first added to the dataset.
"""

import os
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
SALES_DIR = os.path.join(RAW_DIR, "sales")
BACKUPS_DIR = os.path.join(SALES_DIR, "backups")

# Ensure backup directory exists
os.makedirs(BACKUPS_DIR, exist_ok=True)

# Input/output files
SALES_FILE = os.path.join(SALES_DIR, "idealista_listings.json")

def backup_existing_file(file_path):
    """Create a backup of the existing file before modifying it."""
    if not os.path.exists(file_path):
        logger.warning(f"File not found, cannot create backup: {file_path}")
        return None
    
    # Create a timestamp for the backup filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Construct backup filename
    backup_filename = os.path.basename(file_path).split('.')[0]
    backup_path = os.path.join(BACKUPS_DIR, f"{backup_filename}_{timestamp}.json")
    
    # Copy the file
    import shutil
    shutil.copy2(file_path, backup_path)
    
    logger.info(f"Created backup at: {backup_path}")
    return backup_path

def load_sales_listings():
    """Load the sales listings from the JSON file."""
    try:
        with open(SALES_FILE, 'r', encoding='utf-8') as f:
            listings = json.load(f)
        logger.info(f"Loaded {len(listings)} sales listings from {SALES_FILE}")
        return listings
    except FileNotFoundError:
        logger.error(f"File not found: {SALES_FILE}")
        return []
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {SALES_FILE}")
        return []

def save_sales_listings(listings):
    """Save the updated sales listings to the JSON file."""
    try:
        with open(SALES_FILE, 'w', encoding='utf-8') as f:
            json.dump(listings, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(listings)} sales listings to {SALES_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error saving listings: {e}")
        return False

def add_first_seen_date(listings):
    """Add first_seen_date field to each listing if it doesn't exist."""
    updated_count = 0
    
    for listing in listings:
        if 'first_seen_date' not in listing:
            # If the listing doesn't have first_seen_date, use last_updated as the first_seen_date
            listing['first_seen_date'] = listing.get('last_updated', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            updated_count += 1
    
    logger.info(f"Added first_seen_date to {updated_count} listings")
    return listings

def main():
    """Main function to add first_seen_date to property listings."""
    logger.info("Starting process to add first_seen_date to listings")
    
    # Create backup of the existing file
    backup_path = backup_existing_file(SALES_FILE)
    if not backup_path:
        logger.warning("No backup was created. Proceeding with caution.")
    
    # Load sales listings
    listings = load_sales_listings()
    if not listings:
        logger.error("No listings found or error loading listings. Exiting.")
        return
    
    # Add first_seen_date to listings
    updated_listings = add_first_seen_date(listings)
    
    # Save updated listings
    success = save_sales_listings(updated_listings)
    
    if success:
        logger.info("Process completed successfully.")
    else:
        logger.error("Process failed when saving the updated listings.")

if __name__ == "__main__":
    main() 