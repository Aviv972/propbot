#!/usr/bin/env python3
"""
Rental Data Consolidation

This module provides functions to consolidate rental listings from multiple sources 
into a single comprehensive file, handling duplicate entries based on URL.
"""

import os
import json
import logging
import csv
from datetime import datetime
from pathlib import Path
import glob
from typing import List, Dict, Any, Optional, Union

# Import from utils module
from propbot.data_processing.utils import save_json as utils_save_json
from propbot.analysis.metrics.db_functions import get_rental_listings_from_database

# Set up logging
logger = logging.getLogger(__name__)

def load_json_file(file_path: str) -> Optional[List[Dict[str, Any]]]:
    """
    Load JSON data from a file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Loaded JSON data or None if loading fails
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON file: {e}")
        return None

def load_csv_file(file_path: str) -> Optional[List[Dict[str, Any]]]:
    """
    Load CSV data from a file and convert to list of dictionaries.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Loaded CSV data as list of dictionaries or None if loading fails
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        return None

def save_json_file(data: Any, file_path: str, indent: int = 2) -> bool:
    """
    Save data to a JSON file.
    
    Args:
        data: Data to save
        file_path: Path to save the JSON file
        indent: JSON indentation level
        
    Returns:
        True if successful, False otherwise
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Use the utility function that handles Path objects
    return utils_save_json(data, file_path, indent)

def load_existing_consolidated_data(consolidated_json_path: str) -> List[Dict[str, Any]]:
    """
    Load existing consolidated data or create empty list.
    
    Args:
        consolidated_json_path: Path to the consolidated JSON file
        
    Returns:
        List of consolidated listings
    """
    if os.path.exists(consolidated_json_path):
        data = load_json_file(consolidated_json_path)
        if data is None:
            logger.warning(f"Could not load existing consolidated file. Creating new.")
            return []
        return data
    else:
        logger.info(f"No existing consolidated file found. Creating new.")
        return []

def standardize_rental_listing(listing: Dict[str, Any]) -> Dict[str, Any]:
    """
    Standardize rental listing fields to ensure consistency.
    
    Args:
        listing: Raw rental listing dictionary
        
    Returns:
        Standardized rental listing
    """
    # Create a new dictionary with standardized fields
    standardized = {}
    
    # Copy existing fields with standard names
    for field in ['url', 'title', 'location', 'rent_price', 'size', 'num_rooms', 'details']:
        if field in listing:
            standardized[field] = listing[field]
    
    # Handle alternate field names
    if 'price' in listing and 'rent_price' not in standardized:
        standardized['rent_price'] = listing['price']
    
    if 'rooms' in listing and 'num_rooms' not in standardized:
        standardized['num_rooms'] = listing['rooms']
    
    # Ensure snapshot_date is present
    if 'snapshot_date' in listing:
        standardized['snapshot_date'] = listing['snapshot_date']
    else:
        standardized['snapshot_date'] = datetime.now().strftime("%Y-%m-%d")
    
    return standardized

def consolidate_rentals(primary_file: str, consolidated_file: str, raw_dir: Optional[str] = None, 
                        legacy_csv: Optional[str] = None) -> bool:
    """
    Consolidate rental listings from the database into a single file.
    
    Args:
        primary_file: Path to the primary rental listings file (unused)
        consolidated_file: Path to save the consolidated output
        raw_dir: Optional directory to search for additional rental files (unused)
        legacy_csv: Optional path to legacy CSV data (unused)
        
    Returns:
        True if consolidation was successful, False otherwise
    """
    try:
        # Get rental listings from database
        rental_listings = get_rental_listings_from_database()
        
        if not rental_listings:
            logger.error("No rental listings found in database")
            return False
            
        logger.info(f"Loaded {len(rental_listings)} rental listings from database")
        
        # Save consolidated data
        if save_json_file(rental_listings, consolidated_file):
            logger.info(f"Successfully consolidated rental data to {consolidated_file}")
            
            # Create metadata file
            metadata = {
                'last_updated': datetime.now().isoformat(),
                'rental_count': len(rental_listings),
                'sources': {'database': len(rental_listings)},
                'history': [{
                    'timestamp': datetime.now().isoformat(),
                    'added': len(rental_listings),
                    'updated': 0,
                    'skipped': 0,
                    'sources': {'database': len(rental_listings)},
                    'total': len(rental_listings)
                }]
            }
            
            metadata_file = str(consolidated_file).replace('.json', '_metadata.json')
            save_json_file(metadata, metadata_file)
            
            return True
        else:
            logger.error("Failed to save consolidated data")
            return False
            
    except Exception as e:
        logger.error(f"Error during rental consolidation: {e}")
        return False

# CLI entry point
if __name__ == "__main__":
    import argparse
    
    # Set up logging for command line use
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Set up default paths
    base_dir = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    data_dir = base_dir / "data"
    raw_dir = data_dir / "raw" / "rentals" 
    processed_dir = data_dir / "processed"
    
    primary_rentals_file = raw_dir / "rental_listings.json"
    consolidated_json = processed_dir / "rental_listings_consolidated.json"
    legacy_csv_file = base_dir.parent / "rental_complete.csv"
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Consolidate rental data from multiple sources")
    parser.add_argument("--primary", default=str(primary_rentals_file), help="Primary rental file path")
    parser.add_argument("--output", default=str(consolidated_json), help="Output consolidated file path")
    parser.add_argument("--raw-dir", default=str(raw_dir), help="Directory with additional rental files")
    parser.add_argument("--legacy-csv", default=str(legacy_csv_file), help="Legacy CSV file with rental data")
    
    args = parser.parse_args()
    
    # Print directories for debugging
    logger.info(f"BASE_DIR: {base_dir}")
    logger.info(f"RAW_DIR: {args.raw_dir}")
    logger.info(f"PROCESSED_DIR: {processed_dir}")
    logger.info(f"PRIMARY_RENTALS_FILE exists: {os.path.exists(args.primary)}")
    logger.info(f"LEGACY_CSV exists: {os.path.exists(args.legacy_csv)}")
    
    # Run the consolidation
    success = consolidate_rentals(args.primary, args.output, args.raw_dir, args.legacy_csv)
    exit(0 if success else 1) 