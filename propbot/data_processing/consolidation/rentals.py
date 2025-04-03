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
from decimal import Decimal
import pandas as pd

# Import from utils module
from propbot.data_processing.utils import save_json as utils_save_json
from propbot.data_processing.utils import PathJSONEncoder

# Set up logging
logger = logging.getLogger(__name__)

def convert_decimal_to_float(value: Any) -> Any:
    """Convert Decimal values to float."""
    if isinstance(value, Decimal):
        return float(value)
    return value

def convert_numeric_values(data: Any) -> Any:
    """Recursively convert all Decimal values to float in a data structure."""
    if isinstance(data, dict):
        return {k: convert_numeric_values(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numeric_values(v) for v in data]
    return convert_decimal_to_float(data)

def save_json_file(data: Any, file_path: str, indent: int = 2) -> bool:
    """Save data to a JSON file, ensuring numeric values are converted to float."""
    try:
        # Use PathJSONEncoder which handles datetime and Decimal values
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, cls=PathJSONEncoder, ensure_ascii=False)
        return True
    except Exception as e:
        logger.error(f"Error saving JSON file: {e}")
        return False

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

def consolidate_rentals(input_path: Union[str, Path], output_path: Union[str, Path], db_listings: List[Dict[str, Any]] = None) -> bool:
    """
    Consolidate rental listings from database and file sources.
    
    Args:
        input_path: Path to directory containing rental listing files
        output_path: Path to save consolidated listings (not used, kept for backward compatibility)
        db_listings: Optional list of rental listings from database
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Starting rentals consolidation from {input_path}")
        
        # Get database connection
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return False
            
        try:
            # Load listings from database if provided
            if db_listings:
                # Convert any Decimal values to float
                db_listings = convert_numeric_values(db_listings)
                
                # Insert or update listings in database
                with conn.cursor() as cur:
                    for listing in db_listings:
                        if not listing.get('url'):
                            continue
                            
                        # Standardize the listing
                        listing = standardize_rental_listing(listing)
                        
                        # Create SQL query for insert with ON CONFLICT DO UPDATE
                        columns = listing.keys()
                        values_template = '(' + ','.join(['%s'] * len(columns)) + ')'
                        
                        upsert_query = (
                            f"INSERT INTO properties_rentals ({','.join(columns)}) VALUES {values_template} "
                            f"ON CONFLICT (url) DO UPDATE SET "
                            f"{', '.join(f'{col} = EXCLUDED.{col}' for col in columns if col != 'url')}, "
                            f"updated_at = NOW()"
                        )
                        
                        try:
                            values = [listing[col] for col in columns]
                            cur.execute(upsert_query, values)
                        except Exception as e:
                            logger.error(f"Error inserting record {listing['url']}: {str(e)}")
                            continue
                
                logger.info(f"Processed {len(db_listings)} listings from database")
            
            # Load listings from input file if it exists
            if os.path.exists(input_path):
                try:
                    file_listings = load_json_file(input_path)
                    if file_listings is None:
                        logger.error("Failed to load input file")
                        return False
                    
                    # Handle both list and dict formats
                    if isinstance(file_listings, dict) and 'listings' in file_listings:
                        file_listings = file_listings['listings']
                    elif not isinstance(file_listings, list):
                        file_listings = [file_listings]
                    
                    # Process each listing
                    with conn.cursor() as cur:
                        for listing in file_listings:
                            if not listing.get('url'):
                                continue
                                
                            # Standardize the listing
                            listing = standardize_rental_listing(listing)
                            
                            # Create SQL query for insert with ON CONFLICT DO UPDATE
                            columns = listing.keys()
                            values_template = '(' + ','.join(['%s'] * len(columns)) + ')'
                            
                            upsert_query = (
                                f"INSERT INTO properties_rentals ({','.join(columns)}) VALUES {values_template} "
                                f"ON CONFLICT (url) DO UPDATE SET "
                                f"{', '.join(f'{col} = EXCLUDED.{col}' for col in columns if col != 'url')}, "
                                f"updated_at = NOW()"
                            )
                            
                            try:
                                values = [listing[col] for col in columns]
                                cur.execute(upsert_query, values)
                            except Exception as e:
                                logger.error(f"Error inserting record {listing['url']}: {str(e)}")
                                continue
                    
                    logger.info(f"Processed {len(file_listings)} listings from file")
                except Exception as e:
                    logger.error(f"Error processing input file: {e}")
                    # Continue with what we have from database
            
            # Commit the transaction
            conn.commit()
            logger.info("Rental data consolidation completed successfully")
            return True
            
        finally:
            conn.close()
            
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