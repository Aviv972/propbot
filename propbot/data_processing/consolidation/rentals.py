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
    Consolidate rental listings from multiple sources into a single file.
    
    Args:
        primary_file: Path to the primary rental listings file
        consolidated_file: Path to save the consolidated output
        raw_dir: Optional directory to search for additional rental files
        legacy_csv: Optional path to legacy CSV data
        
    Returns:
        True if consolidation was successful, False otherwise
    """
    all_new_listings = []
    sources_info = {}
    
    # 1. Load listings from the primary rental file
    if os.path.exists(primary_file):
        primary_data = load_json_file(primary_file)
        if primary_data is not None:
            # Handle different data structures
            if isinstance(primary_data, list):
                # Check if this is a list of dictionaries with a 'listings' key
                if primary_data and isinstance(primary_data[0], dict) and 'listings' in primary_data[0]:
                    # Extract listings from each month entry
                    primary_listings = []
                    for month_entry in primary_data:
                        if 'listings' in month_entry and isinstance(month_entry['listings'], list):
                            primary_listings.extend(month_entry['listings'])
                else:
                    primary_listings = primary_data
            elif isinstance(primary_data, dict) and 'listings' in primary_data:
                primary_listings = primary_data['listings']
            else:
                primary_listings = [primary_data]
                
            # Standardize listings
            for listing in primary_listings:
                if isinstance(listing, dict):
                    all_new_listings.append(standardize_rental_listing(listing))
                    
            sources_info['primary'] = len(primary_listings)
            logger.info(f"Loaded {len(primary_listings)} rental listings from primary file: {os.path.basename(primary_file)}")
        else:
            logger.warning("Failed to load primary rental data.")
            sources_info['primary'] = 0
    else:
        logger.warning(f"Primary rental file not found: {primary_file}")
        sources_info['primary'] = 0
    
    # 2. Load additional rental files from the raw directory
    if raw_dir and os.path.isdir(raw_dir):
        for file_path in glob.glob(os.path.join(raw_dir, "*.json")):
            # Skip the primary file if it's in the same directory
            if os.path.abspath(file_path) == os.path.abspath(primary_file):
                continue
                
            # Skip files that are clearly not property listings
            file_basename = os.path.basename(file_path)
            if (file_basename.endswith('credits_usage.json') or 
                file_basename.endswith('metadata.json') or 
                file_basename.endswith('rental_credits_usage.json') or
                file_basename.endswith('_metadata.json')):
                logger.info(f"Skipping non-property file: {file_basename}")
                continue
                
            data = load_json_file(file_path)
            if data:
                # Check if this is a list of dictionaries with a 'listings' key
                if isinstance(data, list) and data and isinstance(data[0], dict) and 'listings' in data[0]:
                    # Extract listings from each month entry
                    file_listings = []
                    for month_entry in data:
                        if 'listings' in month_entry and isinstance(month_entry['listings'], list):
                            file_listings.extend(month_entry['listings'])
                    logger.info(f"Loaded {len(file_listings)} rental listings from additional file: {file_basename}")
                    all_new_listings.extend(file_listings)
                # Verify this is property data (should be a list of dicts)
                elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
                    logger.info(f"Loaded {len(data)} rental listings from additional file: {file_basename}")
                    all_new_listings.extend(data)
                else:
                    logger.warning(f"Skipping file with invalid format: {file_basename}")
    
    # 3. Load legacy CSV data if provided
    if legacy_csv and os.path.exists(legacy_csv):
        csv_data = load_csv_file(legacy_csv)
        if csv_data is not None:
            # Convert CSV data to the standard format
            csv_listings = []
            for row in csv_data:
                # Create a standardized listing from CSV row
                listing = {
                    'url': row.get('url', ''),
                    'title': row.get('title', ''),
                    'location': row.get('location', ''),
                    'rent_price': row.get('rent_price', row.get('price', '')),
                    'size': row.get('size', ''),
                    'num_rooms': row.get('num_rooms', row.get('rooms', '')),
                    'details': row.get('details', ''),
                    'snapshot_date': row.get('snapshot_date', datetime.now().strftime("%Y-%m-%d"))
                }
                
                if listing['url']:  # Only add listings with a URL
                    csv_listings.append(listing)
            
            all_new_listings.extend(csv_listings)
            sources_info['legacy_csv'] = len(csv_listings)
            logger.info(f"Loaded {len(csv_listings)} rental listings from legacy CSV: {legacy_csv}")
    
    # Exit if no data was loaded
    if not all_new_listings:
        logger.error("No rental listings found in any source. Exiting.")
        return False
    
    logger.info(f"Total new listings from all sources: {len(all_new_listings)}")
    
    # 4. Load existing consolidated data
    consolidated_data = load_existing_consolidated_data(consolidated_file)
    
    # 5. Create a URL index for quick lookup of existing entries
    url_index = {item['url']: i for i, item in enumerate(consolidated_data) if 'url' in item}
    
    # 6. Process new listings
    added_count = 0
    updated_count = 0
    skipped_count = 0
    
    for listing in all_new_listings:
        if 'url' not in listing or not listing['url']:
            logger.warning("Skipping listing without URL")
            skipped_count += 1
            continue
        
        url = listing['url']
        
        if url in url_index:
            # Update existing entry
            existing_index = url_index[url]
            consolidated_data[existing_index] = listing
            updated_count += 1
        else:
            # Add new entry
            consolidated_data.append(listing)
            url_index[url] = len(consolidated_data) - 1
            added_count += 1
    
    logger.info(f"Added {added_count} new listings, updated {updated_count} existing listings, skipped {skipped_count} invalid listings.")
    logger.info(f"Total consolidated rental listings: {len(consolidated_data)}")
    
    # 7. Save consolidated data
    if save_json_file(consolidated_data, consolidated_file):
        logger.info(f"Successfully consolidated rental data to {consolidated_file}")
        
        # 8. Also create a metadata file
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'rental_count': len(consolidated_data),
            'sources': sources_info,
            'history': []
        }
        
        # Load existing metadata if available
        metadata_file = str(consolidated_file).replace('.json', '_metadata.json')
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    existing_metadata = json.load(f)
                if 'history' in existing_metadata:
                    metadata['history'] = existing_metadata['history']
            except Exception as e:
                logger.warning(f"Could not load existing metadata: {e}")
        
        # Add new history entry
        metadata['history'].append({
            'timestamp': datetime.now().isoformat(),
            'added': added_count,
            'updated': updated_count,
            'skipped': skipped_count,
            'sources': sources_info,
            'total': len(consolidated_data)
        })
        
        save_json_file(metadata, metadata_file)
        
        return True
    else:
        logger.error("Failed to save consolidated data.")
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