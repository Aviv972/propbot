#!/usr/bin/env python3
"""
Sales Data Consolidation

This module provides functions to consolidate sales listings from multiple sources 
into a single comprehensive file, handling duplicate entries based on URL.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
import glob
from typing import List, Dict, Any, Optional

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

def consolidate_sales(primary_file: str, consolidated_file: str, raw_dir: Optional[str] = None, legacy_csv: Optional[str] = None) -> Dict[str, Any]:
    """
    Consolidate sales listings from multiple sources into a single file.
    
    Args:
        primary_file: Path to the primary sales listing file
        consolidated_file: Path to save the consolidated file
        raw_dir: Directory containing additional sales files
        legacy_csv: Optional path to a legacy CSV file to include
        
    Returns:
        Metadata about the consolidation process, or None if failed
    """
    logger.info(f"Starting sales consolidation from {primary_file} to {consolidated_file}")
    
    # Counter for tracking additions and updates
    stats = {
        "new_listings": 0,
        "updated_listings": 0,
        "skipped_listings": 0
    }
    
    # Load existing consolidated data if available
    existing_listings = load_existing_consolidated_data(consolidated_file)
    
    # Create URL index for quick lookup
    url_index = {listing.get('url'): i for i, listing in enumerate(existing_listings) if listing.get('url')}
    
    # Track new and updated listings
    all_new_listings = []
    
    # Process main file
    if os.path.exists(primary_file):
        primary_data = load_json_file(primary_file)
        if primary_data:
            logger.info(f"Loaded {len(primary_data)} sales listings from primary file")
            all_new_listings.extend(primary_data)
        else:
            logger.warning(f"No data loaded from primary file: {primary_file}")
    else:
        logger.warning(f"Primary sales file not found: {primary_file}")
    
    # Process additional sales files from raw directory
    if raw_dir and os.path.isdir(raw_dir):
        # First check the main raw directory
        for file_path in glob.glob(os.path.join(raw_dir, "*.json")):
            # Skip the primary file if it's in the same directory
            if os.path.abspath(file_path) == os.path.abspath(primary_file):
                continue
                
            # Skip files that are clearly not property listings
            file_basename = os.path.basename(file_path)
            if (file_basename.endswith('credits_usage.json') or 
                file_basename.endswith('metadata.json') or 
                file_basename.endswith('rental_credits_usage.json') or
                file_basename.endswith('test_sales.json') or  # Skip test data
                file_basename.endswith('_metadata.json')):
                logger.info(f"Skipping non-property file: {file_basename}")
                continue
                
            data = load_json_file(file_path)
            if data:
                # Verify this is property data (should be a list of dicts)
                if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                    logger.info(f"Loaded {len(data)} sales listings from additional file: {os.path.basename(file_path)}")
                    all_new_listings.extend(data)
                else:
                    logger.warning(f"Skipping file with invalid format: {file_basename}")
                
        # Also check for historical snapshots in the history subdirectory
        history_dir = os.path.join(raw_dir, "history")
        if os.path.isdir(history_dir):
            # Sort files by date to process oldest first (to maintain property history)
            history_files = sorted(glob.glob(os.path.join(history_dir, "*.json")))
            for file_path in history_files:
                # Skip files that are clearly not property listings
                file_basename = os.path.basename(file_path)
                if (file_basename.endswith('credits_usage.json') or 
                    file_basename.endswith('metadata.json') or 
                    file_basename.endswith('rental_credits_usage.json') or
                    file_basename.endswith('test_sales.json') or  # Skip test data
                    file_basename.endswith('_metadata.json')):
                    logger.info(f"Skipping non-property file: {file_basename}")
                    continue
                    
                data = load_json_file(file_path)
                if data:
                    # Verify this is property data (should be a list of dicts)
                    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                        logger.info(f"Loaded {len(data)} sales listings from history file: {os.path.basename(file_path)}")
                        all_new_listings.extend(data)
                    else:
                        logger.warning(f"Skipping history file with invalid format: {file_basename}")
    
    # Process legacy CSV file if provided (we don't implement this yet, just log)
    if legacy_csv and os.path.exists(legacy_csv):
        logger.info(f"Legacy CSV processing not implemented yet. File: {legacy_csv}")
    
    logger.info(f"Total new listings from all sources: {len(all_new_listings)}")
    
    # Process each listing
    for listing in all_new_listings:
        # Skip listings without URLs
        if not listing.get('url'):
            logger.warning("Skipping listing without URL")
            stats["skipped_listings"] += 1
            continue
        
        # Add standardized fields if not present
        if 'is_rental' not in listing:
            listing['is_rental'] = False
        
        if 'snapshot_date' not in listing:
            listing['snapshot_date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Check if this listing already exists
        url = listing.get('url')
        if url in url_index:
            # Update existing listing
            existing_idx = url_index[url]
            # Only update if there are differences
            if listing != existing_listings[existing_idx]:
                existing_listings[existing_idx] = listing
                stats["updated_listings"] += 1
        else:
            # Add new listing
            existing_listings.append(listing)
            url_index[url] = len(existing_listings) - 1
            stats["new_listings"] += 1
    
    # Save consolidated data
    if save_json_file(existing_listings, consolidated_file):
        logger.info(f"Successfully consolidated sales data to {consolidated_file}")
        
        # Create and save metadata
        metadata = {
            "source_files": [primary_file],
            "raw_directory": raw_dir,
            "legacy_csv": legacy_csv,
            "output_file": consolidated_file,
            "total_listings": len(existing_listings),
            "new_listings": stats["new_listings"],
            "updated_listings": stats["updated_listings"],
            "skipped_listings": stats["skipped_listings"],
            "timestamp": datetime.now().isoformat()
        }
        
        metadata_file = f"{os.path.splitext(consolidated_file)[0]}_metadata.json"
        if save_json_file(metadata, metadata_file):
            logger.info(f"Successfully saved {metadata_file}")
        
        return metadata
    else:
        logger.error(f"Failed to save consolidated file: {consolidated_file}")
        return None

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
    raw_dir = data_dir / "raw" / "sales" 
    processed_dir = data_dir / "processed"
    
    primary_sales_file = base_dir.parent / "idealista_listings.json"
    consolidated_json = processed_dir / "sales_listings_consolidated.json"
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Consolidate sales data from multiple sources")
    parser.add_argument("--primary", default=str(primary_sales_file), help="Primary sales file path")
    parser.add_argument("--output", default=str(consolidated_json), help="Output consolidated file path")
    parser.add_argument("--raw-dir", default=str(raw_dir), help="Directory with additional sales files")
    
    args = parser.parse_args()
    
    # Print directories for debugging
    logger.info(f"BASE_DIR: {base_dir}")
    logger.info(f"RAW_DIR: {args.raw_dir}")
    logger.info(f"PROCESSED_DIR: {processed_dir}")
    logger.info(f"PRIMARY_SALES_FILE exists: {os.path.exists(args.primary)}")
    
    # Run the consolidation
    success = consolidate_sales(args.primary, args.output, args.raw_dir)
    exit(0 if success else 1) 