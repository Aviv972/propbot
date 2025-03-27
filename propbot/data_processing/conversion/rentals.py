#!/usr/bin/env python3
"""
Convert Consolidated Rental Data to CSV

This module provides functions to convert consolidated rental data from JSON 
format to a standardized CSV format for use in rental analysis.
"""

import os
import re
import csv
import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Union
from ...utils.extraction_utils import extract_size as extract_size_robust
from ...utils.extraction_utils import extract_room_type as extract_room_type_robust

# Set up logging
logger = logging.getLogger(__name__)

def extract_price(price_str: Optional[str]) -> int:
    """
    Extract the rental price from a string and convert to numeric value.
    
    Args:
        price_str: String containing price information
        
    Returns:
        Monthly rental price in euros as an integer (e.g., "1,400€/month" -> 1400)
    """
    if not price_str or not isinstance(price_str, str):
        return 0
    
    # Try to extract the numeric part of the price
    price_match = re.search(r'[\d,.]+', price_str.replace(' ', ''))
    if not price_match:
        return 0
    
    price_str = price_match.group(0)
    
    # Remove non-numeric characters except for the decimal point/comma
    price_str = re.sub(r'[^\d.,]', '', price_str)
    
    # Replace comma with dot for decimal
    if ',' in price_str and '.' in price_str:
        # If both comma and dot exist, assume comma is thousands separator
        price_str = price_str.replace(',', '')
    else:
        # Otherwise, treat comma as decimal point
        price_str = price_str.replace(',', '.')
    
    try:
        price = float(price_str)
        
        # For rental prices, if the value is very small (like 1.40),
        # it might actually mean 1400 (units confusion)
        if price < 100:
            price *= 1000
            
        return int(price)  # Return as integer
    except (ValueError, TypeError):
        logger.warning(f"Could not convert price string: {price_str}")
        return 0

def extract_size(size_str):
    """
    Extract numeric size value from a string.
    
    Note: This function is deprecated and remains only for backward compatibility.
    Use propbot.utils.extraction_utils.extract_size instead.
    
    Args:
        size_str: String containing size information (e.g., "120 m²")
        
    Returns:
        Integer representing square meters or 0 if invalid
    """
    if not size_str:
        return 0
    
    # Use the robust implementation from extraction_utils
    size, _ = extract_size_robust(size_str)
    return int(size) if size is not None else 0

def extract_room_type(title):
    """
    Extract room type from property title.
    
    Note: This function is deprecated and remains only for backward compatibility.
    Use propbot.utils.extraction_utils.extract_room_type instead.
    
    Args:
        title: Property title that may contain room type (e.g., "T2 apartment")
        
    Returns:
        Room type string (e.g., "T2") or empty string if not found
    """
    if not title:
        return ""
    
    # Use the robust implementation from extraction_utils
    room_type = extract_room_type_robust(title)
    return room_type if room_type else ""

def extract_location(location_str: Optional[str], title: Optional[str] = None) -> str:
    """
    Extract a clean location string.
    
    Args:
        location_str: String containing location information
        title: Optional title to check for location
        
    Returns:
        Cleaned location string
    """
    # Check the location string
    if location_str and isinstance(location_str, str):
        return location_str.strip()
    
    # Try to extract from title as fallback
    if title and isinstance(title, str):
        # Look for "in Location" pattern
        location_match = re.search(r'in\s+([^,]+)(?:,|$)', title)
        if location_match:
            return location_match.group(1).strip()
    
    return ""

def convert_rentals(input_path: Union[str, Path], output_path: Union[str, Path]) -> bool:
    """
    Convert the consolidated rental data to a standardized CSV format.
    
    Args:
        input_path: Path to the consolidated JSON input file
        output_path: Path to save the CSV output file
        
    Returns:
        True if conversion was successful, False otherwise
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    
    # Check if input file exists
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        return False
    
    # Load the consolidated rental data
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            rental_data = json.load(f)
        
        logger.info(f"Read {len(rental_data)} records from input file")
    except Exception as e:
        logger.error(f"Error loading input file: {e}")
        return False
    
    # Create a backup of the existing file if it exists
    if os.path.exists(output_path):
        backup_file = f"{output_path}.backup.{datetime.now().strftime('%Y%m%d%H%M%S')}"
        try:
            os.rename(output_path, backup_file)
            logger.info(f"Created backup of existing file: {backup_file}")
        except Exception as e:
            logger.warning(f"Could not create backup: {e}")
    
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Process and write the data to CSV
    processed_count = 0
    
    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'url', 'title', 'location', 'price', 'size', 'num_rooms', 
                'room_type', 'details', 'snapshot_date', 'is_rental'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for listing in rental_data:
                # Skip invalid listings
                if 'url' not in listing or not listing['url']:
                    continue
                
                # Extract key data
                title = listing.get('title', '')
                details = listing.get('details', '')
                
                # Handle different possible field names for price
                price_value = 0
                if 'rent_price' in listing:
                    price_value = extract_price(listing['rent_price'])
                elif 'price' in listing:
                    price_value = extract_price(listing['price'])
                
                # Extract size - handle different field names
                size_value = 0
                if 'size' in listing and listing['size']:
                    size_value = extract_size(listing['size'])
                
                # Extract room type
                room_type = extract_room_type(title)
                
                # Extract number of rooms - try from room_type first then from other fields
                num_rooms = 0
                if room_type and room_type[0] == 'T' and len(room_type) > 1:
                    try:
                        num_rooms = int(room_type[1:])
                    except ValueError:
                        pass
                elif 'num_rooms' in listing:
                    try:
                        num_rooms = int(listing['num_rooms'])
                    except (ValueError, TypeError):
                        pass
                
                # Extract location
                location = extract_location(listing.get('location', ''), title)
                
                # Prepare row for CSV
                row = {
                    'url': listing.get('url', ''),
                    'title': title,
                    'location': location,
                    'price': price_value,
                    'size': size_value,
                    'num_rooms': num_rooms,
                    'room_type': room_type,
                    'details': details,
                    'snapshot_date': listing.get('snapshot_date', datetime.now().strftime('%Y-%m-%d')),
                    'is_rental': True
                }
                
                writer.writerow(row)
                processed_count += 1
            
        logger.info(f"Converted {processed_count} records to CSV format")
        
        # Create metadata
        metadata = {
            'source_file': str(input_path),
            'output_file': str(output_path),
            'record_count': processed_count,
            'processed_at': datetime.now().isoformat(),
            'fields': fieldnames
        }
        
        # Save metadata to a sidecar file
        metadata_file = output_path.with_suffix('.metadata.json')
        try:
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"Saved metadata to {metadata_file}")
        except Exception as e:
            logger.warning(f"Could not save metadata: {e}")
        
        return True
    except Exception as e:
        logger.error(f"Error converting data: {e}")
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
    input_json = data_dir / "processed" / "rental_listings_consolidated.json"
    output_csv = data_dir / "processed" / "rentals.csv"
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Convert consolidated rental data to CSV")
    parser.add_argument("--input", default=str(input_json), help="Input JSON file path")
    parser.add_argument("--output", default=str(output_csv), help="Output CSV file path")
    
    args = parser.parse_args()
    
    # Run the conversion
    success = convert_rentals(args.input, args.output)
    exit(0 if success else 1) 