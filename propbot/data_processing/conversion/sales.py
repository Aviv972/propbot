#!/usr/bin/env python3
"""
Convert Consolidated Sales Data to CSV

This module provides functions to convert consolidated sales data from JSON 
format to a standardized CSV format for use in rental analysis and other modules.
"""

import os
import re
import csv
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Union
import pandas as pd
from ...utils.extraction_utils import extract_size as extract_size_robust
from ...utils.extraction_utils import extract_room_type as extract_room_type_robust
from ...utils.extraction_utils import extract_price_improved

# Set up logging
logger = logging.getLogger(__name__)

def extract_price(price_str: Optional[str]) -> int:
    """
    Extract the price from a string and convert to numeric value.
    
    Note: This function is a wrapper around the improved version in extraction_utils.
    It maintains backward compatibility while using the more robust implementation.
    
    Args:
        price_str: String containing price information
        
    Returns:
        Price in euros as an integer (e.g., "350,000 €" -> 350000)
    """
    if not price_str:
        return 0
    
    # Use the improved version from extraction_utils
    price_value = extract_price_improved(price_str)
    
    # Convert to integer for backward compatibility
    return int(price_value) if price_value > 0 else 0

def extract_size(size_str, room_type=None):
    """
    Extract numeric size value from a string.
    
    Note: This function is deprecated and remains only for backward compatibility.
    Use propbot.utils.extraction_utils.extract_size instead.
    
    Args:
        size_str: String containing size information (e.g., "120 m²")
        room_type: Optional room type (T0, T1, T2, etc.) to help validate the extracted size
        
    Returns:
        Integer representing square meters or 0 if invalid
    """
    if not size_str:
        return 0
    
    # Use the robust implementation from extraction_utils with room type context
    size, confidence = extract_size_robust(size_str, room_type)
    if not confidence:
        logger.debug(f"Low confidence size extraction: '{size_str}' -> {size}. Room type: {room_type}")
    
    return int(size) if size is not None else 0

def extract_rooms(rooms_str: Optional[str]) -> int:
    """
    Extract the number of rooms from a string and convert to numeric value.
    
    Args:
        rooms_str: String containing room information
        
    Returns:
        Number of rooms as an integer (e.g., "3 rooms" -> 3)
    """
    if not rooms_str or not isinstance(rooms_str, str):
        return 0
    
    # Try to extract the numeric part of the rooms
    rooms_match = re.search(r'\d+', rooms_str)
    if not rooms_match:
        return 0
    
    try:
        return int(rooms_match.group(0))
    except (ValueError, TypeError):
        logger.warning(f"Could not convert rooms string: {rooms_str}")
        return 0

def extract_room_type_from_title(title):
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

def extract_size_from_title(title, details, room_type=None):
    """
    Extract size from title or details.
    
    Note: This function is partially deprecated and uses the robust extraction utilities internally.
    
    Args:
        title: Property title text
        details: Property details text
        room_type: Optional room type to help validate the size
        
    Returns:
        Integer size in square meters or 0 if not found
    """
    # Try to extract from details first (most reliable)
    if details:
        size, confidence = extract_size_robust(details, room_type)
        if size is not None:
            return int(size)
    
    # Then try from title
    if title:
        size, confidence = extract_size_robust(title, room_type)
        if size is not None:
            return int(size)
    
    # If we have a room type, use a reasonable estimate
    if room_type:
        if room_type == "T0" or room_type == "Studio":
            return 35  # Average size for studios
        elif room_type == "T1":
            return 50  # Average size for 1-bedroom
        elif room_type == "T2":
            return 70  # Average size for 2-bedroom
        elif room_type == "T3":
            return 90  # Average size for 3-bedroom
        elif room_type == "T4":
            return 120  # Average size for 4-bedroom
        elif room_type == "T5":
            return 150  # Average size for 5-bedroom
    
    return 0  # No size found

def convert_sales(input_path: Union[str, Path], output_path: Union[str, Path]) -> bool:
    """
    Convert the consolidated sales data to a standardized CSV format.
    
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
    
    # Load the consolidated sales data
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            sales_data = json.load(f)
        
        logger.info(f"Read {len(sales_data)} records from input file")
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
                'price_per_sqm', 'room_type', 'snapshot_date'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for listing in sales_data:
                # Skip invalid listings
                if 'url' not in listing or not listing['url']:
                    continue
                
                # Extract key data
                title = listing.get('title', '')
                details = listing.get('details', '')
                price = extract_price(listing.get('price', '0'))
                
                # Extract or estimate size and room type from title
                room_type = extract_room_type_from_title(title)
                size = extract_size(listing.get('size', '0'), room_type)
                
                # If size is missing, try to extract from title or details
                if not size or size == 0:
                    size = extract_size_from_title(title, details, room_type)
                
                # Extract number of rooms
                num_rooms = 0
                if room_type and room_type[0] == 'T' and len(room_type) > 1:
                    try:
                        num_rooms = int(room_type[1:])
                    except ValueError:
                        pass
                
                # Calculate price per sqm
                price_per_sqm = round(price / size, 2) if size > 0 and price > 0 else 0
                
                # Prepare row for CSV
                row = {
                    'url': listing.get('url', ''),
                    'title': title,
                    'location': listing.get('location', ''),
                    'price': price,
                    'size': size,
                    'num_rooms': num_rooms,
                    'price_per_sqm': price_per_sqm,
                    'room_type': room_type,
                    'snapshot_date': listing.get('snapshot_date', datetime.now().strftime('%Y-%m-%d'))
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
    input_json = data_dir / "processed" / "sales_listings_consolidated.json"
    output_csv = data_dir / "processed" / "sales.csv"
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Convert consolidated sales data to CSV")
    parser.add_argument("--input", default=str(input_json), help="Input JSON file path")
    parser.add_argument("--output", default=str(output_csv), help="Output CSV file path")
    
    args = parser.parse_args()
    
    # Run the conversion
    success = convert_sales(args.input, args.output)
    exit(0 if success else 1) 