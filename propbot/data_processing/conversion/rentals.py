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
from ...utils.extraction_utils import extract_price_improved
import psycopg2
from psycopg2 import extras
from decimal import Decimal

# Set up logging
logger = logging.getLogger(__name__)

def extract_price(price_str: Optional[Union[str, int, float]]) -> Optional[float]:
    """
    Extract the rental price from a string or numeric value and convert to float.
    
    Note: This function is a wrapper around the improved version in extraction_utils.
    It maintains backward compatibility while using the more robust implementation.
    
    Args:
        price_str: String or numeric value containing price information
        
    Returns:
        Monthly rental price in euros as a float, or None if invalid
    """
    # Handle None case
    if price_str is None:
        return None
        
    # Handle numeric types directly
    if isinstance(price_str, (int, float)):
        return float(price_str)
    
    # Use the improved version from extraction_utils
    price_value = extract_price_improved(price_str)
    
    # Return None for invalid prices to maintain backward compatibility
    return price_value if price_value > 0 else None

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
    Convert rental data from database to standardized CSV format.
    
    Args:
        input_path: Path to input file (not used, kept for backward compatibility)
        output_path: Path to save the CSV output file
        
    Returns:
        True if conversion was successful, False otherwise
    """
    try:
        # Get database connection
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return False
            
        try:
            # Query rental data from database
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                    SELECT 
                        url, title, price, size, rooms, 
                        price_per_sqm, location, neighborhood,
                        details, snapshot_date, first_seen_date
                    FROM properties_rentals
                    ORDER BY snapshot_date DESC
                """)
                
                # Convert to list of dictionaries
                records = []
                for row in cur.fetchall():
                    record = dict(row)
                    # Convert Decimal values to float
                    for key, value in record.items():
                        if isinstance(value, Decimal):
                            record[key] = float(value)
                    records.append(record)
                
                logger.info(f"Retrieved {len(records)} rental records from database")
            
            if not records:
                logger.warning("No rental records found in database")
                return False
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Write to CSV
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'url', 'title', 'price', 'size', 'rooms', 
                    'price_per_sqm', 'location', 'neighborhood',
                    'details', 'snapshot_date', 'first_seen_date'
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)
            
            logger.info(f"Converted {len(records)} rental records to CSV format")
            return True
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error converting rental data: {e}")
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