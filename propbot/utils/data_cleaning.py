"""PropBot data cleaning utilities"""

import os
import re
import json
import csv
import logging
import pandas as pd
from datetime import datetime
from ..config import RAW_DATA_DIR, PROCESSED_DATA_DIR

# Configure logging
logger = logging.getLogger(__name__)

def extract_price(price_str):
    """Extract numeric price from a string with currency."""
    if not price_str or pd.isna(price_str):
        return None
    
    # Remove non-numeric characters except decimal point
    digits = re.sub(r'[^\d.]', '', str(price_str))
    if not digits:
        return None
    
    try:
        return int(float(digits))
    except (ValueError, TypeError):
        return None

def extract_size(size_str, room_type=None):
    """
    Extract numeric size from a size string with units.
    
    This function is deprecated and should be replaced with 
    propbot.utils.extraction_utils.extract_size in new code.
    
    Args:
        size_str: String containing size information
        room_type: Optional room type (T0, T1, etc.) to help validate size
        
    Returns:
        Float representing size in square meters or None if invalid
    """
    if not size_str or pd.isna(size_str):
        return None
    
    # Check if extraction_utils is available
    try:
        from propbot.utils.extraction_utils import extract_size as extract_size_robust
        # Use the robust implementation if available
        size, _ = extract_size_robust(str(size_str), room_type)
        return size
    except ImportError:
        # Fall back to simple implementation
        # Find all numbers in the string
        matches = re.findall(r'\d+(?:\.\d+)?', str(size_str))
        if not matches:
            return None
        
        try:
            # Return the first number found
            return float(matches[0])
        except (ValueError, TypeError, IndexError):
            return None

def extract_room_type(details):
    """Extract room type from details text."""
    if not details or pd.isna(details):
        return None
    
    details = str(details).lower()
    
    # Define regex patterns for different room types
    patterns = {
        r'\b1\s*bed|\bstudio\b|\bone\s*bed': '1 bedroom',
        r'\b2\s*bed|\btwo\s*bed': '2 bedroom',
        r'\b3\s*bed|\bthree\s*bed': '3 bedroom',
        r'\b4\s*bed|\bfour\s*bed': '4 bedroom',
        r'\b5\+\s*bed|\bfive\+\s*bed': '5+ bedroom'
    }
    
    # Check each pattern
    for pattern, room_type in patterns.items():
        if re.search(pattern, details):
            return room_type
    
    return None

def convert_rental_data():
    """Convert rental listings JSON data to CSV format."""
    logger.info("Converting rental data from JSON to CSV")
    
    # Ensure directories exist
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    
    # Input and output file paths
    json_file = os.path.join(RAW_DATA_DIR, 'rental_listings.json')
    csv_file = os.path.join(PROCESSED_DATA_DIR, 'rental_data.csv')
    
    # Check if the input file exists
    if not os.path.exists(json_file):
        logger.error(f"Input file not found: {json_file}")
        return False
    
    try:
        # Load the JSON data
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Loaded {len(data)} rental listings from JSON")
        
        # Create a list to hold the converted data
        converted_data = []
        
        # Process each listing
        for listing in data:
            # Extract relevant information
            converted_listing = {
                'property_id': listing.get('id', ''),
                'url': listing.get('url', ''),
                'location': listing.get('location', ''),
                'price': extract_price(listing.get('price', '')),
                'size_sqm': extract_size(listing.get('size', '')),
                'room_type': extract_room_type(listing.get('details', '')),
                'details': listing.get('details', ''),
                'snapshot_date': datetime.now().strftime('%Y-%m-%d')
            }
            
            converted_data.append(converted_listing)
        
        # Write to CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            if converted_data:
                fieldnames = converted_data[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(converted_data)
        
        logger.info(f"Successfully converted {len(converted_data)} listings to CSV: {csv_file}")
        return True
    
    except Exception as e:
        logger.exception(f"Error converting rental data: {e}")
        return False

def clean_location_data(df):
    """Clean location data in the DataFrame."""
    if 'location' not in df.columns:
        logger.warning("No 'location' column found in DataFrame")
        return df
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Standardize location names
    location_patterns = {
        r'\bD(\d{1,2})\b': r'Dublin \1',  # Convert D1, D2, etc. to Dublin 1, Dublin 2
        r'Dublin(\d{1,2})': r'Dublin \1',  # Convert Dublin1, Dublin2 to Dublin 1, Dublin 2
        r'North\s*Dublin': 'North Dublin',
        r'South\s*Dublin': 'South Dublin',
        r'West\s*Dublin': 'West Dublin',
        r'East\s*Dublin': 'East Dublin',
        r'City\s*Centre': 'City Centre',
        r'City\s*Center': 'City Centre'
    }
    
    # Apply each pattern
    for pattern, replacement in location_patterns.items():
        df['location'] = df['location'].astype(str).str.replace(
            pattern, replacement, regex=True
        )
    
    logger.info("Location data cleaned successfully")
    return df

def clean_price_data(df):
    """Clean price data in the DataFrame."""
    if 'price' not in df.columns:
        logger.warning("No 'price' column found in DataFrame")
        return df
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Convert price to numeric, replacing errors with NaN
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    # Remove outliers (e.g., prices that are too low or too high)
    q1 = df['price'].quantile(0.01)  # 1st percentile
    q3 = df['price'].quantile(0.99)  # 99th percentile
    
    # Filter out prices outside the range
    df = df[(df['price'] >= q1) & (df['price'] <= q3)]
    
    logger.info("Price data cleaned successfully")
    return df

def clean_and_save_data(input_file, output_file=None):
    """Clean data in a CSV file and save the result."""
    if output_file is None:
        # If no output file is specified, use the input file
        output_file = input_file
    
    try:
        # Load the data
        df = pd.read_csv(input_file)
        logger.info(f"Loaded {len(df)} rows from {input_file}")
        
        # Apply cleaning functions
        df = clean_location_data(df)
        df = clean_price_data(df)
        
        # Save the cleaned data
        df.to_csv(output_file, index=False)
        logger.info(f"Saved {len(df)} cleaned rows to {output_file}")
        
        return True
    
    except Exception as e:
        logger.exception(f"Error cleaning data: {e}")
        return False 